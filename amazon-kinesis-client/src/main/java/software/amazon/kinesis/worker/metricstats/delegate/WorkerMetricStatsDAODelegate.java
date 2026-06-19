/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.kinesis.worker.metricstats.delegate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.DeleteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

import static java.util.Objects.nonNull;
import static software.amazon.kinesis.common.FutureUtils.unwrappingFuture;
import static software.amazon.kinesis.worker.metricstats.WorkerMetricStats.KEY_LAST_UPDATE_TIME;

/**
 * Data Access Object to abstract accessing {@link WorkerMetricStats} from
 * the DDB table it is stored in.
 *
 * <p>This follows the same delegate pattern as
 * {@link software.amazon.kinesis.coordinator.delegate.CoordinatorStateDAODelegate}.
 * The base class contains all DDB operation implementations. Subclasses provide
 * their specific table name/configuration, bean type {@code T}, and override
 * {@link #initialize()} behavior.</p>
 *
 * <p>The type parameter {@code T} allows subclasses to use the correct bean type
 * for their DDB table schema. The legacy table uses {@link WorkerMetricStats} directly
 * (PK attribute: {@code wid}), while the lease table uses
 * {@link software.amazon.kinesis.worker.metricstats.LeaseTableWorkerMetricStats}
 * (PK attribute: {@code leaseKey}).</p>
 *
 * @param <T> the concrete bean type that maps to the DDB table schema
 */
@KinesisClientInternalApi
public abstract class WorkerMetricStatsDAODelegate<T extends WorkerMetricStats> {

    protected final Logger log;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    protected final String tableName;
    protected final DynamoDbAsyncTable<T> table;
    private final Class<T> beanClass;
    private final String paritionKeyAttributeName;
    private final Long workerMetricsReporterFrequencyMillis;

    /**
     * @param dynamoDbAsyncClient the DynamoDB async client
     * @param tableName the name of the DDB table
     * @param beanClass the class of the bean type (needed for UpdateItemEnhancedRequest)
     * @param tableSchema the schema to use for mapping the bean type to/from DDB records
     */
    public WorkerMetricStatsDAODelegate(
            final DynamoDbAsyncClient dynamoDbAsyncClient,
            final String tableName,
            final Class<T> beanClass,
            final TableSchema<T> tableSchema,
            final String paritionKeyAttributeName,
            final Long workerMetricsReporterFrequencyMillis,
            final Logger logger) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.tableName = tableName;
        this.beanClass = beanClass;
        final DynamoDbEnhancedAsyncClient enhancedClient = DynamoDbEnhancedAsyncClient.builder()
                .dynamoDbClient(dynamoDbAsyncClient)
                .build();
        this.table = enhancedClient.table(tableName, tableSchema);
        this.paritionKeyAttributeName = paritionKeyAttributeName;
        this.workerMetricsReporterFrequencyMillis = workerMetricsReporterFrequencyMillis;
        this.log = logger;
    }

    /**
     * Initialize the delegate. Implementations may check table existence
     * or perform other setup.
     *
     * @throws DependencyException if initialization fails due to external dependencies
     */
    public abstract void initialize() throws DependencyException;

    /**
     * Convert a base {@link WorkerMetricStats} into the concrete bean type {@code T}.
     * The legacy delegate can simply return the input (since T = WorkerMetricStats),
     * while the lease table delegate converts to LeaseTableWorkerMetricStats.
     */
    protected abstract T toEntity(WorkerMetricStats workerMetrics);

    protected DynamoDbAsyncClient getDynamoDbAsyncClient() {
        return dynamoDbAsyncClient;
    }

    protected String getTableName() {
        return tableName;
    }

    /**
     * Retrieve all worker metric stats entries from the backing table
     *
     * @return list of all {@link WorkerMetricStats} entries
     * @throws DependencyException if DynamoDB operation fails unexpectedly
     * @throws InvalidStateException if the backing table does not exist
     * @throws ProvisionedThroughputException if DynamoDB lacks capacity
     */
    public abstract List<WorkerMetricStats> getAllWorkerMetricStats()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Update (or create) the worker metric stats for a given worker.
     * Performs an upsert, ignoring null attributes.
     *
     * @param workerMetrics the worker metrics to persist
     * @throws DependencyException if DynamoDB operation fails unexpectedly
     * @throws InvalidStateException if the backing table does not exist
     * @throws ProvisionedThroughputException if DynamoDB lacks capacity
     */
    public void updateMetrics(final WorkerMetricStats workerMetrics)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final T entity = toEntity(workerMetrics);
        validateWorkerMetrics(workerMetrics);
        final UpdateItemEnhancedRequest<T> request = UpdateItemEnhancedRequest.builder(beanClass)
                .item(entity)
                .ignoreNulls(true)
                .build();
        try {
            unwrappingFuture(() -> table.updateItem(request));
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(
                    String.format("Cannot update WorkerMetricStats, because table %s does not exist", tableName));
        } catch (final Exception e) {
            throw new DependencyException(e);
        }
    }

    /**
     * Delete the worker metric stats entry with a conditional check on lastUpdateTime.
     * If the worker has come alive and updated the lastUpdateTime since the provided
     * entry was read, the deletion will fail gracefully.
     *
     * @param workerMetrics the entry to delete (workerId and lastUpdateTime required)
     * @return true if deleted, false if the conditional check failed
     * @throws DependencyException if DynamoDB operation fails unexpectedly
     * @throws InvalidStateException if the backing table does not exist
     * @throws ProvisionedThroughputException if DynamoDB lacks capacity
     */
    public boolean deleteMetrics(final WorkerMetricStats workerMetrics)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Preconditions.checkArgument(nonNull(workerMetrics.getWorkerId()), "WorkerID is not provided");
        Preconditions.checkArgument(nonNull(workerMetrics.getLastUpdateTime()), "LastUpdateTime is not provided");

        final DeleteItemEnhancedRequest request = DeleteItemEnhancedRequest.builder()
                .key(Key.builder().partitionValue(workerMetrics.getWorkerId()).build())
                .conditionExpression(Expression.builder()
                        .expression(String.format("#key = :value AND attribute_exists (%s)", paritionKeyAttributeName))
                        .expressionNames(ImmutableMap.of("#key", KEY_LAST_UPDATE_TIME))
                        .expressionValues(ImmutableMap.of(
                                ":value", AttributeValue.fromN(Long.toString(workerMetrics.getLastUpdateTime()))))
                        .build())
                .build();

        try {
            unwrappingFuture(() -> table.deleteItem(request));
            return true;
        } catch (final ConditionalCheckFailedException e) {
            log.warn("Failed to delete WorkerMetricStats due to conditional failure for worker: {}", workerMetrics, e);
            return false;
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(
                    String.format("Cannot delete WorkerMetricStats, because table %s does not exist", tableName));
        } catch (final Exception e) {
            throw new DependencyException(e);
        }
    }

    private void validateWorkerMetrics(final WorkerMetricStats workerMetrics) {
        Preconditions.checkArgument(nonNull(workerMetrics.getMetricStats()), "ResourceMetrics not provided");

        final List<String> entriesWithoutValues = workerMetrics.getMetricStats().entrySet().stream()
                .filter(entry -> entry.getValue() == null || entry.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        Preconditions.checkArgument(
                entriesWithoutValues.isEmpty(), "Following metric stats dont have any values " + entriesWithoutValues);

        Preconditions.checkArgument(nonNull(workerMetrics.getLastUpdateTime()), "LastUpdateTime field not set");

        // If the LastUpdateTime field is 2x older than the reporter interval, it is considered stale.
        Preconditions.checkArgument(
                Duration.between(Instant.ofEpochSecond(workerMetrics.getLastUpdateTime()), Instant.now())
                                .toMillis()
                        < 2 * workerMetricsReporterFrequencyMillis,
                "LastUpdateTime is more than 2x older than workerMetricsReporterFrequencyMillis");
    }
}
