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

package software.amazon.kinesis.worker.metricstats;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.waiters.WaiterResponse;
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
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.utils.DdbUtil;

import static java.util.Objects.nonNull;
import static software.amazon.kinesis.common.FutureUtils.unwrappingFuture;
import static software.amazon.kinesis.worker.metricstats.WorkerMetricStats.KEY_LAST_UPDATE_TIME;
import static software.amazon.kinesis.worker.metricstats.WorkerMetricStats.KEY_WORKER_ID;

@Slf4j
@KinesisClientInternalApi
public class WorkerMetricStatsDAO {
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;
    private final DynamoDbAsyncTable<WorkerMetricStats> table;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final WorkerMetricsTableConfig tableConfig;
    private final Long workerMetricsReporterFrequencyMillis;

    public WorkerMetricStatsDAO(
            final DynamoDbAsyncClient dynamoDbAsyncClient,
            final WorkerMetricsTableConfig tableConfig,
            final Long workerMetricsReporterFrequencyMillis) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.dynamoDbEnhancedAsyncClient = DynamoDbEnhancedAsyncClient.builder()
                .dynamoDbClient(dynamoDbAsyncClient)
                .build();
        this.table = dynamoDbEnhancedAsyncClient.table(
                tableConfig.tableName(), TableSchema.fromBean(WorkerMetricStats.class));
        this.tableConfig = tableConfig;
        this.workerMetricsReporterFrequencyMillis = workerMetricsReporterFrequencyMillis;
    }

    /**
     * Performs initialization of the WorkerMetricStats DAO and table.
     * This will create the table if it doesn't exist.
     */
    public void initialize() throws DependencyException {
        createTableIfDoesNotExist();
    }

    /**
     * Updates the workerMetrics for the provided worker, method ignores the null attributes and overrides
     * the only non-null from {@param workerMetrics}. This is a blocking call.
     *
     * @param workerMetrics : Updated WorkerMetricStats object, resourceStats, workerId and lastUpdateTime are
     *                      required fields from {@param workerMetrics}
     */
    public void updateMetrics(final WorkerMetricStats workerMetrics) {
        validateWorkerMetrics(workerMetrics);
        final UpdateItemEnhancedRequest<WorkerMetricStats> request = UpdateItemEnhancedRequest.builder(
                        WorkerMetricStats.class)
                .item(workerMetrics)
                .ignoreNulls(true)
                .build();
        unwrappingFuture(() -> table.updateItem(request));
    }

    /**
     * Deletes the WorkerMetricStats entry with conditional check on lastUpdateTime, if the worker has come alive and
     * updated the lastUpdateTime then we no longer need to perform the deletion.
     * @param workerMetrics WorkerMetricStats that needs to be deleted.
     * @return
     */
    public boolean deleteMetrics(final WorkerMetricStats workerMetrics) {
        Preconditions.checkArgument(nonNull(workerMetrics.getWorkerId()), "WorkerID is not provided");
        Preconditions.checkArgument(nonNull(workerMetrics.getLastUpdateTime()), "LastUpdateTime is not provided");

        final DeleteItemEnhancedRequest request = DeleteItemEnhancedRequest.builder()
                .key(Key.builder().partitionValue(workerMetrics.getWorkerId()).build())
                .conditionExpression(Expression.builder()
                        .expression(String.format("#key = :value AND attribute_exists (%s)", KEY_WORKER_ID))
                        .expressionNames(ImmutableMap.of("#key", KEY_LAST_UPDATE_TIME))
                        .expressionValues(ImmutableMap.of(
                                ":value", AttributeValue.fromN(Long.toString(workerMetrics.getLastUpdateTime()))))
                        .build())
                .build();

        try {
            unwrappingFuture(() -> table.deleteItem(request));
            return true;
        } catch (final ConditionalCheckFailedException e) {
            log.warn(
                    "Failed to delete the WorkerMetricStats due to conditional failure for worker : {}",
                    workerMetrics,
                    e);
            return false;
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

    /**
     * Performs the scan on the storage and returns list of all workerMetricStats objects.
     *
     * @return : List of all worker metric stats
     */
    public List<WorkerMetricStats> getAllWorkerMetricStats() {
        log.debug("Scanning DDB table {}", table.tableName());
        final List<WorkerMetricStats> workerMetricStats = new ArrayList<>();
        unwrappingFuture(() -> table.scan().items().subscribe(workerMetricStats::add));
        return workerMetricStats;
    }

    private TableDescription getTableDescription() {
        try {
            final DescribeTableResponse response = unwrappingFuture(() -> dynamoDbAsyncClient.describeTable(
                    DescribeTableRequest.builder().tableName(table.tableName()).build()));
            return response.table();
        } catch (final ResourceNotFoundException e) {
            return null;
        }
    }

    private void createTableIfDoesNotExist() throws DependencyException {
        TableDescription tableDescription = getTableDescription();
        if (tableDescription == null) {
            unwrappingFuture(DdbUtil.tableCreator(
                    WorkerMetricStats::getKeySchema,
                    WorkerMetricStats::getAttributeDefinitions,
                    tableConfig,
                    dynamoDbAsyncClient));
            tableDescription = getTableDescription();
            log.info("Table : {} created.", table.tableName());
        } else {
            log.info("Table : {} already existing, skipping creation...", table.tableName());
        }

        if (tableDescription.tableStatus() != TableStatus.ACTIVE) {
            log.info("Waiting for DDB Table: {} to become active", table.tableName());
            try (final DynamoDbAsyncWaiter waiter = dynamoDbAsyncClient.waiter()) {
                final WaiterResponse<DescribeTableResponse> response =
                        unwrappingFuture(() -> waiter.waitUntilTableExists(
                                r -> r.tableName(table.tableName()), o -> o.waitTimeout(Duration.ofMinutes(10))));
                response.matched()
                        .response()
                        .orElseThrow(() -> new DependencyException(new IllegalStateException(
                                "Creating WorkerMetricStats table timed out",
                                response.matched().exception().orElse(null))));
            }

            unwrappingFuture(() -> DdbUtil.pitrEnabler(tableConfig, dynamoDbAsyncClient));
        }
    }
}
