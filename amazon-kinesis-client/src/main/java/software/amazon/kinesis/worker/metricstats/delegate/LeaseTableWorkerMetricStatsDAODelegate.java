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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats.LeaseTableWorkerMetricStats;

import static software.amazon.kinesis.common.FutureUtils.unwrappingFuture;

/**
 * Delegate for WorkerMetricStats stored in the unified lease table.
 *
 * <p>The lease table uses {@code leaseKey} as its partition key attribute name,
 * whereas the legacy dedicated table uses {@code wid}. This delegate uses the
 * {@link LeaseTableWorkerMetricStats} inner subclass which overrides the partition key
 * getter to map the {@code workerId} field to the {@code leaseKey} DDB attribute.</p>
 *
 * <p>The lease table is always available (created by the lease management layer),
 * so {@link #initialize()} is a no-op.</p>
 *
 * <p><b>Note on scan efficiency:</b> The scan in {@link #getAllWorkerMetricStats()} uses a
 * filter expression on {@code entityType = WORKER_METRIC_STATS}. In the legacy dedicated
 * table this filter is effectively a no-op (all records match). In the lease table, this
 * is not efficient as DynamoDB still reads the entire table and applies the filter
 * server-side. A dedicated entityType GSI would have poor partition distribution for lease
 * entries since all leases share the same entityType value. A better future optimization is
 * to use the leaseOwner GSI (with entityType stored in the leaseOwner attribute position
 * (for non-lease entities) and once the GSI becomes available after the migration
 * state machine completes it can be used for efficient scans which is essentially a query.</p>
 */
@Slf4j
@KinesisClientInternalApi
public class LeaseTableWorkerMetricStatsDAODelegate extends WorkerMetricStatsDAODelegate<LeaseTableWorkerMetricStats> {

    public LeaseTableWorkerMetricStatsDAODelegate(
            final DynamoDbAsyncClient dynamoDbAsyncClient,
            String leaseTableName,
            final Long workerMetricsReporterFrequencyMillis) {
        super(
                dynamoDbAsyncClient,
                leaseTableName,
                LeaseTableWorkerMetricStats.class,
                TableSchema.fromBean(LeaseTableWorkerMetricStats.class),
                DynamoDBLeaseSerializer.LEASE_KEY_KEY,
                workerMetricsReporterFrequencyMillis,
                log);
    }

    @Override
    public void initialize() throws DependencyException {
        // No-op — the lease table is always available (created by lease management)
    }

    /**
     * Retrieve all worker metric stats entries from the backing table, filtered by
     * {@code entityType = WORKER_METRIC_STATS}. In the legacy dedicated table this
     * filter is a no-op since all entries should match. In the lease table, this
     * ensures only WorkerMetricStats records are returned (not leases, coordinator state, etc.).
     *
     * @return list of all {@link WorkerMetricStats} entries
     * @throws DependencyException if DynamoDB operation fails unexpectedly
     * @throws InvalidStateException if the backing table does not exist
     * @throws ProvisionedThroughputException if DynamoDB lacks capacity
     */
    @Override
    public List<WorkerMetricStats> getAllWorkerMetricStats()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Scanning WorkerMetricStats from table {}", tableName);

        final Expression filterExpression = Expression.builder()
                .expression("#et = :etVal")
                .expressionNames(Collections.singletonMap("#et", DynamoDBLeaseSerializer.ENTITY_TYPE_ATTRIBUTE_NAME))
                .expressionValues(Collections.singletonMap(
                        ":etVal",
                        AttributeValue.builder()
                                .s(EntityType.WORKER_METRIC_STATS.getDdbValue())
                                .build()))
                .build();

        final ScanEnhancedRequest scanRequest =
                ScanEnhancedRequest.builder().filterExpression(filterExpression).build();

        final List<WorkerMetricStats> workerMetricStats = new ArrayList<>();
        try {
            unwrappingFuture(() -> table.scan(scanRequest).items().subscribe(workerMetricStats::add));
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(
                    String.format("Cannot scan WorkerMetricStats, because table %s does not exist", tableName));
        } catch (final Exception e) {
            throw new DependencyException(e);
        }
        return workerMetricStats;
    }

    @Override
    public void updateMetrics(final WorkerMetricStats workerMetrics)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        super.updateMetrics(workerMetrics);
    }

    /**
     * Convert a base {@link WorkerMetricStats} into a {@link LeaseTableWorkerMetricStats}
     * so the Enhanced Client maps workerId to the {@code leaseKey} attribute.
     */
    @Override
    protected LeaseTableWorkerMetricStats toEntity(final WorkerMetricStats workerMetrics) {
        if (workerMetrics instanceof LeaseTableWorkerMetricStats) {
            return (LeaseTableWorkerMetricStats) workerMetrics;
        }
        return LeaseTableWorkerMetricStats.builder()
                .workerId(workerMetrics.getWorkerId())
                .entityType(workerMetrics.getEntityType())
                .lastUpdateTime(workerMetrics.getLastUpdateTime())
                .metricStats(workerMetrics.getMetricStats())
                .operatingRange(workerMetrics.getOperatingRange())
                .properties(workerMetrics.getProperties())
                .supportCode(workerMetrics.getSupportCode())
                .supportCodeUpdateEpochSeconds(workerMetrics.getSupportCodeUpdateEpochSeconds())
                .build();
    }
}
