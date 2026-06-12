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

import java.util.List;

import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats.LeaseTableWorkerMetricStats;

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
 */
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
                workerMetricsReporterFrequencyMillis);
    }

    @Override
    public void initialize() throws DependencyException {
        // No-op — the lease table is always available (created by lease management)
    }

    @Override
    public List<WorkerMetricStats> getAllWorkerMetricStats()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return super.getAllWorkerMetricStats();
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
