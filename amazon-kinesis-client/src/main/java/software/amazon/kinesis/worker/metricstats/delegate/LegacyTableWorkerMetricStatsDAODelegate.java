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
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats.LegacyWorkerMetricStats;

import static software.amazon.kinesis.common.FutureUtils.unwrappingFuture;

/**
 * Delegate for the legacy (separate) WorkerMetricStats DDB table.
 *
 * <p>The legacy table uses {@code wid} as its partition key, which is defined in
 * {@link LegacyWorkerMetricStats}.</p>
 *
 * <p>On {@link #initialize()}, checks if the legacy table exists. If it does not exist,
 * the delegate is marked as not enabled and all read operations become no-ops
 * (returning empty collections). Write operations on a non-enabled delegate
 * will throw {@link InvalidStateException}.</p>
 *
 * <p>The legacy table should only be deleted after the table migration status moves to
 * COMPLETE, at which point no read/write calls should be routed to this delegate.</p>
 */
@Slf4j
@KinesisClientInternalApi
public class LegacyTableWorkerMetricStatsDAODelegate extends WorkerMetricStatsDAODelegate<LegacyWorkerMetricStats> {

    private volatile boolean enabled = true;

    public LegacyTableWorkerMetricStatsDAODelegate(
            final DynamoDbAsyncClient dynamoDbAsyncClient,
            final WorkerMetricsTableConfig tableConfig,
            final Long workerMetricsReporterFrequencyMillis) {
        super(
                dynamoDbAsyncClient,
                tableConfig.tableName(),
                LegacyWorkerMetricStats.class,
                TableSchema.fromBean(LegacyWorkerMetricStats.class),
                WorkerMetricStats.KEY_WORKER_ID,
                workerMetricsReporterFrequencyMillis,
                log);
    }

    @Override
    public void initialize() throws DependencyException {
        final DescribeTableRequest request =
                DescribeTableRequest.builder().tableName(getTableName()).build();
        try {
            unwrappingFuture(() -> getDynamoDbAsyncClient().describeTable(request));
            log.info("Legacy WorkerMetricStats table {} exists, delegate enabled", getTableName());
            enabled = true;
        } catch (final ResourceNotFoundException e) {
            log.info("Legacy WorkerMetricStats table {} does not exist, delegate not enabled", getTableName());
            enabled = false;
        } catch (final Exception e) {
            throw new DependencyException(
                    "Unable to determine if legacy WorkerMetricStats table " + getTableName() + " exists", e);
        }
    }

    /**
     * For the legacy table, simply cast to LegacyWorkerMetricStats using the builder.
     */
    @Override
    protected LegacyWorkerMetricStats toEntity(final WorkerMetricStats workerMetrics) {
        if (workerMetrics instanceof LegacyWorkerMetricStats) {
            return (LegacyWorkerMetricStats) workerMetrics;
        }
        return LegacyWorkerMetricStats.builder()
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

    /**
     * @return true if the legacy table exists and this delegate is enabled for operations
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Retrieve all worker metric stats entries from the backing table. In the legacy table entries
     * are not filtered by entity type since during migration old workers will not be writing
     * the entity type.
     *
     * @return list of all {@link WorkerMetricStats} entries
     * @throws DependencyException if DynamoDB operation fails unexpectedly
     * @throws InvalidStateException if the backing table does not exist
     * @throws ProvisionedThroughputException if DynamoDB lacks capacity
     */
    @Override
    public List<WorkerMetricStats> getAllWorkerMetricStats()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (!enabled) {
            return Collections.emptyList();
        }
        log.debug("Scanning WorkerMetricStats from table {}", tableName);

        final ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder().build();

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
        if (!enabled) {
            throw new InvalidStateException(
                    "Legacy WorkerMetricStats table does not exist, cannot update worker metrics");
        }
        super.updateMetrics(workerMetrics);
    }

    @Override
    public boolean deleteMetrics(final WorkerMetricStats workerMetrics)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (!enabled) {
            throw new InvalidStateException(
                    "Legacy WorkerMetricStats table does not exist, cannot delete worker metrics");
        }
        return super.deleteMetrics(workerMetrics);
    }
}
