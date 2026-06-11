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

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatus;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatusProvider;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.worker.metricstats.delegate.LeaseTableWorkerMetricStatsDAODelegate;
import software.amazon.kinesis.worker.metricstats.delegate.LegacyTableWorkerMetricStatsDAODelegate;
import software.amazon.kinesis.worker.metricstats.delegate.WorkerMetricStatsDAODelegate;

/**
 * Data Access Object that routes {@link WorkerMetricStats} operations to the appropriate
 * DDB table based on the {@link TableMigrationStatus} captured at initialization time.
 *
 * The write target is cached at startup so that it does not change dynamically during the
 * lifetime of this DAO instance. If the migration status changes, it will only reflect
 * on the next startup.
 *
 * Read Logic (getAllWorkerMetricStats):
 *   Combines results from both legacy and lease table delegates.
 *   If status == COMPLETE, reads only from lease table.
 *   Caller is responsible for determining which entries are current/latest/not expired.
 *
 * Write/Delete Logic:
 *   If status == COMPLETE or PENDING: write to lease table only.
 *   If status == INIT or DEPLOYED: write to legacy table only.
 */
@Slf4j
@KinesisClientInternalApi
public class WorkerMetricStatsDAO {

    private final LeaseTableWorkerMetricStatsDAODelegate leaseTableDaoDelegate;
    private final LegacyTableWorkerMetricStatsDAODelegate legacyTableDaoDelegate;
    private final TableMigrationStatusProvider tableMigrationStatusProvider;
    private volatile TableMigrationStatus cachedStatus;
    private volatile boolean initialized;

    public WorkerMetricStatsDAO(
            final DynamoDbAsyncClient dynamoDbAsyncClient,
            final WorkerMetricsTableConfig workerMetricsTableConfig,
            final String leaseTableName,
            Long workerMetricsReporterFrequencyMillis,
            final TableMigrationStatusProvider tableMigrationStatusProvider) {
        this.leaseTableDaoDelegate = new LeaseTableWorkerMetricStatsDAODelegate(
                dynamoDbAsyncClient, leaseTableName, workerMetricsReporterFrequencyMillis);
        this.legacyTableDaoDelegate = new LegacyTableWorkerMetricStatsDAODelegate(
                dynamoDbAsyncClient, workerMetricsTableConfig, workerMetricsReporterFrequencyMillis);
        this.tableMigrationStatusProvider = tableMigrationStatusProvider;
        this.initialized = false;
    }

    public LegacyTableWorkerMetricStatsDAODelegate getLegacyTableDaoDelegate() {
        return legacyTableDaoDelegate;
    }

    public LeaseTableWorkerMetricStatsDAODelegate getLeaseTableDaoDelegate() {
        return leaseTableDaoDelegate;
    }

    /**
     * Initialize the DAO: initializes both delegates and caches the current migration status
     * to determine the write target for the lifetime of this DAO instance.
     *
     * @throws DependencyException if unable to determine legacy table existence
     * @throws InvalidStateException if the TableMigrationStatusProvider is still UNKNOWN
     */
    public void initialize() throws DependencyException {
        if (initialized) {
            log.info("WorkerMetricStatsDAO already initialized");
            return;
        }
        legacyTableDaoDelegate.initialize();
        leaseTableDaoDelegate.initialize();

        cachedStatus = tableMigrationStatusProvider.getTableMigrationStatus();
        if (cachedStatus == TableMigrationStatus.TABLE_MIGRATION_STATUS_UNKNOWN) {
            throw new DependencyException(
                    "Cannot initialize WorkerMetricStatsDAO: TableMigrationStatusProvider is still UNKNOWN", null);
        }
        initialized = true;
        log.info(
                "WorkerMetricStatsDAO initialized. Legacy enabled: {}, cached migration status: {}",
                legacyTableDaoDelegate.isEnabled(),
                cachedStatus);
    }

    // ==================== Read Operations ====================

    /**
     * Get all worker metric stats by combining results from both delegates.
     * If migration is COMPLETE, reads only from lease table.
     * The caller is responsible for determining which entries are current/latest/not expired.
     *
     * @return list of all worker metric stats from both tables (may contain duplicates by workerId)
     * @throws DependencyException if DDB fails unexpectedly
     * @throws InvalidStateException if a required table does not exist
     * @throws ProvisionedThroughputException if DDB lacks capacity
     */
    public List<WorkerMetricStats> getAllWorkerMetricStats()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        ensureInitialized();

        if (cachedStatus == TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE) {
            return leaseTableDaoDelegate.getAllWorkerMetricStats();
        }

        // Combine from both tables; caller decides which entries are current
        final List<WorkerMetricStats> combined = new ArrayList<>();
        combined.addAll(legacyTableDaoDelegate.getAllWorkerMetricStats());
        combined.addAll(leaseTableDaoDelegate.getAllWorkerMetricStats());
        return combined;
    }

    // ==================== Write Operations ====================

    /**
     * Update (or create) the worker metric stats for a given worker.
     * Routes to the appropriate table based on the cached migration status.
     *
     * @param workerMetrics the worker metrics to persist
     * @throws DependencyException if DDB fails unexpectedly
     * @throws InvalidStateException if not initialized or table does not exist
     * @throws ProvisionedThroughputException if DDB lacks capacity
     */
    public void updateMetrics(final WorkerMetricStats workerMetrics)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        ensureInitialized();
        getWriteDelegate().updateMetrics(workerMetrics);
    }

    /**
     * Delete the worker metric stats entry with a conditional check on lastUpdateTime.
     *
     * @param workerMetrics the entry to delete (workerId and lastUpdateTime required)
     * @return true if deleted, false if the conditional check failed
     * @throws DependencyException if DDB fails unexpectedly
     * @throws InvalidStateException if not initialized or table does not exist
     * @throws ProvisionedThroughputException if DDB lacks capacity
     */
    public boolean deleteMetrics(final WorkerMetricStats workerMetrics)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        ensureInitialized();
        return getWriteDelegate().deleteMetrics(workerMetrics);
    }

    // ==================== Private Helpers ====================

    private WorkerMetricStatsDAODelegate<?> getWriteDelegate() {
        switch (cachedStatus) {
            case TABLE_MIGRATION_STATUS_COMPLETE:
            case TABLE_MIGRATION_STATUS_PENDING:
                return leaseTableDaoDelegate;
            case TABLE_MIGRATION_STATUS_INIT:
            case TABLE_MIGRATION_STATUS_DEPLOYED:
                return legacyTableDaoDelegate;
            default:
                throw new IllegalStateException("Cannot determine write delegate for status: " + cachedStatus);
        }
    }

    private void ensureInitialized() throws InvalidStateException {
        if (!initialized) {
            throw new InvalidStateException("WorkerMetricStatsDAO is not initialized. Call initialize() first.");
        }
    }
}
