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
package software.amazon.kinesis.coordinator.assignment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatus;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatusProvider;
import software.amazon.kinesis.coordinator.migration.TableMigrationSummary;
import software.amazon.kinesis.leases.EntityDAO;
import software.amazon.kinesis.leases.EntityDAO.Entity;
import software.amazon.kinesis.leases.EntityDAO.EntityScanList;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;

/**
 * Migration-aware implementation of {@link LAMDataManager} that encapsulates:
 * <ul>
 *   <li>Reading leases and worker metrics from the lease table (via EntityDAO) and
 *       optionally the legacy table (via WorkerMetricStatsDAO's legacy delegate)</li>
 *   <li>Filtering out invalid worker metrics (logging + emitting metrics)</li>
 *   <li>Filtering to only valid, non-expired worker metrics</li>
 *   <li>Async deletion of stale worker metrics (using the correct delegate based on origin)</li>
 *   <li>Computing and publishing {@link TableMigrationSummary} for migration state decisions</li>
 * </ul>
 *
 * <p>LAM receives valid, non-expired worker metrics and applies its own domain-specific
 * filtering (e.g., excluding workers with failing metrics from assignment).</p>
 *
 * The main purpose of this class it to reuse the scan result from LAM
 * for other things like tableMigrationStateMachine.
 * Ideally the two should be decoupled and can do their own scan to get
 * the result they want separately at the cost of higher DDB usage.
 * To make table migration seemless keep the iops low and being efficient
 * is better.
 */
@Slf4j
@KinesisClientInternalApi
@ThreadSafe
public class MigrationAwareLAMDataManager implements LAMDataManager {

    private static final int DDB_LOAD_RETRY_ATTEMPT = 1;
    public static final int DEFAULT_NO_OF_SKIP_STAT_FOR_DEAD_WORKER_THRESHOLD = 2;

    private final EntityDAO entityDAO;
    private final WorkerMetricStatsDAO workerMetricsDAO;
    private final TableMigrationStatusProvider tableMigrationStatusProvider;
    private final Consumer<TableMigrationSummary> migrationSummaryConsumer;
    private final ExecutorService executorService;
    private final Duration staleWorkerMetricsCleanupDuration;

    /**
     * The max age for a worker metric entry to be considered active.
     */
    private final Duration workerMetricsExpiryDuration;

    /**
     * Duration threshold for considering a support code heartbeat as still valid.
     * 3x the reporter frequency to tolerate transient delays.
     */
    private final Duration supportCodeExpiryThreshold;

    public MigrationAwareLAMDataManager(
            final EntityDAO entityDAO,
            final WorkerMetricStatsDAO workerMetricsDAO,
            final TableMigrationStatusProvider tableMigrationStatusProvider,
            final Consumer<TableMigrationSummary> migrationSummaryConsumer,
            final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config) {
        this.entityDAO = entityDAO;
        this.workerMetricsDAO = workerMetricsDAO;
        this.tableMigrationStatusProvider = tableMigrationStatusProvider;
        this.migrationSummaryConsumer = migrationSummaryConsumer;
        this.workerMetricsExpiryDuration = Duration.ofMillis(
                DEFAULT_NO_OF_SKIP_STAT_FOR_DEAD_WORKER_THRESHOLD * config.workerMetricsReporterFreqInMillis());
        this.supportCodeExpiryThreshold = Duration.ofMillis(config.workerMetricsReporterFreqInMillis() * 3);
        this.staleWorkerMetricsCleanupDuration = config.staleWorkerMetricsEntryCleanupDuration();
        this.executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("lam-data-load-%d").build());
    }

    @Override
    public LAMDataSnapshot loadData(final MetricsScope metricsScope) {
        final TableMigrationStatus status = tableMigrationStatusProvider.getTableMigrationStatus();

        // Step 1: Start lease table scan async on the executor service.
        final CompletableFuture<Map<EntityType, EntityScanList>> leaseTableFuture = CompletableFuture.supplyAsync(
                () -> loadWithRetry(() -> entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS)),
                executorService);

        // Step 2: Load legacy worker metrics synchronously on the calling thread (small table).
        final List<WorkerMetricStats> legacyTableWorkerMetrics;
        if (shouldScanLegacyTable(status)) {
            legacyTableWorkerMetrics =
                    loadWithRetry(workerMetricsDAO.getLegacyTableDaoDelegate()::getAllWorkerMetricStats);
        } else {
            legacyTableWorkerMetrics = Collections.emptyList();
        }

        // Step 3: Wait for the lease table scan to complete
        final Map<EntityType, EntityScanList> scanResults = leaseTableFuture.join();
        final EntityScanList leaseScanList = scanResults.getOrDefault(
                EntityType.LEASE, EntityScanList.builder().build());
        final EntityScanList metricsScanList = scanResults.getOrDefault(
                EntityType.WORKER_METRIC_STATS, EntityScanList.builder().build());

        final List<Lease> leases = extractLeases(leaseScanList);
        final List<WorkerMetricStats> leaseTableWorkerMetrics = extractWorkerMetrics(metricsScanList);
        final List<String> leaseDeserializationFailures = leaseScanList.getDeserializationFailures();

        // Step 4: Log and emit deserialization failures
        if (!leaseDeserializationFailures.isEmpty()) {
            log.warn("Leases that failed deserialization: {}", leaseDeserializationFailures);
            MetricsUtil.addCount(
                    metricsScope,
                    "LeaseDeserializationFailureCount",
                    leaseDeserializationFailures.size(),
                    MetricsLevel.SUMMARY);
        }

        // Step 5: Compute and publish migration summary (only active/non-expired workers count)
        publishMigrationSummary(leases, leaseTableWorkerMetrics, legacyTableWorkerMetrics);

        // Step 6: Merge, validate, filter worker metrics
        final List<WorkerMetricStats> allRawMetrics = new ArrayList<>(leaseTableWorkerMetrics);
        allRawMetrics.addAll(legacyTableWorkerMetrics);

        // Filter out invalid entries — log and emit metric
        final List<String> invalidWorkerIds = allRawMetrics.stream()
                .filter(wm -> !wm.isValidWorkerMetric())
                .map(WorkerMetricStats::getWorkerId)
                .collect(Collectors.toList());
        if (!invalidWorkerIds.isEmpty()) {
            log.warn("List of workerIds with invalid entries: {}", invalidWorkerIds);
            metricsScope.addData(
                    "NumWorkersWithInvalidEntry", invalidWorkerIds.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);
        }

        final List<WorkerMetricStats> validMetrics = allRawMetrics.stream()
                .filter(WorkerMetricStats::isValidWorkerMetric)
                .collect(Collectors.toList());

        // Step 7: Delete stale entries async on the executor service
        deleteStaleWorkerMetricsAsync(leaseTableWorkerMetrics, legacyTableWorkerMetrics);

        // Step 8: Return valid, non-expired metrics.
        final List<WorkerMetricStats> activeWorkerMetrics = validMetrics.stream()
                .filter(wm -> !wm.isExpired(workerMetricsExpiryDuration))
                .collect(Collectors.toList());

        log.info(
                "LAMDataManager loaded {} leases, {} raw worker metrics, {} active worker metrics "
                        + "(migration status: {})",
                leases.size(),
                allRawMetrics.size(),
                activeWorkerMetrics.size(),
                status);

        return LAMDataSnapshot.builder()
                .leases(leases)
                .leaseDeserializationFailures(leaseDeserializationFailures)
                .workerMetricStats(activeWorkerMetrics)
                .build();
    }

    /**
     * Deletes stale worker metrics entries asynchronously.
     * Uses the correct delegate based on where the metric originated from.
     */
    private void deleteStaleWorkerMetricsAsync(
            final List<WorkerMetricStats> leaseTableMetrics, final List<WorkerMetricStats> legacyTableMetrics) {

        final List<WorkerMetricStats> staleFromLeaseTable = leaseTableMetrics.stream()
                .filter(wm -> wm.isStale(staleWorkerMetricsCleanupDuration))
                .collect(Collectors.toList());

        final List<WorkerMetricStats> staleFromLegacyTable = legacyTableMetrics.stream()
                .filter(wm -> wm.isStale(staleWorkerMetricsCleanupDuration))
                .collect(Collectors.toList());

        if (staleFromLeaseTable.isEmpty() && staleFromLegacyTable.isEmpty()) {
            return;
        }

        log.info(
                "Deleting stale worker metrics: {} from lease table, {} from legacy table",
                staleFromLeaseTable.size(),
                staleFromLegacyTable.size());

        // Fire-and-forget async deletion on the executor service
        // worker metrics DAO only does routing based on table migration state for
        // delete and update you need to perform it on the table the entity came from
        // handle delete by manual routing here. Potentially can add DataSource in the
        // entity to indicate which table it came from just use the workerMetricsDAO
        // to delete.
        CompletableFuture.runAsync(
                () -> {
                    for (final WorkerMetricStats wm : staleFromLeaseTable) {
                        try {
                            workerMetricsDAO.getLeaseTableDaoDelegate().deleteMetrics(wm);
                        } catch (final Exception e) {
                            log.warn("Failed to delete stale worker metric {} from lease table", wm.getWorkerId(), e);
                        }
                    }
                    for (final WorkerMetricStats wm : staleFromLegacyTable) {
                        try {
                            workerMetricsDAO.getLegacyTableDaoDelegate().deleteMetrics(wm);
                        } catch (final Exception e) {
                            log.warn("Failed to delete stale worker metric {} from legacy table", wm.getWorkerId(), e);
                        }
                    }
                },
                executorService);
    }

    private boolean shouldScanLegacyTable(final TableMigrationStatus status) {
        return status != TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE
                && workerMetricsDAO.getLegacyTableDaoDelegate() != null;
    }

    private <T> T loadWithRetry(final Callable<T> loadFunction) {
        int retryAttempt = 0;
        while (true) {
            try {
                return loadFunction.call();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            } catch (final Exception e) {
                if (retryAttempt < DDB_LOAD_RETRY_ATTEMPT) {
                    log.warn("Failed to load, retrying (attempt {})", retryAttempt + 1, e);
                    retryAttempt++;
                } else {
                    throw new CompletionException(e);
                }
            }
        }
    }

    @Override
    public void shutdown() {
        executorService.shutdownNow();
        log.info("MigrationAwareLAMDataManager executor service shut down.");
    }

    private void publishMigrationSummary(
            final List<Lease> leases,
            final List<WorkerMetricStats> leaseTableMetrics,
            final List<WorkerMetricStats> legacyTableMetrics) {

        // Only consider unexpired leases for counting lease owners
        final Set<String> unexpiredLeaseOwners = leases.stream()
                .filter(lease -> !lease.isExpiredOrUnassigned())
                .map(Lease::leaseOwner)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        // All unique lease owners (regardless of expiry) - mirrors MigrationReadyMonitor's
        // getUniqueLeaseOwnersFromLeaseTable behavior
        final Set<String> allLeaseOwners =
                leases.stream().map(Lease::leaseOwner).filter(Objects::nonNull).collect(Collectors.toSet());

        final Set<String> activeWorkersInLeaseTable = leaseTableMetrics.stream()
                .filter(wm -> !wm.isExpired(workerMetricsExpiryDuration))
                .map(WorkerMetricStats::getWorkerId)
                .collect(Collectors.toSet());

        final Set<String> activeWorkersInLegacyTable = legacyTableMetrics.stream()
                .filter(wm -> !wm.isExpired(workerMetricsExpiryDuration))
                .map(WorkerMetricStats::getWorkerId)
                .collect(Collectors.toSet());

        final Set<String> allActiveWorkersWithMetrics = new HashSet<>(activeWorkersInLeaseTable);
        allActiveWorkersWithMetrics.addAll(activeWorkersInLegacyTable);

        // Count how many lease owners are emitting active metrics (in either table)
        // This mirrors the core check in MigrationReadyMonitor.areLeaseOwnersEmittingWorkerMetrics
        final long leaseOwnersWithActiveMetricsCount = allLeaseOwners.stream()
                .filter(allActiveWorkersWithMetrics::contains)
                .count();

        // Compute min support code inline (merged from FleetSupportCodeEvaluator)
        final int minSupportCode = computeMinSupportCode(unexpiredLeaseOwners, leaseTableMetrics, legacyTableMetrics);

        final TableMigrationSummary summary = TableMigrationSummary.builder()
                .totalActiveWorkersWithMetrics(allActiveWorkersWithMetrics.size())
                .activeWorkersWithMetricsInLeaseTable(activeWorkersInLeaseTable.size())
                .activeWorkersWithMetricsInLegacyTable(activeWorkersInLegacyTable.size())
                .workersWithUnexpiredLeases(unexpiredLeaseOwners.size())
                .totalWorkersWithLeases(allLeaseOwners.size())
                .leaseOwnersWithActiveMetrics((int) leaseOwnersWithActiveMetricsCount)
                .minSupportCode(minSupportCode)
                .build();

        log.info("Table migration summary: {}", summary);
        migrationSummaryConsumer.accept(summary);
    }

    /**
     * Computes the minimum support code across all lease-owning workers by cross-referencing
     * lease owners with their worker metrics support info.
     *
     * <p>Classification logic for each lease-owning worker:</p>
     * <ul>
     *   <li>If lease owner has no worker metrics entry → min support = 0</li>
     *   <li>If supportCode is null → min support = 0</li>
     *   <li>If supportCodeUpdateEpochSeconds is fresh → use its support code value</li>
     *   <li>If supportCodeUpdateEpochSeconds is expired → min support = 0</li>
     * </ul>
     *
     * @return min support code, 0 if any worker blocks, -1 if no lease owners to evaluate
     */
    private int computeMinSupportCode(
            final Set<String> leaseOwnerWorkerIds,
            final List<WorkerMetricStats> leaseTableMetrics,
            final List<WorkerMetricStats> legacyTableMetrics) {

        if (leaseOwnerWorkerIds.isEmpty()) {
            return -1;
        }

        // Build support info map: workerId -> WorkerSupportInfo (most recent heartbeat wins)
        final Map<String, WorkerMetricStats.WorkerSupportInfo> supportInfoByWorkerId = new HashMap<>();
        final List<WorkerMetricStats> allMetrics = new ArrayList<>(leaseTableMetrics);
        allMetrics.addAll(legacyTableMetrics);

        for (final WorkerMetricStats wm : allMetrics) {
            final String workerId = wm.getWorkerId();
            final WorkerMetricStats.WorkerSupportInfo candidate = WorkerMetricStats.WorkerSupportInfo.builder()
                    .supportCode(wm.getSupportCode())
                    .supportCodeUpdateEpochSeconds(wm.getSupportCodeUpdateEpochSeconds())
                    .build();

            supportInfoByWorkerId.merge(workerId, candidate, (existing, incoming) -> {
                final Long existingSlu = existing.getSupportCodeUpdateEpochSeconds();
                final Long incomingSlu = incoming.getSupportCodeUpdateEpochSeconds();
                if (existingSlu == null) {
                    return incoming;
                }
                if (incomingSlu == null) {
                    return existing;
                }
                return incomingSlu > existingSlu ? incoming : existing;
            });
        }

        // Evaluate min support code across lease owners
        int minSupport = Integer.MAX_VALUE;
        for (final String leaseOwner : leaseOwnerWorkerIds) {
            final WorkerMetricStats.WorkerSupportInfo info = supportInfoByWorkerId.get(leaseOwner);

            if (info == null) {
                log.info("computeMinSupportCode: lease owner '{}' has no worker metrics entry, min=0.", leaseOwner);
                return 0;
            }

            final Integer supportCode = info.getSupportCode();
            if (supportCode == null) {
                log.info("computeMinSupportCode: lease owner '{}' has no support code, min=0.", leaseOwner);
                return 0;
            }

            if (!info.isSupportCodeExpired(supportCodeExpiryThreshold)) {
                minSupport = Math.min(minSupport, supportCode);
            } else {
                log.info(
                        "computeMinSupportCode: lease owner '{}' has expired support code heartbeat, min=0.",
                        leaseOwner);
                return 0;
            }
        }

        final int result = (minSupport == Integer.MAX_VALUE) ? -1 : minSupport;
        log.info("computeMinSupportCode: result={} (leaseOwners={})", result, leaseOwnerWorkerIds.size());
        return result;
    }

    private List<Lease> extractLeases(final EntityScanList scanList) {
        final List<Entity> leaseEntities = scanList.getEntities();
        final List<Lease> leases = new ArrayList<>(leaseEntities.size());
        for (final Entity entity : leaseEntities) {
            if (entity instanceof Lease) {
                leases.add((Lease) entity);
            }
        }
        return leases;
    }

    private List<WorkerMetricStats> extractWorkerMetrics(final EntityScanList scanList) {
        final List<Entity> metricEntities = scanList.getEntities();
        final List<WorkerMetricStats> metrics = new ArrayList<>(metricEntities.size());
        for (final Entity entity : metricEntities) {
            if (entity instanceof WorkerMetricStats) {
                metrics.add((WorkerMetricStats) entity);
            }
        }
        return metrics;
    }
}
