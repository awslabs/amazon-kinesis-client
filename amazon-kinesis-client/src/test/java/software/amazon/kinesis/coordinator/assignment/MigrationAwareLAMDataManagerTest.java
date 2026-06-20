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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatus;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatusProvider;
import software.amazon.kinesis.coordinator.migration.TableMigrationSummary;
import software.amazon.kinesis.leases.EntityDAO;
import software.amazon.kinesis.leases.EntityDAO.Entity;
import software.amazon.kinesis.leases.EntityDAO.EntityScanList;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.NullMetricsScope;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;
import software.amazon.kinesis.worker.metricstats.delegate.LeaseTableWorkerMetricStatsDAODelegate;
import software.amazon.kinesis.worker.metricstats.delegate.LegacyTableWorkerMetricStatsDAODelegate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MigrationAwareLAMDataManagerTest {

    @Mock
    private EntityDAO entityDAO;

    @Mock
    private WorkerMetricStatsDAO workerMetricsDAO;

    @Mock
    private LegacyTableWorkerMetricStatsDAODelegate legacyDelegate;

    @Mock
    private LeaseTableWorkerMetricStatsDAODelegate leaseTableDelegate;

    @Mock
    private TableMigrationStatusProvider tableMigrationStatusProvider;

    private AtomicReference<TableMigrationSummary> capturedSummary;
    private Consumer<TableMigrationSummary> summaryConsumer;

    private MigrationAwareLAMDataManager manager;

    private static final long REPORTER_FREQ_MILLIS = Duration.ofHours(1).toMillis();

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        capturedSummary = new AtomicReference<>();
        summaryConsumer = capturedSummary::set;
        when(workerMetricsDAO.getLegacyTableDaoDelegate()).thenReturn(legacyDelegate);
        when(workerMetricsDAO.getLeaseTableDaoDelegate()).thenReturn(leaseTableDelegate);
    }

    private MigrationAwareLAMDataManager createManager(TableMigrationStatus status) {
        when(tableMigrationStatusProvider.getTableMigrationStatus()).thenReturn(status);
        LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                new LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig();
        config.workerMetricsReporterFreqInMillis(REPORTER_FREQ_MILLIS);
        config.staleWorkerMetricsEntryCleanupDuration(Duration.ofDays(1000));
        return new MigrationAwareLAMDataManager(
                entityDAO,
                workerMetricsDAO,
                tableMigrationStatusProvider,
                summaryConsumer,
                config,
                MoreExecutors.newDirectExecutorService());
    }

    private Lease createLease(String leaseKey, String owner) {
        Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.leaseOwner(owner);
        lease.leaseCounter(10L);
        return lease;
    }

    private WorkerMetricStats createActiveWorkerMetrics(String workerId) {
        return WorkerMetricStats.LegacyWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of("C", ImmutableList.of(50D, 50D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private WorkerMetricStats createExpiredWorkerMetrics(String workerId) {
        return WorkerMetricStats.LegacyWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(0L) // epoch 0 = always expired
                .metricStats(ImmutableMap.of("C", ImmutableList.of(50D, 50D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private WorkerMetricStats createInvalidWorkerMetrics(String workerId) {
        // null lastUpdateTime makes it invalid
        return WorkerMetricStats.LegacyWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(null)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(50D, 50D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private WorkerMetricStats createWorkerWithSupportCode(String workerId, Integer supportCode, Long supportCodeEpoch) {
        return WorkerMetricStats.LegacyWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of("C", ImmutableList.of(50D, 50D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .supportCode(supportCode)
                .supportCodeUpdateEpochSeconds(supportCodeEpoch)
                .build();
    }

    private Map<EntityType, EntityScanList> buildScanResult(List<Lease> leases, List<WorkerMetricStats> workerMetrics) {
        Map<EntityType, EntityScanList> result = new EnumMap<>(EntityType.class);
        List<Entity> leaseEntities = new ArrayList<>(leases);
        List<Entity> metricEntities = new ArrayList<>(workerMetrics);
        result.put(
                EntityType.LEASE,
                EntityScanList.builder()
                        .entities(leaseEntities)
                        .deserializationFailures(Collections.emptyList())
                        .build());
        result.put(
                EntityType.WORKER_METRIC_STATS,
                EntityScanList.builder()
                        .entities(metricEntities)
                        .deserializationFailures(Collections.emptyList())
                        .build());
        return result;
    }

    @Test
    void loadData_migrationComplete_doesNotScanLegacyTable() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        Lease lease = createLease("lease1", "worker1");
        WorkerMetricStats wm = createActiveWorkerMetrics("worker1");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.singletonList(lease), Collections.singletonList(wm)));

        LAMDataSnapshot snapshot = manager.loadData(new NullMetricsScope());

        assertNotNull(snapshot);
        assertEquals(1, snapshot.getLeases().size());
        assertEquals(1, snapshot.getWorkerMetricStats().size());
        assertEquals("worker1", snapshot.getWorkerMetricStats().get(0).getWorkerId());
        // Verify legacy table was NOT scanned
        verify(legacyDelegate, never()).getAllWorkerMetricStats();
    }

    @Test
    void loadData_migrationNotComplete_scansBothTables() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);

        Lease lease = createLease("lease1", "worker1");
        WorkerMetricStats wmLeaseTable = createActiveWorkerMetrics("worker1");
        WorkerMetricStats wmLegacyTable = createActiveWorkerMetrics("worker2");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.singletonList(lease), Collections.singletonList(wmLeaseTable)));
        when(legacyDelegate.getAllWorkerMetricStats()).thenReturn(Collections.singletonList(wmLegacyTable));

        LAMDataSnapshot snapshot = manager.loadData(new NullMetricsScope());

        assertNotNull(snapshot);
        assertEquals(1, snapshot.getLeases().size());
        assertEquals(2, snapshot.getWorkerMetricStats().size());
        // Verify legacy table WAS scanned
        verify(legacyDelegate).getAllWorkerMetricStats();
    }

    @Test
    void loadData_filtersInvalidWorkerMetrics() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        WorkerMetricStats validWm = createActiveWorkerMetrics("validWorker");
        WorkerMetricStats invalidWm = createInvalidWorkerMetrics("invalidWorker");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.emptyList(), Arrays.asList(validWm, invalidWm)));

        LAMDataSnapshot snapshot = manager.loadData(new NullMetricsScope());

        // Only valid worker should be in the snapshot
        assertEquals(1, snapshot.getWorkerMetricStats().size());
        assertEquals("validWorker", snapshot.getWorkerMetricStats().get(0).getWorkerId());
    }

    @Test
    void loadData_filtersExpiredWorkerMetrics() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        WorkerMetricStats activeWm = createActiveWorkerMetrics("activeWorker");
        WorkerMetricStats expiredWm = createExpiredWorkerMetrics("expiredWorker");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.emptyList(), Arrays.asList(activeWm, expiredWm)));

        LAMDataSnapshot snapshot = manager.loadData(new NullMetricsScope());

        // Only active (non-expired) worker should be returned
        assertEquals(1, snapshot.getWorkerMetricStats().size());
        assertEquals("activeWorker", snapshot.getWorkerMetricStats().get(0).getWorkerId());
    }

    @Test
    void loadData_publishesMigrationSummary() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);

        Lease lease = createLease("lease1", "worker1");
        WorkerMetricStats wmLeaseTable = createActiveWorkerMetrics("worker1");
        WorkerMetricStats wmLegacyTable = createActiveWorkerMetrics("worker2");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.singletonList(lease), Collections.singletonList(wmLeaseTable)));
        when(legacyDelegate.getAllWorkerMetricStats()).thenReturn(Collections.singletonList(wmLegacyTable));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        // worker1 in lease table, worker2 in legacy table → 2 total active
        assertEquals(2, summary.getTotalActiveWorkersWithMetrics());
        assertEquals(1, summary.getActiveWorkersWithMetricsInLeaseTable());
        assertEquals(1, summary.getActiveWorkersWithMetricsInLegacyTable());
        assertEquals(1, summary.getWorkersWithUnexpiredLeases());
        // worker1 owns lease1 and worker1 has active metrics → 1 lease owner with active metrics
        assertEquals(1, summary.getTotalWorkersWithLeases());
        assertEquals(1, summary.getLeaseOwnersWithActiveMetrics());
    }

    @Test
    void loadData_migrationSummary_noLeaseOwners_minSupportCodeIsNegativeOne() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // No leases, so no lease owners to evaluate
        WorkerMetricStats wm = createActiveWorkerMetrics("worker1");
        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.emptyList(), Collections.singletonList(wm)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(-1, summary.getMinSupportCode());
    }

    @Test
    void loadData_migrationSummary_leaseOwnerWithNoMetrics_minSupportCodeIsZero() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // Lease owned by worker2 but no worker metrics for worker2
        Lease lease = createLease("lease1", "worker2");
        WorkerMetricStats wm = createActiveWorkerMetrics("worker1");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.singletonList(lease), Collections.singletonList(wm)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(0, summary.getMinSupportCode());
    }

    @Test
    void loadData_migrationSummary_allWorkersWithFreshSupportCode_returnsMinCode() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        long freshEpoch = Instant.now().getEpochSecond();
        WorkerMetricStats wm1 = createWorkerWithSupportCode("worker1", 2, freshEpoch);
        WorkerMetricStats wm2 = createWorkerWithSupportCode("worker2", 3, freshEpoch);
        Lease lease1 = createLease("lease1", "worker1");
        Lease lease2 = createLease("lease2", "worker2");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Arrays.asList(lease1, lease2), Arrays.asList(wm1, wm2)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(2, summary.getMinSupportCode());
    }

    @Test
    void loadData_migrationSummary_workerWithNullSupportCode_returnsZero() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        long freshEpoch = Instant.now().getEpochSecond();
        WorkerMetricStats wm1 = createWorkerWithSupportCode("worker1", null, freshEpoch);
        Lease lease1 = createLease("lease1", "worker1");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.singletonList(lease1), Collections.singletonList(wm1)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(0, summary.getMinSupportCode());
    }

    @Test
    void loadData_migrationSummary_workerWithExpiredSupportCode_returnsZero() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // Support code update epoch is very old (expired)
        long staleEpoch = Instant.now().minus(Duration.ofDays(10)).getEpochSecond();
        WorkerMetricStats wm1 = createWorkerWithSupportCode("worker1", 2, staleEpoch);
        Lease lease1 = createLease("lease1", "worker1");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.singletonList(lease1), Collections.singletonList(wm1)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(0, summary.getMinSupportCode());
    }

    @Test
    void loadData_deserializationFailures_reportedInSnapshot() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        Map<EntityType, EntityScanList> scanResult = new EnumMap<>(EntityType.class);
        scanResult.put(
                EntityType.LEASE,
                EntityScanList.builder()
                        .entities(Collections.emptyList())
                        .deserializationFailures(Arrays.asList("badLease1", "badLease2"))
                        .build());
        scanResult.put(
                EntityType.WORKER_METRIC_STATS,
                EntityScanList.builder()
                        .entities(Collections.emptyList())
                        .deserializationFailures(Collections.emptyList())
                        .build());

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(scanResult);

        LAMDataSnapshot snapshot = manager.loadData(new NullMetricsScope());

        assertEquals(2, snapshot.getLeaseDeserializationFailures().size());
        assertTrue(snapshot.getLeaseDeserializationFailures().contains("badLease1"));
        assertTrue(snapshot.getLeaseDeserializationFailures().contains("badLease2"));
    }

    @Test
    void loadData_entityDAOThrowsDependencyException_propagates() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenThrow(new DependencyException(new RuntimeException("DDB unavailable")));

        assertThrows(Exception.class, () -> manager.loadData(new NullMetricsScope()));
    }

    @Test
    void loadData_migrationDeployed_scansBothTables() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);

        WorkerMetricStats wmLeaseTable = createActiveWorkerMetrics("worker1");
        WorkerMetricStats wmLegacy = createActiveWorkerMetrics("worker2");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.emptyList(), Collections.singletonList(wmLeaseTable)));
        when(legacyDelegate.getAllWorkerMetricStats()).thenReturn(Collections.singletonList(wmLegacy));

        LAMDataSnapshot snapshot = manager.loadData(new NullMetricsScope());

        assertEquals(2, snapshot.getWorkerMetricStats().size());
        verify(legacyDelegate).getAllWorkerMetricStats();
    }

    @Test
    void loadData_emptyResults_returnsEmptySnapshot() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.emptyList(), Collections.emptyList()));

        LAMDataSnapshot snapshot = manager.loadData(new NullMetricsScope());

        assertNotNull(snapshot);
        assertTrue(snapshot.getLeases().isEmpty());
        assertTrue(snapshot.getWorkerMetricStats().isEmpty());
        assertTrue(snapshot.getLeaseDeserializationFailures().isEmpty());
    }

    @Test
    void loadData_legacyDelegateNull_doesNotScanLegacyTable() throws Exception {
        // Simulate no legacy delegate available
        when(workerMetricsDAO.getLegacyTableDaoDelegate()).thenReturn(null);
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);

        WorkerMetricStats wm = createActiveWorkerMetrics("worker1");
        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.emptyList(), Collections.singletonList(wm)));

        LAMDataSnapshot snapshot = manager.loadData(new NullMetricsScope());

        assertEquals(1, snapshot.getWorkerMetricStats().size());
        verify(legacyDelegate, never()).getAllWorkerMetricStats();
    }

    @Test
    void loadData_onlyUnownedLeases_workersWithUnexpiredLeasesIsZero() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // Create a lease with null owner (unassigned)
        Lease lease = new Lease();
        lease.leaseKey("lease1");

        WorkerMetricStats wm = createActiveWorkerMetrics("worker1");
        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.singletonList(lease), Collections.singletonList(wm)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(0, summary.getWorkersWithUnexpiredLeases());
    }

    @Test
    void loadData_mergedMetricsFromBothTables() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);

        // Same worker in both tables
        WorkerMetricStats wmLeaseTable = createActiveWorkerMetrics("worker1");
        WorkerMetricStats wmLegacyTable = createActiveWorkerMetrics("worker1");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.emptyList(), Collections.singletonList(wmLeaseTable)));
        when(legacyDelegate.getAllWorkerMetricStats()).thenReturn(Collections.singletonList(wmLegacyTable));

        LAMDataSnapshot snapshot = manager.loadData(new NullMetricsScope());

        // Both entries are returned - LAMDataSnapshot documents that callers handle dedup
        assertEquals(2, snapshot.getWorkerMetricStats().size());
    }

    @Test
    void loadData_migrationSummary_totalWorkersWithLeases_countsAllLeaseOwners() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // 3 leases owned by 2 distinct workers
        Lease lease1 = createLease("lease1", "worker1");
        Lease lease2 = createLease("lease2", "worker2");
        Lease lease3 = createLease("lease3", "worker1");
        WorkerMetricStats wm1 = createActiveWorkerMetrics("worker1");
        WorkerMetricStats wm2 = createActiveWorkerMetrics("worker2");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Arrays.asList(lease1, lease2, lease3), Arrays.asList(wm1, wm2)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        // 2 distinct lease owners
        assertEquals(2, summary.getTotalWorkersWithLeases());
    }

    @Test
    void loadData_migrationSummary_totalWorkersWithLeases_excludesNullOwners() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // One lease with owner, one unassigned (null owner)
        Lease assignedLease = createLease("lease1", "worker1");
        Lease unassignedLease = new Lease();
        unassignedLease.leaseKey("lease2");

        WorkerMetricStats wm = createActiveWorkerMetrics("worker1");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(
                        buildScanResult(Arrays.asList(assignedLease, unassignedLease), Collections.singletonList(wm)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        // Only 1 owner (null owners excluded)
        assertEquals(1, summary.getTotalWorkersWithLeases());
    }

    @Test
    void loadData_migrationSummary_leaseOwnersWithActiveMetrics_allOwnersEmitting() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);

        // 2 lease owners, both have active metrics
        Lease lease1 = createLease("lease1", "worker1");
        Lease lease2 = createLease("lease2", "worker2");
        WorkerMetricStats wm1 = createActiveWorkerMetrics("worker1");
        WorkerMetricStats wm2 = createActiveWorkerMetrics("worker2");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Arrays.asList(lease1, lease2), Collections.singletonList(wm1)));
        when(legacyDelegate.getAllWorkerMetricStats()).thenReturn(Collections.singletonList(wm2));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(2, summary.getTotalWorkersWithLeases());
        assertEquals(2, summary.getLeaseOwnersWithActiveMetrics());
        // All lease owners are emitting → these are equal
        assertEquals(summary.getTotalWorkersWithLeases(), summary.getLeaseOwnersWithActiveMetrics());
    }

    @Test
    void loadData_migrationSummary_leaseOwnersWithActiveMetrics_someOwnersNotEmitting() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // 3 lease owners: worker1, worker2, worker3
        // Only worker1 and worker3 have active metrics
        Lease lease1 = createLease("lease1", "worker1");
        Lease lease2 = createLease("lease2", "worker2");
        Lease lease3 = createLease("lease3", "worker3");
        WorkerMetricStats wm1 = createActiveWorkerMetrics("worker1");
        WorkerMetricStats wm3 = createActiveWorkerMetrics("worker3");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Arrays.asList(lease1, lease2, lease3), Arrays.asList(wm1, wm3)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(3, summary.getTotalWorkersWithLeases());
        // worker2 has no metrics → only 2 lease owners with active metrics
        assertEquals(2, summary.getLeaseOwnersWithActiveMetrics());
    }

    @Test
    void loadData_migrationSummary_leaseOwnersWithActiveMetrics_ownerHasExpiredMetrics() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // worker1 has active metrics, worker2 has expired metrics
        Lease lease1 = createLease("lease1", "worker1");
        Lease lease2 = createLease("lease2", "worker2");
        WorkerMetricStats wm1 = createActiveWorkerMetrics("worker1");
        WorkerMetricStats wm2 = createExpiredWorkerMetrics("worker2");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Arrays.asList(lease1, lease2), Arrays.asList(wm1, wm2)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(2, summary.getTotalWorkersWithLeases());
        // worker2's metrics are expired, so only worker1 counts
        assertEquals(1, summary.getLeaseOwnersWithActiveMetrics());
    }

    @Test
    void loadData_migrationSummary_leaseOwnersWithActiveMetrics_noLeases() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // No leases, but workers exist with metrics
        WorkerMetricStats wm1 = createActiveWorkerMetrics("worker1");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.emptyList(), Collections.singletonList(wm1)));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(0, summary.getTotalWorkersWithLeases());
        assertEquals(0, summary.getLeaseOwnersWithActiveMetrics());
    }

    @Test
    void loadData_migrationSummary_leaseOwnersWithActiveMetrics_metricsInLegacyTableCount() throws Exception {
        manager = createManager(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);

        // worker1 has lease but metrics only in legacy table
        Lease lease1 = createLease("lease1", "worker1");
        WorkerMetricStats wmLegacy = createActiveWorkerMetrics("worker1");

        when(entityDAO.scanEntities(EntityType.LEASE, EntityType.WORKER_METRIC_STATS))
                .thenReturn(buildScanResult(Collections.singletonList(lease1), Collections.emptyList()));
        when(legacyDelegate.getAllWorkerMetricStats()).thenReturn(Collections.singletonList(wmLegacy));

        manager.loadData(new NullMetricsScope());

        TableMigrationSummary summary = capturedSummary.get();
        assertNotNull(summary);
        assertEquals(1, summary.getTotalWorkersWithLeases());
        // worker1 has active metrics in legacy table → counts
        assertEquals(1, summary.getLeaseOwnersWithActiveMetrics());
        assertEquals(0, summary.getActiveWorkersWithMetricsInLeaseTable());
        assertEquals(1, summary.getActiveWorkersWithMetricsInLegacyTable());
    }
}
