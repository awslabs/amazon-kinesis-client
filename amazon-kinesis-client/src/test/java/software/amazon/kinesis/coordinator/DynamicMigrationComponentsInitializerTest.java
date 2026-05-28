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
package software.amazon.kinesis.coordinator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider.LeaseAssignmentMode;
import software.amazon.kinesis.coordinator.assignment.LeaseAssignmentManager;
import software.amazon.kinesis.coordinator.migration.ClientVersion;
import software.amazon.kinesis.leader.DynamoDBLockBasedLeaderDecider;
import software.amazon.kinesis.leader.MigrationAdaptiveLeaderDecider;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsManager;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class DynamicMigrationComponentsInitializerTest {

    private DynamicMigrationComponentsInitializer migrationInitializer;

    private final MetricsFactory mockMetricsFactory = new NullMetricsFactory();
    private final LeaseRefresher mockLeaseRefresher = mock(LeaseRefresher.class, Mockito.RETURNS_MOCKS);
    private final CoordinatorStateDAO mockCoordinatorStateDAO = mock(CoordinatorStateDAO.class, Mockito.RETURNS_MOCKS);
    private final ScheduledExecutorService mockWorkerMetricsScheduler =
            mock(ScheduledExecutorService.class, RETURNS_MOCKS);
    private final WorkerMetricStatsDAO mockWorkerMetricsDAO = mock(WorkerMetricStatsDAO.class, RETURNS_MOCKS);
    private final WorkerMetricStatsManager mockWorkerMetricsManager =
            mock(WorkerMetricStatsManager.class, RETURNS_MOCKS);
    private final ScheduledExecutorService mockLamThreadPool = mock(ScheduledExecutorService.class, RETURNS_MOCKS);
    private final LeaseAssignmentManager mockLam = mock(LeaseAssignmentManager.class, RETURNS_MOCKS);
    private final BiFunction<ScheduledExecutorService, LeaderDecider, LeaseAssignmentManager> mockLamCreator =
            mock(LeaseAssignmentManagerSupplier.class);
    private final MigrationAdaptiveLeaderDecider mockMigrationAdaptiveLeaderDecider =
            mock(MigrationAdaptiveLeaderDecider.class);
    private final Supplier<MigrationAdaptiveLeaderDecider> mockAdaptiveLeaderDeciderCreator =
            mock(MigrationAdaptiveLeaderDeciderSupplier.class);
    private final DeterministicShuffleShardSyncLeaderDecider mockDeterministicLeaderDecider =
            mock(DeterministicShuffleShardSyncLeaderDecider.class);
    private final Supplier<DeterministicShuffleShardSyncLeaderDecider> mockDeterministicLeaderDeciderCreator =
            mock(DeterministicShuffleShardSyncLeaderDeciderSupplier.class);
    private final DynamoDBLockBasedLeaderDecider mockDdbLockLeaderDecider = mock(DynamoDBLockBasedLeaderDecider.class);
    private final Supplier<DynamoDBLockBasedLeaderDecider> mockDdbLockBasedLeaderDeciderCreator =
            mock(DynamoDBLockBasedLeaderDeciderSupplier.class);
    private final String workerIdentifier = "TEST_WORKER_ID";
    private final WorkerUtilizationAwareAssignmentConfig workerUtilizationAwareAssignmentConfig =
            new WorkerUtilizationAwareAssignmentConfig();
    final MigrationAdaptiveLeaseAssignmentModeProvider mockConsumer =
            mock(MigrationAdaptiveLeaseAssignmentModeProvider.class);

    private static final String APPLICATION_NAME = "TEST_APPLICATION";

    @BeforeEach
    public void setup() {
        workerUtilizationAwareAssignmentConfig.workerMetricsTableConfig(new WorkerMetricsTableConfig(APPLICATION_NAME));
        when(mockLamCreator.apply(any(ScheduledExecutorService.class), any(LeaderDecider.class)))
                .thenReturn(mockLam);
        when(mockAdaptiveLeaderDeciderCreator.get()).thenReturn(mockMigrationAdaptiveLeaderDecider);
        when(mockDdbLockBasedLeaderDeciderCreator.get()).thenReturn(mockDdbLockLeaderDecider);
        when(mockDeterministicLeaderDeciderCreator.get()).thenReturn(mockDeterministicLeaderDecider);

        migrationInitializer = new DynamicMigrationComponentsInitializer(
                mockMetricsFactory,
                mockLeaseRefresher,
                mockCoordinatorStateDAO,
                mockWorkerMetricsScheduler,
                mockWorkerMetricsDAO,
                mockWorkerMetricsManager,
                mockLamThreadPool,
                mockLamCreator,
                mockAdaptiveLeaderDeciderCreator,
                mockDeterministicLeaderDeciderCreator,
                mockDdbLockBasedLeaderDeciderCreator,
                workerIdentifier,
                workerUtilizationAwareAssignmentConfig,
                mockConsumer);
    }

    @Test
    public void testInitialize_ClientVersion3_X() throws DependencyException {
        // Test initializing to verify correct leader decider is created
        migrationInitializer.initialize(ClientVersion.CLIENT_VERSION_3X);

        verify(mockWorkerMetricsManager).startManager();
        verify(mockDdbLockBasedLeaderDeciderCreator).get();
        verify(mockAdaptiveLeaderDeciderCreator, never()).get();
        verify(mockDeterministicLeaderDeciderCreator, never()).get();
        verify(mockLamCreator).apply(eq(mockLamThreadPool), eq(migrationInitializer.leaderDecider()));

        // verify LeaseAssignmentModeChange consumer initialization
        verify(mockConsumer).initialize(eq(false), eq(LeaseAssignmentMode.WORKER_UTILIZATION_AWARE_ASSIGNMENT));

        when(mockLeaseRefresher.waitUntilLeaseOwnerToLeaseKeyIndexExists(anyLong(), anyLong()))
                .thenReturn(true);

        // test initialization from state machine
        migrationInitializer.initializeClientVersionFor3x(ClientVersion.CLIENT_VERSION_INIT);
        verify(mockWorkerMetricsDAO).initialize();
        verify(mockWorkerMetricsScheduler).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
        // verify that GSI will be created if it doesn't exist
        verify(mockLeaseRefresher).createLeaseOwnerToLeaseKeyIndexIfNotExists();
        // and it will block for the creation
        verify(mockLeaseRefresher).waitUntilLeaseOwnerToLeaseKeyIndexExists(anyLong(), anyLong());
        verify(mockDdbLockLeaderDecider).initialize();
        verify(mockLam).start();
    }

    /**
     * exactly same as above except:
     * 1. migration adaptive leader decider will be created in addition to ddb lock leader decider.
     * 2. dynamicModeChangeSupportNeeded is returned as true to LeaseAssignmentModeChange notification consumer
     * 3. gsi creation is not triggered
     */
    @Test
    public void testInitialize_ClientVersion_3_xWithRollback() throws DependencyException {
        // Test initializing to verify correct leader decider is created
        migrationInitializer.initialize(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK);

        verify(mockWorkerMetricsManager).startManager();

        verify(mockDdbLockBasedLeaderDeciderCreator).get();
        verify(mockAdaptiveLeaderDeciderCreator).get();
        verify(mockDeterministicLeaderDeciderCreator, never()).get();
        verify(mockLamCreator).apply(eq(mockLamThreadPool), eq(migrationInitializer.leaderDecider()));

        // verify LeaseAssignmentModeChange consumer initialization
        verify(mockConsumer).initialize(eq(true), eq(LeaseAssignmentMode.WORKER_UTILIZATION_AWARE_ASSIGNMENT));

        // test initialization from state machine
        migrationInitializer.initializeClientVersionFor3xWithRollback(ClientVersion.CLIENT_VERSION_INIT);

        verify(mockWorkerMetricsDAO).initialize();
        verify(mockWorkerMetricsScheduler).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
        verify(mockLeaseRefresher, never()).createLeaseOwnerToLeaseKeyIndexIfNotExists();
        verify(mockLeaseRefresher, never()).waitUntilLeaseOwnerToLeaseKeyIndexExists(anyLong(), anyLong());
        verify(mockMigrationAdaptiveLeaderDecider).updateLeaderDecider(mockDdbLockLeaderDecider);
        verify(mockLam).start();
    }

    @ParameterizedTest
    @CsvSource({"CLIENT_VERSION_UPGRADE_FROM_2X", "CLIENT_VERSION_2X"})
    public void testInitialize_ClientVersion_All2_X(final ClientVersion clientVersion) throws DependencyException {
        // Test initializing to verify correct leader decider is created
        migrationInitializer.initialize(clientVersion);

        verify(mockWorkerMetricsManager).startManager();

        verify(mockDdbLockBasedLeaderDeciderCreator, never()).get();
        verify(mockAdaptiveLeaderDeciderCreator).get();
        verify(mockDeterministicLeaderDeciderCreator).get();
        verify(mockLamCreator).apply(eq(mockLamThreadPool), eq(migrationInitializer.leaderDecider()));

        // verify LeaseAssignmentModeChange consumer initialization
        verify(mockConsumer).initialize(eq(true), eq(LeaseAssignmentMode.DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT));

        // test initialization from state machine
        if (clientVersion == ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X) {
            migrationInitializer.initializeClientVersionForUpgradeFrom2x(ClientVersion.CLIENT_VERSION_INIT);
            // start worker stats and create gsi without waiting
            verify(mockWorkerMetricsDAO).initialize();
            verify(mockWorkerMetricsScheduler).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
            verify(mockLeaseRefresher).createLeaseOwnerToLeaseKeyIndexIfNotExists();
        } else {
            migrationInitializer.initializeClientVersionFor2x(ClientVersion.CLIENT_VERSION_INIT);
            verify(mockWorkerMetricsDAO, never()).initialize();
            verify(mockWorkerMetricsScheduler, never()).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
            verify(mockLeaseRefresher, never()).createLeaseOwnerToLeaseKeyIndexIfNotExists();
        }

        verify(mockLeaseRefresher, never()).waitUntilLeaseOwnerToLeaseKeyIndexExists(anyLong(), anyLong());
        verify(mockMigrationAdaptiveLeaderDecider).updateLeaderDecider(mockDeterministicLeaderDecider);
        verify(mockLam, never()).start();
    }

    @Test
    public void testShutdown() throws InterruptedException, DependencyException {
        when(mockLamThreadPool.awaitTermination(anyLong(), any())).thenReturn(true);
        when(mockWorkerMetricsScheduler.awaitTermination(anyLong(), any())).thenReturn(true);

        migrationInitializer.initialize(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X);
        migrationInitializer.shutdown();

        verify(mockLamThreadPool).shutdown();
        verify(mockWorkerMetricsScheduler).shutdown();

        verify(mockLam).stop();
        // leader decider is not shutdown from DynamicMigrationComponentsInitializer
        // scheduler does the shutdown
        // verify(migrationInitializer.leaderDecider()).shutdown();
        verify(mockWorkerMetricsManager).stopManager();
    }

    @Test
    public void initializationFails_WhenGsiIsNotActiveIn3_X() throws DependencyException {
        migrationInitializer.initialize(ClientVersion.CLIENT_VERSION_3X);
        // test initialization from state machine

        when(mockLeaseRefresher.waitUntilLeaseOwnerToLeaseKeyIndexExists(anyLong(), anyLong()))
                .thenReturn(false);

        assertThrows(
                DependencyException.class,
                () -> migrationInitializer.initializeClientVersionFor3x(ClientVersion.CLIENT_VERSION_INIT));
    }

    @Test
    public void initializationDoesNotFail_WhenGsiIsNotActiveIn3_XWithRollback() throws DependencyException {
        migrationInitializer.initialize(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK);
        // test initialization from state machine

        when(mockLeaseRefresher.waitUntilLeaseOwnerToLeaseKeyIndexExists(anyLong(), anyLong()))
                .thenReturn(false);

        assertDoesNotThrow(
                () -> migrationInitializer.initializeClientVersionFor3xWithRollback(ClientVersion.CLIENT_VERSION_INIT));
    }

    @Test
    public void testComponentsInitialization_AfterFlip() throws DependencyException {
        migrationInitializer.initialize(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X);
        migrationInitializer.initializeClientVersionForUpgradeFrom2x(ClientVersion.CLIENT_VERSION_INIT);

        // Test flip
        migrationInitializer.initializeClientVersionFor3xWithRollback(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X);

        // verify
        verify(mockLam).start();
        verify(mockConsumer).updateLeaseAssignmentMode(eq(LeaseAssignmentMode.WORKER_UTILIZATION_AWARE_ASSIGNMENT));
        verify(mockDdbLockBasedLeaderDeciderCreator).get();
        verify(mockMigrationAdaptiveLeaderDecider).updateLeaderDecider(eq(mockDdbLockLeaderDecider));
    }

    @Test
    public void testComponentsInitialization_AfterRollForward() throws DependencyException {
        final ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);

        doReturn(mockFuture)
                .when(mockWorkerMetricsScheduler)
                .scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        migrationInitializer.initialize(ClientVersion.CLIENT_VERSION_2X);
        migrationInitializer.initializeClientVersionFor2x(ClientVersion.CLIENT_VERSION_INIT);

        // test roll-forward
        reset(mockWorkerMetricsScheduler);
        reset(mockLeaseRefresher);
        migrationInitializer.initializeClientVersionForUpgradeFrom2x(ClientVersion.CLIENT_VERSION_2X);

        // verify
        verify(mockWorkerMetricsDAO).initialize(); // Worker metrics table must be recreated
        verify(mockWorkerMetricsScheduler) // Worker metrics reporting is started
                .scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        // GSI is recreated but does not wait for active status
        verify(mockLeaseRefresher).createLeaseOwnerToLeaseKeyIndexIfNotExists();
        verify(mockLeaseRefresher, never()).waitUntilLeaseTableExists(anyLong(), anyLong());
    }

    @Test
    public void testComponentsInitialization_Rollback_BeforeFlip() throws DependencyException {
        final ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        doReturn(mockFuture)
                .when(mockWorkerMetricsScheduler)
                .scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        migrationInitializer.initialize(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X);
        migrationInitializer.initializeClientVersionForUpgradeFrom2x(ClientVersion.CLIENT_VERSION_INIT);

        // test rollback before flip
        migrationInitializer.initializeClientVersionFor2x(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X);

        // verify
        verify(mockFuture).cancel(anyBoolean());
    }

    @Test
    public void testComponentsInitialization_Rollback_AfterFlip() throws DependencyException {
        final ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        doReturn(mockFuture)
                .when(mockWorkerMetricsScheduler)
                .scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        migrationInitializer.initialize(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK);
        migrationInitializer.initializeClientVersionFor3xWithRollback(ClientVersion.CLIENT_VERSION_INIT);

        // test rollback before flip
        migrationInitializer.initializeClientVersionFor2x(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK);

        // verify
        verify(mockFuture).cancel(anyBoolean());
        verify(mockConsumer).updateLeaseAssignmentMode(eq(LeaseAssignmentMode.DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT));
        verify(mockLam).stop();
        verify(mockDeterministicLeaderDeciderCreator).get();
        verify(mockMigrationAdaptiveLeaderDecider).updateLeaderDecider(mockDeterministicLeaderDecider);
    }

    @Test
    public void testWorkerMetricsReporting() throws DependencyException {
        final ArgumentCaptor<Runnable> argumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        final ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        doReturn(mockFuture)
                .when(mockWorkerMetricsScheduler)
                .scheduleAtFixedRate(argumentCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));
        when(mockWorkerMetricsManager.getOperatingRange()).thenReturn(new HashMap<String, List<Long>>() {
            {
                put("CPU", Collections.singletonList(80L));
            }
        });
        when(mockWorkerMetricsManager.computeMetrics()).thenReturn(new HashMap<String, List<Double>>() {
            {
                put("CPU", Arrays.asList(90.0, 85.0, 77.0, 91.0, 82.0));
            }
        });

        migrationInitializer.initialize(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK);
        migrationInitializer.initializeClientVersionFor3xWithRollback(ClientVersion.CLIENT_VERSION_INIT);

        // run the worker stats reporting thread
        argumentCaptor.getValue().run();

        // verify
        final ArgumentCaptor<WorkerMetricStats> statsCaptor = ArgumentCaptor.forClass(WorkerMetricStats.class);
        verify(mockWorkerMetricsDAO).updateMetrics(statsCaptor.capture());
        Assertions.assertEquals(workerIdentifier, statsCaptor.getValue().getWorkerId());
        Assertions.assertEquals(
                80L, statsCaptor.getValue().getOperatingRange().get("CPU").get(0));
        Assertions.assertEquals(
                90.0, statsCaptor.getValue().getMetricStats().get("CPU").get(0));
        Assertions.assertEquals(
                77.0, statsCaptor.getValue().getMetricStats().get("CPU").get(2));
    }

    private abstract static class DynamoDBLockBasedLeaderDeciderSupplier
            implements Supplier<DynamoDBLockBasedLeaderDecider> {}

    private abstract static class DeterministicShuffleShardSyncLeaderDeciderSupplier
            implements Supplier<DeterministicShuffleShardSyncLeaderDecider> {}

    private abstract static class MigrationAdaptiveLeaderDeciderSupplier
            implements Supplier<MigrationAdaptiveLeaderDecider> {}

    private abstract static class LeaseAssignmentManagerSupplier
            implements BiFunction<ScheduledExecutorService, LeaderDecider, LeaseAssignmentManager> {}
}
