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
package software.amazon.kinesis.coordinator.migration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.kinesis.coordinator.CoordinatorConfig.ClientVersionConfig;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.DynamicMigrationComponentsInitializer;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.coordinator.migration.MigrationReadyMonitorTest.TestDataType;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.coordinator.migration.MigrationState.MIGRATION_HASH_KEY;

@Slf4j
public class MigrationStateMachineTest {
    private static final String WORKER_ID = "MigrationStateMachineTestWorker";
    private static final long WORKER_STATS_EXPIRY_SECONDS = 1L;

    private MigrationStateMachine stateMachineUnderTest;
    private final MetricsFactory nullMetricsFactory = new NullMetricsFactory();
    private final Callable<Long> mockTimeProvider = mock(Callable.class, Mockito.RETURNS_MOCKS);
    private final LeaderDecider mockLeaderDecider = mock(LeaderDecider.class, Mockito.RETURNS_MOCKS);
    private final CoordinatorStateDAO mockCoordinatorStateDAO =
            Mockito.mock(CoordinatorStateDAO.class, Mockito.RETURNS_MOCKS);
    private final WorkerMetricStatsDAO mockWorkerMetricsDao = mock(WorkerMetricStatsDAO.class, Mockito.RETURNS_MOCKS);
    private final LeaseRefresher mockLeaseRefresher = mock(LeaseRefresher.class, Mockito.RETURNS_MOCKS);
    private final DynamicMigrationComponentsInitializer mockInitializer =
            mock(DynamicMigrationComponentsInitializer.class, Mockito.RETURNS_MOCKS);
    private final ScheduledExecutorService mockMigrationStateMachineThreadPool =
            mock(ScheduledExecutorService.class, Mockito.RETURNS_MOCKS);
    private final Random mockRandom = Mockito.mock(Random.class, Mockito.RETURNS_MOCKS);

    @BeforeEach
    public void setup() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(null);
        when(mockCoordinatorStateDAO.createCoordinatorStateIfNotExists(any())).thenReturn(true);
        when(mockLeaderDecider.isLeader(eq(WORKER_ID))).thenReturn(true);
        when(mockTimeProvider.call()).thenReturn(10000L);
        when(mockInitializer.leaderDecider()).thenReturn(mockLeaderDecider);
        when(mockInitializer.workerIdentifier()).thenReturn(WORKER_ID);
        when(mockInitializer.workerMetricsDAO()).thenReturn(mockWorkerMetricsDao);
        when(mockInitializer.workerMetricsExpirySeconds()).thenReturn(WORKER_STATS_EXPIRY_SECONDS);
        when(mockInitializer.leaseRefresher()).thenReturn(mockLeaseRefresher);
    }

    @BeforeAll
    public static void beforeAll() {
        MigrationReadyMonitorTest.populateTestDataMap();
    }

    private MigrationStateMachine getStateMachineUnderTest(final ClientVersionConfig config)
            throws DependencyException {
        final MigrationStateMachine migrationStateMachine = new MigrationStateMachineImpl(
                nullMetricsFactory,
                mockTimeProvider,
                mockCoordinatorStateDAO,
                mockMigrationStateMachineThreadPool,
                config,
                mockRandom,
                mockInitializer,
                WORKER_ID,
                Duration.ofMinutes(0).getSeconds());
        migrationStateMachine.initialize();
        return migrationStateMachine;
    }

    @ParameterizedTest
    @CsvSource({
        "CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X, CLIENT_VERSION_UPGRADE_FROM_2X",
        "CLIENT_VERSION_CONFIG_3X, CLIENT_VERSION_3X"
    })
    public void testStateMachineInitialization(
            final ClientVersionConfig config, final ClientVersion expectedStateMachineState) throws Exception {
        stateMachineUnderTest = getStateMachineUnderTest(config);
        Assertions.assertEquals(expectedStateMachineState, stateMachineUnderTest.getCurrentClientVersion());
    }

    @Test
    public void testMigrationReadyFlip() throws Exception {
        stateMachineUnderTest = getStateMachineUnderTest(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);

        // After initialization, state machine should start to monitor for upgrade readiness
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockMigrationStateMachineThreadPool, times(2))
                .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        initiateAndTestFlip(runnableCaptor);
    }

    @Test
    public void testRollbackAfterFlip() throws Exception {
        stateMachineUnderTest = getStateMachineUnderTest(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);

        // After initialization, state machine should start to monitor for upgrade readiness
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockMigrationStateMachineThreadPool, times(2))
                .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        initiateAndTestFlip(runnableCaptor);

        // A new version monitor must be created after flip
        runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockMigrationStateMachineThreadPool, timeout(100).times(1))
                .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        final Runnable rollbackMonitorRunnable = runnableCaptor.getValue();
        initiateAndTestRollBack(rollbackMonitorRunnable);
    }

    @Test
    public void testRollForward() throws Exception {
        stateMachineUnderTest = getStateMachineUnderTest(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);

        // After initialization, state machine should start to monitor for upgrade readiness
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockMigrationStateMachineThreadPool, times(2))
                .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));
        initiateAndTestFlip(runnableCaptor);

        // A new version monitor must be created after flip
        runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockMigrationStateMachineThreadPool, times(1))
                .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        final Runnable rollbackMonitorRunnable = runnableCaptor.getValue();
        initiateAndTestRollBack(rollbackMonitorRunnable);

        // A new version monitor must be created after rollback
        runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockMigrationStateMachineThreadPool, times(1))
                .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        final Runnable rollforwardMonitorRunnable = runnableCaptor.getValue();
        initiateAndTestRollForward(rollforwardMonitorRunnable);
    }

    @Test
    public void testRollbackBeforeFlip() throws Exception {
        stateMachineUnderTest = getStateMachineUnderTest(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);

        // After initialization, state machine should start to monitor for upgrade readiness
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockMigrationStateMachineThreadPool, times(2))
                .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        initiateAndTestRollbackBeforeFlip(runnableCaptor);
    }

    @Test
    public void successfulUpgradeAfterFlip() throws Exception {
        stateMachineUnderTest = getStateMachineUnderTest(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);

        // After initialization, state machine should start to monitor for upgrade readiness
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockMigrationStateMachineThreadPool, times(2))
                .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        initiateAndTestFlip(runnableCaptor);

        // A new version monitor must be created after flip
        runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockMigrationStateMachineThreadPool, timeout(100).times(1))
                .scheduleWithFixedDelay(runnableCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        final Runnable successfulUpgradeMonitor = runnableCaptor.getValue();

        initiateAndTestSuccessfulUpgrade(successfulUpgradeMonitor);
    }

    private void initiateAndTestFlip(final ArgumentCaptor<Runnable> runnableCaptor) throws Exception {
        final Runnable migrationReadyMonitorRunnable =
                runnableCaptor.getAllValues().get(0) instanceof MigrationReadyMonitor
                        ? runnableCaptor.getAllValues().get(0)
                        : runnableCaptor.getAllValues().get(1);

        final Runnable versionChangeMonitorRunnable =
                runnableCaptor.getAllValues().get(0) instanceof ClientVersionChangeMonitor
                        ? runnableCaptor.getAllValues().get(0)
                        : runnableCaptor.getAllValues().get(1);

        when(mockLeaseRefresher.isLeaseOwnerToLeaseKeyIndexActive()).thenReturn(true);
        when(mockLeaseRefresher.listLeases())
                .thenReturn(MigrationReadyMonitorTest.TEST_DATA_MAP
                        .get(TestDataType.WORKER_READY_CONDITION_MET)
                        .getLeaseList());
        when(mockWorkerMetricsDao.getAllWorkerMetricStats())
                .thenReturn(MigrationReadyMonitorTest.TEST_DATA_MAP
                        .get(TestDataType.WORKER_READY_CONDITION_MET)
                        .getWorkerMetrics());

        // during flip, the migrationReady callback handling will update MigrationState.
        // mock a successful update of MigrationState and return the captured state back
        // when clientVersion change monitor tried to read the value from DDB.

        final ArgumentCaptor<MigrationState> stateCaptor = ArgumentCaptor.forClass(MigrationState.class);
        when(mockCoordinatorStateDAO.updateCoordinatorStateWithExpectation(stateCaptor.capture(), any(HashMap.class)))
                .thenReturn(true);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY))
                .thenAnswer(invocation -> stateCaptor.getValue());

        reset(mockMigrationStateMachineThreadPool);
        log.info("TestLog ----------- Initiate a flip -------------");
        // Invoke the monitor callbacks so the version flips to 3.x with rollback
        migrationReadyMonitorRunnable.run();
        Assertions.assertEquals(
                ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK,
                stateCaptor.getValue().getClientVersion());

        versionChangeMonitorRunnable.run();
        Assertions.assertEquals(
                ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK, stateMachineUnderTest.getCurrentClientVersion());

        verify(mockInitializer)
                .initializeClientVersionFor3xWithRollback(eq(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X));
        log.info("TestLog ----------- flip done -------------");
    }

    private void initiateAndTestRollbackBeforeFlip(final ArgumentCaptor<Runnable> runnableCaptor) throws Exception {
        final Runnable versionChangeMonitorRunnable =
                runnableCaptor.getAllValues().get(0) instanceof ClientVersionChangeMonitor
                        ? runnableCaptor.getAllValues().get(0)
                        : runnableCaptor.getAllValues().get(1);

        final MigrationState state =
                new MigrationState(MIGRATION_HASH_KEY, WORKER_ID).update(ClientVersion.CLIENT_VERSION_2X, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(state);
        reset(mockMigrationStateMachineThreadPool);
        reset(mockInitializer);
        log.info("TestLog ----------- Initiate rollback before flip -------------");
        versionChangeMonitorRunnable.run();
        log.info("TestLog ----------- rollback before flip done -------------");
        Assertions.assertEquals(ClientVersion.CLIENT_VERSION_2X, stateMachineUnderTest.getCurrentClientVersion());
        verify(mockInitializer).initializeClientVersionFor2x(eq(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X));
    }

    private void initiateAndTestRollBack(final Runnable rollbackMonitorRunnable) throws Exception {
        final MigrationState state =
                new MigrationState(MIGRATION_HASH_KEY, WORKER_ID).update(ClientVersion.CLIENT_VERSION_2X, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(state);
        reset(mockMigrationStateMachineThreadPool);
        reset(mockInitializer);
        log.info("TestLog ----------- Initiate rollback -------------");
        rollbackMonitorRunnable.run();
        log.info("TestLog ----------- rollback done -------------");
        Assertions.assertEquals(ClientVersion.CLIENT_VERSION_2X, stateMachineUnderTest.getCurrentClientVersion());
        verify(mockInitializer).initializeClientVersionFor2x(eq(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK));
    }

    private void initiateAndTestRollForward(final Runnable rollforwardMonitorRunnable) throws Exception {
        final MigrationState state = new MigrationState(MIGRATION_HASH_KEY, WORKER_ID)
                .update(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(state);
        reset(mockMigrationStateMachineThreadPool);
        reset(mockInitializer);
        log.info("TestLog ----------- Initiate roll-forward -------------");
        rollforwardMonitorRunnable.run();
        log.info("TestLog ----------- roll-forward done -------------");
        Assertions.assertEquals(
                ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, stateMachineUnderTest.getCurrentClientVersion());
        verify(mockInitializer).initializeClientVersionForUpgradeFrom2x(eq(ClientVersion.CLIENT_VERSION_2X));
    }

    private void initiateAndTestSuccessfulUpgrade(final Runnable successfulUpgradeMonitor) throws Exception {
        final MigrationState state =
                new MigrationState(MIGRATION_HASH_KEY, WORKER_ID).update(ClientVersion.CLIENT_VERSION_3X, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(state);
        reset(mockMigrationStateMachineThreadPool);
        reset(mockInitializer);
        log.info("TestLog ----------- Initiate successful upgrade -------------");
        successfulUpgradeMonitor.run();
        log.info("TestLog ----------- successful upgrade done -------------");
        Assertions.assertEquals(ClientVersion.CLIENT_VERSION_3X, stateMachineUnderTest.getCurrentClientVersion());
        verify(mockInitializer).initializeClientVersionFor3x(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK);
    }
}
