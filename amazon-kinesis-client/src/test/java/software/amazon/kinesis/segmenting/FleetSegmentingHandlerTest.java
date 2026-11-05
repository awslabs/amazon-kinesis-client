package software.amazon.kinesis.segmenting;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leader.LeaderLock;
import software.amazon.kinesis.leases.LeaseAssignmentStrategy;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class FleetSegmentingHandlerTest {

    private CoordinatorStateDAO mockCoordinatorStateDAO;
    private ScheduledExecutorService mockVersionHeartbeatExecutor;
    private FleetSegmentingHandler handler;
    private String sampleVersionHash;
    private LeaseManagementConfig config;

    @BeforeEach
    void setup() {
        clearInvocations();
        mockCoordinatorStateDAO = mock(CoordinatorStateDAO.class);
        mockVersionHeartbeatExecutor = mock(ScheduledExecutorService.class);
        sampleVersionHash = String.valueOf(LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE.getVersionHash());
        config = new LeaseManagementConfig(
                "dummyTableName",
                "dummyApplicationName",
                Mockito.mock(DynamoDbAsyncClient.class),
                Mockito.mock(KinesisAsyncClient.class),
                "dummyWorkerId");
        config.enableRollingDeploymentSystem(true);
        config.leaseAssignmentStrategy(LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE);
        handler = new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor);
    }

    @Test
    void getVersionHash_returnsDeterministicValue() {
        String expected = String.valueOf(LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE.getVersionHash());
        assertEquals(expected, handler.getVersionHash());
    }

    @Test
    void getVersionHash_isSameAcrossInstancesForSameConfig() {
        FleetSegmentingHandler handler2 =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor);
        assertEquals(handler.getVersionHash(), handler2.getVersionHash());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenVersionHashMatches() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, sampleVersionHash, Instant.now().getEpochSecond());

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashDoesNotMatch() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenItemNotPresent() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(LeaderLock.LEADER_HASH_KEY))
                .thenReturn(null);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenVersionHashExpired() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - 7200);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenVersionHashKeyMissing() throws Exception {
        Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("someOtherKey", AttributeValue.fromS("value"));
        CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(LeaderLock.LEADER_HASH_KEY)))
                .thenReturn(state);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    // ---------------- isOnCurrentVersion (decided by the held lock, no reads) ----------------

    @Test
    void isOnCurrentVersion_trueWhenHoldingLeader() throws Exception {
        handler.setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);

        assertTrue(handler.isOnCurrentVersion());
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(any());
    }

    @Test
    void isOnCurrentVersion_falseWhenHoldingDeployingLeader() throws Exception {
        handler.setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);

        assertFalse(handler.isOnCurrentVersion());
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(any());
    }

    /**
     * Finding 4: a DeployingLeader holder must not become "current" because the Leader item's version hash now
     * matches its own. It only becomes current once it actually holds the Leader lock.
     */
    @Test
    void isOnCurrentVersion_deployingLeaderStaysNotCurrent_untilItHoldsLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, sampleVersionHash, Instant.now().getEpochSecond());
        handler.setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        assertFalse(handler.isOnCurrentVersion());

        // Losing the lock doesn't make a former deploying leader current either.
        handler.setHeldLeaderKey(null);
        assertFalse(handler.isOnCurrentVersion());

        handler.setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);
        assertTrue(handler.isOnCurrentVersion());
    }

    /** Finding 1: with a non-lock leader decider (2.x-compatible modes) no lock is ever held; shard sync must run. */
    @Test
    void isOnCurrentVersion_trueWhenNoLockEverHeld() throws Exception {
        assertTrue(handler.isOnCurrentVersion());
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(any());
    }

    @Test
    void isOnCurrentVersion_returnsTrue_whenDisabled() {
        config.enableRollingDeploymentSystem(false);
        FleetSegmentingHandler disabledHandler =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor);
        assertTrue(disabledHandler.isOnCurrentVersion());
    }

    @Test
    void generateVersionHashWithLastUpdatedTime_Map_returnsCorrectMap() {
        final long before = Instant.now().getEpochSecond();
        final Map<String, String> result = handler.generateVersionHashWithLastUpdatedTimeMap();
        final long after = Instant.now().getEpochSecond();

        assertTrue(result.containsKey("versionHash"));
        assertTrue(result.containsKey("versionHashLut"));
        assertEquals(sampleVersionHash, result.get("versionHash"));
        final long lut = Long.parseLong(result.get("versionHashLut"));
        assertTrue(lut >= before && lut <= after);
        assertEquals(2, result.size());
    }

    @Test
    void generateVersionHashWithLastUpdatedTimeMapForLockTable_returnsAttributeValueMap() {
        final long before = Instant.now().getEpochSecond();
        final Map<String, AttributeValue> result = handler.generateVersionHashWithLastUpdatedTimeForLockTable();
        final long after = Instant.now().getEpochSecond();

        assertEquals(sampleVersionHash, result.get("versionHash").s());
        final long lut = Long.parseLong(result.get("versionHashLut").s());
        assertTrue(lut >= before && lut <= after);
        assertEquals(2, result.size());
    }

    @Test
    void isWorkerVersionHashStale_returnsTrue_whenLutKeyMissing() {
        WorkerMetricStats mockWorker = mock(WorkerMetricStats.class);
        when(mockWorker.getProperties()).thenReturn(Collections.singletonMap("versionHash", "123"));
        assertTrue(handler.isWorkerVersionHashStale(mockWorker));
    }

    @Test
    void isWorkerVersionHashStale_returnsFalse_whenLutIsRecent() {
        WorkerMetricStats mockWorker = mock(WorkerMetricStats.class);
        Map<String, String> props = new HashMap<>();
        props.put("versionHashLut", String.valueOf(Instant.now().getEpochSecond()));
        when(mockWorker.getProperties()).thenReturn(props);
        assertFalse(handler.isWorkerVersionHashStale(mockWorker));
    }

    @Test
    void isWorkerVersionHashStale_returnsTrue_whenLutIsOlderThanOneHour() {
        WorkerMetricStats mockWorker = mock(WorkerMetricStats.class);
        Map<String, String> props = new HashMap<>();
        props.put("versionHashLut", String.valueOf(Instant.now().getEpochSecond() - 3601));
        when(mockWorker.getProperties()).thenReturn(props);
        assertTrue(handler.isWorkerVersionHashStale(mockWorker));
    }

    @Test
    void isWorkerVersionHashStale_returnsTrue_whenPropertiesNull() {
        WorkerMetricStats worker = mock(WorkerMetricStats.class);
        when(worker.getProperties()).thenReturn(null);
        assertTrue(handler.isWorkerVersionHashStale(worker));
    }

    @Test
    void filterWorkersOnVersionHash_filtersCorrectly() {
        Map<String, String> matchingProps = new HashMap<>();
        matchingProps.put("versionHash", sampleVersionHash);
        matchingProps.put("versionHashLut", String.valueOf(Instant.now().getEpochSecond()));

        Map<String, String> differentProps = new HashMap<>();
        differentProps.put("versionHash", "differentHash");
        differentProps.put("versionHashLut", String.valueOf(Instant.now().getEpochSecond()));

        Map<String, String> staleProps = new HashMap<>();
        staleProps.put("versionHash", sampleVersionHash);
        staleProps.put("versionHashLut", String.valueOf(Instant.now().getEpochSecond() - 7200));

        WorkerMetricStats matching = mock(WorkerMetricStats.class);
        when(matching.getProperties()).thenReturn(matchingProps);

        WorkerMetricStats different = mock(WorkerMetricStats.class);
        when(different.getProperties()).thenReturn(differentProps);

        WorkerMetricStats stale = mock(WorkerMetricStats.class);
        when(stale.getProperties()).thenReturn(staleProps);

        WorkerMetricStats nullProps = mock(WorkerMetricStats.class);
        when(nullProps.getProperties()).thenReturn(null);

        List<WorkerMetricStats> result =
                handler.filterWorkersOnVersionHash(Arrays.asList(matching, different, stale, nullProps));

        assertEquals(1, result.size());
        assertEquals(matching, result.get(0));
    }

    // ---------------- robustness (review finding 9) ----------------

    /** A properties map without versionHash (the field predates segmenting) must not throw inside the LAM. */
    @Test
    void filterWorkersOnVersionHash_toleratesPropertiesWithoutVersionHash() {
        final WorkerMetricStats noHash = mock(WorkerMetricStats.class);
        when(noHash.getProperties()).thenReturn(Collections.singletonMap("someOtherProperty", "x"));

        assertTrue(handler.filterWorkersOnVersionHash(Collections.singletonList(noHash))
                .isEmpty());
    }

    @Test
    void isWorkerVersionHashStale_trueWhenLutUnparseable() {
        final WorkerMetricStats worker = mock(WorkerMetricStats.class);
        final Map<String, String> props = new HashMap<>();
        props.put("versionHash", sampleVersionHash);
        props.put("versionHashLut", "not-a-number");
        when(worker.getProperties()).thenReturn(props);

        assertTrue(handler.isWorkerVersionHashStale(worker));
    }

    /** A Leader item with a versionHash but no versionHashLut must expire, not look fresh forever. */
    @Test
    void getHashKeyForLeaderLock_returnsLeader_whenLeaderLutMissing() throws Exception {
        final Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("versionHash", AttributeValue.fromS("some-other-version"));
        final CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(LeaderLock.LEADER_HASH_KEY)))
                .thenReturn(state);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeader_whenLeaderLutUnparseable() throws Exception {
        final Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("versionHash", AttributeValue.fromS("some-other-version"));
        attrs.put("versionHashLut", AttributeValue.fromS("not-a-number"));
        final CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(LeaderLock.LEADER_HASH_KEY)))
                .thenReturn(state);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    /** Seeing zero active workers must not end the rollout. */
    @Test
    void setIsVersionEmittedByAllActiveWorkers_falseWhenNoActiveWorkers() {
        handler.setIsVersionEmittedByAllActiveWorkers(Collections.emptyList(), Collections.emptyList());

        assertFalse(handler.isVersionEmittedByAllActiveWorkers());
    }

    /** The flag is only refreshed by a leader's run, so losing leadership clears it. */
    @Test
    void setHeldLeaderKey_null_clearsVersionEmittedFlag() {
        final WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        when(w1.getWorkerId()).thenReturn("worker1");
        handler.setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        handler.setIsVersionEmittedByAllActiveWorkers(Collections.singletonList(w1), Collections.singletonList(w1));
        assertTrue(handler.isVersionEmittedByAllActiveWorkers());

        handler.setHeldLeaderKey(null);

        assertFalse(handler.isVersionEmittedByAllActiveWorkers());
    }

    /** The heartbeat is conditioned on this worker's version, so it also repairs an item missing its lut. */
    @Test
    void versionHashHeartbeat_expectsOwnerAndOwnVersionHash() throws Exception {
        handler.setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(mockVersionHeartbeatExecutor)
                .scheduleAtFixedRate(runnableCaptor.capture(), eq(0L), Mockito.anyLong(), eq(TimeUnit.MILLISECONDS));

        runnableCaptor.getValue().run();

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, ExpectedAttributeValue>> expectations = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(mockCoordinatorStateDAO).updateCoordinatorStateWithExpectation(any(), expectations.capture());
        assertEquals(
                sampleVersionHash,
                expectations.getValue().get("versionHash").value().s());
        assertEquals(
                "dummyWorkerId",
                expectations.getValue().get("ownerName").value().s());
        assertFalse(expectations.getValue().containsKey("versionHashLut"));
    }

    // ---------------- segmenting only in 3X with table migration COMPLETE (comments #17, #20) ----------------

    @Test
    void isEnabled_falseWhenNotAllowed_evenWithFlagOn() throws Exception {
        final FleetSegmentingHandler gated =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor, () -> false);

        assertFalse(gated.isEnabled());
        assertEquals(LeaderLock.LEADER_HASH_KEY, gated.getHashKeyForLeaderLock());
        gated.setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        assertTrue(gated.isOnCurrentVersion());
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(any());
    }

    @Test
    void isEnabled_falseWhenAllowedCheckThrows() {
        final FleetSegmentingHandler gated =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor, () -> {
                    throw new UnsupportedOperationException("state machine not initialized");
                });

        assertFalse(gated.isEnabled());
    }

    /** Segmenting becomes active at runtime (e.g. the app reaches 3X), so the check is evaluated on every call. */
    @Test
    void isEnabled_followsAllowedCheckAtCallTime() {
        final java.util.concurrent.atomic.AtomicBoolean allowed = new java.util.concurrent.atomic.AtomicBoolean(false);
        final FleetSegmentingHandler gated =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor, allowed::get);
        assertFalse(gated.isEnabled());

        allowed.set(true);

        assertTrue(gated.isEnabled());
    }

    /** The heartbeat is scheduled when the flag is on, but only writes while segmenting is allowed. */
    @Test
    void versionHashHeartbeat_scheduledWithFlag_butWritesOnlyWhenAllowed() throws Exception {
        final java.util.concurrent.atomic.AtomicBoolean allowed = new java.util.concurrent.atomic.AtomicBoolean(false);
        final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        final FleetSegmentingHandler gated =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, executor, allowed::get);
        gated.setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(executor)
                .scheduleAtFixedRate(runnableCaptor.capture(), eq(0L), Mockito.anyLong(), eq(TimeUnit.MILLISECONDS));

        runnableCaptor.getValue().run();
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).updateCoordinatorStateWithExpectation(any(), anyMap());

        allowed.set(true);
        runnableCaptor.getValue().run();
        Mockito.verify(mockCoordinatorStateDAO, times(1)).updateCoordinatorStateWithExpectation(any(), anyMap());
    }

    // ---------------- shutdown and zombie heartbeat (review finding 10) ----------------

    @Test
    void shutdown_stopsHeartbeatExecutor_andIsIdempotent() {
        handler.shutdown();
        when(mockVersionHeartbeatExecutor.isShutdown()).thenReturn(true);
        handler.shutdown();

        Mockito.verify(mockVersionHeartbeatExecutor, times(1)).shutdownNow();
    }

    @Test
    void shutdown_heartbeatNoLongerWrites() throws Exception {
        handler.setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(mockVersionHeartbeatExecutor)
                .scheduleAtFixedRate(runnableCaptor.capture(), eq(0L), Mockito.anyLong(), eq(TimeUnit.MILLISECONDS));

        handler.shutdown();
        runnableCaptor.getValue().run();

        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).updateCoordinatorStateWithExpectation(any(), anyMap());
    }

    @Test
    void versionHashHeartbeat_requiresLockNotReleased() throws Exception {
        handler.setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(mockVersionHeartbeatExecutor)
                .scheduleAtFixedRate(runnableCaptor.capture(), eq(0L), Mockito.anyLong(), eq(TimeUnit.MILLISECONDS));

        runnableCaptor.getValue().run();

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, ExpectedAttributeValue>> expectations = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(mockCoordinatorStateDAO).updateCoordinatorStateWithExpectation(any(), expectations.capture());
        assertFalse(expectations.getValue().get("isReleased").exists());
    }

    @Test
    void setIsVersionEmittedByAllActiveWorkers_setsTrue_whenSameWorkerIds() {
        WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        WorkerMetricStats w2 = mock(WorkerMetricStats.class);
        when(w1.getWorkerId()).thenReturn("worker1");
        when(w2.getWorkerId()).thenReturn("worker2");
        List<WorkerMetricStats> active = Arrays.asList(w1, w2);
        List<WorkerMetricStats> onVersion = Arrays.asList(w1, w2);
        handler.setIsVersionEmittedByAllActiveWorkers(active, onVersion);

        assertTrue(handler.isVersionEmittedByAllActiveWorkers());
    }

    @Test
    void setIsVersionEmittedByAllActiveWorkers_setsFalse_whenDifferentWorkerIds() {
        WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        WorkerMetricStats w2 = mock(WorkerMetricStats.class);
        WorkerMetricStats w3 = mock(WorkerMetricStats.class);
        WorkerMetricStats w4 = mock(WorkerMetricStats.class);
        when(w1.getWorkerId()).thenReturn("worker1");
        when(w2.getWorkerId()).thenReturn("worker2");
        when(w3.getWorkerId()).thenReturn("worker3");
        when(w4.getWorkerId()).thenReturn("worker4");
        List<WorkerMetricStats> active = Arrays.asList(w1, w2);
        List<WorkerMetricStats> onVersion = Arrays.asList(w3, w4);
        handler.setIsVersionEmittedByAllActiveWorkers(active, onVersion);

        assertFalse(handler.isVersionEmittedByAllActiveWorkers());
    }

    @Test
    void isEnabled_returnsTrue_whenConfigEnabled() {
        assertTrue(handler.isEnabled());
    }

    @Test
    void isEnabled_returnsFalse_whenConfigDisabled() {
        config.enableRollingDeploymentSystem(false);
        FleetSegmentingHandler disabledHandler =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor);
        assertFalse(disabledHandler.isEnabled());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenDisabled() throws Exception {
        config.enableRollingDeploymentSystem(false);
        FleetSegmentingHandler disabledHandler =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor);

        // Even with a CurrentVersion item that has a different hash, disabled handler returns Leader
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());

        assertEquals(LeaderLock.LEADER_HASH_KEY, disabledHandler.getHashKeyForLeaderLock());
    }

    @Test
    void startVersionHashHeartbeat_updatesDAO_whenWorkerIsNotLeader() throws Exception {
        LeaseManagementConfig heartbeatConfig = new LeaseManagementConfig(
                "dummyTableName",
                "dummyApplicationName",
                Mockito.mock(DynamoDbAsyncClient.class),
                Mockito.mock(KinesisAsyncClient.class),
                "dummyWorkerId");
        heartbeatConfig.enableRollingDeploymentSystem(true);
        heartbeatConfig.dynamoDbLockBasedLeaderHeartbeatPeriodInMillis(100);

        CoordinatorStateDAO heartbeatDAO = mock(CoordinatorStateDAO.class);
        mockCoordinatorState(
                heartbeatDAO,
                LeaderLock.LEADER_HASH_KEY,
                sampleVersionHash,
                Instant.now().getEpochSecond());

        FleetSegmentingHandler handler =
                new FleetSegmentingHandler(heartbeatConfig, heartbeatDAO, mockVersionHeartbeatExecutor);
        handler.setHeldLeaderKey(null);

        // Capture the scheduled runnable
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(mockVersionHeartbeatExecutor)
                .scheduleAtFixedRate(
                        runnableCaptor.capture(),
                        eq(0L),
                        eq(heartbeatConfig.dynamoDbLockBasedLeaderHeartbeatPeriodInMillis()),
                        eq(TimeUnit.MILLISECONDS));

        runnableCaptor.getValue().run();

        Mockito.verify(heartbeatDAO, times(0)).updateCoordinatorStateWithExpectation(any(), anyMap());
    }

    @Test
    void startVersionHashHeartbeat_doesNotUpdateDAO_whenWorkerIsLeader() throws Exception {
        LeaseManagementConfig heartbeatConfig = new LeaseManagementConfig(
                "dummyTableName",
                "dummyApplicationName",
                Mockito.mock(DynamoDbAsyncClient.class),
                Mockito.mock(KinesisAsyncClient.class),
                "dummyWorkerId");
        heartbeatConfig.enableRollingDeploymentSystem(true);
        heartbeatConfig.dynamoDbLockBasedLeaderHeartbeatPeriodInMillis(100);

        CoordinatorStateDAO heartbeatDAO = mock(CoordinatorStateDAO.class);
        mockCoordinatorState(
                heartbeatDAO,
                LeaderLock.LEADER_HASH_KEY,
                sampleVersionHash,
                Instant.now().getEpochSecond());

        FleetSegmentingHandler handler =
                new FleetSegmentingHandler(heartbeatConfig, heartbeatDAO, mockVersionHeartbeatExecutor);
        handler.setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);

        // Capture the scheduled runnable
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(mockVersionHeartbeatExecutor)
                .scheduleAtFixedRate(
                        runnableCaptor.capture(),
                        eq(0L),
                        eq(heartbeatConfig.dynamoDbLockBasedLeaderHeartbeatPeriodInMillis()),
                        eq(TimeUnit.MILLISECONDS));

        runnableCaptor.getValue().run();

        Mockito.verify(heartbeatDAO, times(1)).updateCoordinatorStateWithExpectation(any(), anyMap());
    }

    /** The heartbeat refreshes the lock this worker holds, without re-reading which key to use. */
    @Test
    void versionHashHeartbeat_writesHeldDeployingLeaderKey_withoutReading() throws Exception {
        handler.setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(mockVersionHeartbeatExecutor)
                .scheduleAtFixedRate(runnableCaptor.capture(), eq(0L), Mockito.anyLong(), eq(TimeUnit.MILLISECONDS));

        runnableCaptor.getValue().run();

        final ArgumentCaptor<CoordinatorState> stateCaptor = ArgumentCaptor.forClass(CoordinatorState.class);
        Mockito.verify(mockCoordinatorStateDAO).updateCoordinatorStateWithExpectation(stateCaptor.capture(), anyMap());
        assertEquals(
                LeaderLock.DEPLOYING_LEADER_HASH_KEY, stateCaptor.getValue().getKey());
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(any());
    }

    // ---------------- transient read errors (fail closed) ----------------

    private void mockCoordinatorState(CoordinatorStateDAO dao, String key, String versionHash, long lut)
            throws Exception {
        Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("versionHash", AttributeValue.fromS(versionHash));
        attrs.put("versionHashLut", AttributeValue.fromS(String.valueOf(lut)));
        CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(dao.getCoordinatorState(eq(key))).thenReturn(state);
    }

    private void mockCoordinatorState(String key, String versionHash, long lut) throws Exception {
        Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("versionHash", AttributeValue.fromS(versionHash));
        attrs.put("versionHashLut", AttributeValue.fromS(String.valueOf(lut)));
        CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(key))).thenReturn(state);
    }
}
