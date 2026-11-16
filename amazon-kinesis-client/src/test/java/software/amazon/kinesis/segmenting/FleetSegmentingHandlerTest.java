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
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leader.LeaderLock;
import software.amazon.kinesis.leases.LeaseAssignmentStrategy;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
        when(mockCoordinatorStateDAO.getCoordinatorState(LeaderVersionState.LEADER_VERSION_HASH_KEY))
                .thenReturn(null);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenLeaderVersionExpired() throws Exception {
        // Two hours since the last heartbeat, past the leader version expiry.
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
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(LeaderVersionState.LEADER_VERSION_HASH_KEY)))
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
     * A DeployingLeader holder must not become "current" because the Leader item's version hash now
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

    /** With a non-lock leader decider (2.x-compatible modes) no lock is ever held; shard sync must run. */
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

    // ---------------- robustness against bad version data ----------------

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

    /** A version item with a versionHash but no heartbeat must expire, not look fresh forever. */
    @Test
    void getHashKeyForLeaderLock_returnsLeader_whenLeaderLutMissing() throws Exception {
        final Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("versionHash", AttributeValue.fromS("some-other-version"));
        final CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(LeaderVersionState.LEADER_VERSION_HASH_KEY)))
                .thenReturn(state);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeader_whenLeaderLutUnparseable() throws Exception {
        final Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("versionHash", AttributeValue.fromS("some-other-version"));
        attrs.put(LeaderVersionState.HEARTBEAT_ATTRIBUTE_NAME, AttributeValue.fromN("not-a-number"));
        final CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(LeaderVersionState.LEADER_VERSION_HASH_KEY)))
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

    // ---------------- segmenting only in 3X with table migration COMPLETE ----------------

    @Test
    void isEnabled_falseWhenNotAllowed_evenWithFlagOn() throws Exception {
        final FleetSegmentingHandler gated =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor, () -> false);

        assertFalse(gated.isEnabled());
        assertEquals(LeaderLock.LEADER_HASH_KEY, gated.getHashKeyForLeaderLock());
        gated.setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);
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

    // ---------------- shutdown and the version DAO ----------------

    @Test
    void shutdown_stopsHeartbeatExecutor_andIsIdempotent() {
        handler.shutdown();
        when(mockVersionHeartbeatExecutor.isShutdown()).thenReturn(true);
        handler.shutdown();

        Mockito.verify(mockVersionHeartbeatExecutor, times(1)).shutdownNow();
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

        // Even with a LeaderVersion item that has a different hash, disabled handler returns Leader
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());

        assertEquals(LeaderLock.LEADER_HASH_KEY, disabledHandler.getHashKeyForLeaderLock());
    }

    /** The leader version is read from its own item, never from the Leader lock item. */
    @Test
    void getHashKeyForLeaderLock_readsLeaderVersionItem_notLockItem() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());

        handler.getHashKeyForLeaderLock();

        Mockito.verify(mockCoordinatorStateDAO).getCoordinatorState(LeaderVersionState.LEADER_VERSION_HASH_KEY);
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(LeaderLock.LEADER_HASH_KEY);
    }

    @Test
    void getHashKeyForLeaderLock_failsClosed_onReadErrorAfterSuccess() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());
        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());

        when(mockCoordinatorStateDAO.getCoordinatorState(LeaderVersionState.LEADER_VERSION_HASH_KEY))
                .thenThrow(new DependencyException(new RuntimeException("throttled")));

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_throws_onReadErrorWithNoPreviousSuccess() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(LeaderVersionState.LEADER_VERSION_HASH_KEY))
                .thenThrow(new DependencyException(new RuntimeException("throttled")));

        assertThrows(VersionStateUnavailableException.class, () -> handler.getHashKeyForLeaderLock());
    }

    @Test
    void setHeldLeaderKey_passesHeldKeyToLeaderVersionDAO_whenEnabled() {
        final LeaderVersionDAO dao = mock(LeaderVersionDAO.class);
        final FleetSegmentingHandler withDao = new FleetSegmentingHandler(config, dao, () -> true);

        withDao.setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        Mockito.verify(dao).setHeldLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);

        withDao.setHeldLeaderKey(null);
        Mockito.verify(dao).setHeldLockKey(null);
    }

    /** While segmenting isn't allowed, the DAO is told no lock is held, so it writes no version item. */
    @Test
    void setHeldLeaderKey_passesNullToLeaderVersionDAO_whenNotAllowed() {
        final LeaderVersionDAO dao = mock(LeaderVersionDAO.class);
        final FleetSegmentingHandler withDao = new FleetSegmentingHandler(config, dao, () -> false);

        withDao.setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY);

        Mockito.verify(dao).setHeldLockKey(null);
        Mockito.verify(dao, Mockito.never()).setHeldLockKey(LeaderLock.LEADER_HASH_KEY);
    }

    @Test
    void shutdown_shutsDownLeaderVersionDAO() {
        final LeaderVersionDAO dao = mock(LeaderVersionDAO.class);
        final FleetSegmentingHandler withDao = new FleetSegmentingHandler(config, dao, () -> true);

        withDao.shutdown();

        Mockito.verify(dao).shutdown();
    }

    @Test
    void constructor_schedulesHeartbeatOnlyWhenFlagOn() {
        Mockito.verify(mockVersionHeartbeatExecutor)
                .scheduleAtFixedRate(any(Runnable.class), eq(0L), Mockito.anyLong(), eq(TimeUnit.MILLISECONDS));

        config.enableRollingDeploymentSystem(false);
        final ScheduledExecutorService disabledExecutor = mock(ScheduledExecutorService.class);
        new FleetSegmentingHandler(config, mockCoordinatorStateDAO, disabledExecutor);

        Mockito.verify(disabledExecutor, Mockito.never())
                .scheduleAtFixedRate(any(Runnable.class), Mockito.anyLong(), Mockito.anyLong(), any());
    }

    // ---------------- LeaderVersion outlives a stopped Leader holder ----------------

    /**
     * The Leader holder stopped heartbeating 90s ago (e.g. crashed), well past a worker's 60s staleness: a worker on
     * another version still stays on DeployingLeader.
     */
    @Test
    void getHashKeyForLeaderLock_leaderStoppedHeartbeating_otherVersion_staysOnDeployingLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - 90);

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_leaderStoppedHeartbeating_ownVersion_contendsForLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, sampleVersionHash, Instant.now().getEpochSecond() - 90);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    /** Once every active worker reports this worker's version (e.g. the last old-version worker died), take Leader. */
    @Test
    void getHashKeyForLeaderLock_leaderStoppedHeartbeating_allWorkersOnMyVersion_contendsForLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - 90);
        final FleetSegmentingHandler flipping = flippingHandler();
        final WorkerMetricStats w1 = workerOnMyVersion("worker1");
        flipping.setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        flipping.setIsVersionEmittedByAllActiveWorkers(Collections.singletonList(w1), Collections.singletonList(w1));
        flipping.setFleetViewReader(
                () -> new FleetSegmentingHandler.FleetView(Collections.singletonList(w1), Collections.emptyList()));
        TimeUnit.MILLISECONDS.sleep(LEASE_DURATION_MILLIS + 5);

        assertEquals(LeaderLock.LEADER_HASH_KEY, flipping.getHashKeyForLeaderLock());
    }

    /** Past the leader version expiry, the item no longer holds other versions back. */
    @Test
    void getHashKeyForLeaderLock_pastLeaderVersionExpiry_contendsForLeader() throws Exception {
        final long pastExpirySeconds =
                TimeUnit.MILLISECONDS.toSeconds(FleetSegmentingHandler.LEADER_VERSION_EXPIRY_MILLIS) + 60;
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - pastExpirySeconds);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_justUnderLeaderVersionExpiry_staysOnDeployingLeader() throws Exception {
        final long underExpirySeconds =
                TimeUnit.MILLISECONDS.toSeconds(FleetSegmentingHandler.LEADER_VERSION_EXPIRY_MILLIS) - 60;
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - underExpirySeconds);

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    /** A read error after holding back keeps the worker on DeployingLeader (fail closed). */
    @Test
    void getHashKeyForLeaderLock_leaderStoppedHeartbeating_thenReadError_keepsDeployingLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - 90);
        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());

        when(mockCoordinatorStateDAO.getCoordinatorState(LeaderVersionState.LEADER_VERSION_HASH_KEY))
                .thenThrow(new DependencyException(new RuntimeException("throttled")));

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    // ---------------- F2: one enablement value per leadership check ----------------

    /** A key recorded while disabled never reaches the version DAO, even if segmenting is enabled by now. */
    @Test
    void setHeldLeaderKey_withDisabledSnapshot_doesNotPassKeyToDao_evenWhenNowEnabled() {
        final LeaderVersionDAO dao = mock(LeaderVersionDAO.class);
        final FleetSegmentingHandler withDao = new FleetSegmentingHandler(config, dao, () -> true);

        withDao.setHeldLeaderKey(LeaderLock.LEADER_HASH_KEY, false);

        Mockito.verify(dao).setHeldLockKey(null);
        Mockito.verify(dao, Mockito.never()).setHeldLockKey(LeaderLock.LEADER_HASH_KEY);
    }

    @Test
    void getHashKeyForLeaderLock_withDisabledSnapshot_returnsLeaderWithoutReading() throws Exception {
        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock(false));
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(any());
    }

    // ---------------- F3: a held DeployingLeader is never current ----------------

    @Test
    void isOnCurrentVersion_falseWhileHoldingDeployingLeader_evenWhenDisabled() {
        final FleetSegmentingHandler gated =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor, () -> false);

        gated.setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        assertFalse(gated.isOnCurrentVersion());

        // Once it holds no lock, a disabled worker behaves as before segmenting again.
        gated.setHeldLeaderKey(null);
        assertTrue(gated.isOnCurrentVersion());
    }

    // ---------------- F5: the flip is re-validated with a fresh read ----------------

    @Test
    void flip_freshReadSeesWorkerOnOtherVersion_staysOnDeployingLeader() throws Exception {
        final FleetSegmentingHandler flipping = readyToFlip(
                Arrays.asList(workerOnMyVersion("worker1"), workerOnVersion("returned", "oldHash")),
                Collections.emptyList());

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, flipping.getHashKeyForLeaderLock());
    }

    /** A worker back on the old version renews its lease before its metrics row is fresh again. */
    @Test
    void flip_leaseOfOtherVersionOwnerWasRenewed_staysOnDeployingLeader() throws Exception {
        final FleetSegmentingHandler flipping = readyToFlip(
                Collections.singletonList(workerOnMyVersion("worker1")),
                Collections.singletonList(lease("shard-1", "returned", 8L)));

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, flipping.getHashKeyForLeaderLock());
    }

    /** A lease of a dead old-version owner doesn't hold the flip back: its counter hasn't moved. */
    @Test
    void flip_leaseOfDeadOtherVersionOwner_takesLeader() throws Exception {
        final FleetSegmentingHandler flipping = readyToFlip(
                Collections.singletonList(workerOnMyVersion("worker1")),
                Collections.singletonList(lease("shard-1", "returned", 7L)));

        assertEquals(LeaderLock.LEADER_HASH_KEY, flipping.getHashKeyForLeaderLock());
    }

    @Test
    void flip_freshReadFails_staysOnDeployingLeader() throws Exception {
        final FleetSegmentingHandler flipping =
                readyToFlip(Collections.singletonList(workerOnMyVersion("worker1")), Collections.emptyList());
        flipping.setFleetViewReader(() -> {
            throw new DependencyException(new RuntimeException("throttled"));
        });

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, flipping.getHashKeyForLeaderLock());
    }

    @Test
    void flip_beforeALeaseDurationHasPassed_staysOnDeployingLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());
        final FleetSegmentingHandler flipping = flippingHandler();
        final WorkerMetricStats w1 = workerOnMyVersion("worker1");
        flipping.setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        flipping.setFleetViewReader(
                () -> new FleetSegmentingHandler.FleetView(Collections.singletonList(w1), Collections.emptyList()));
        flipping.setIsVersionEmittedByAllActiveWorkers(Collections.singletonList(w1), Collections.singletonList(w1));

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, flipping.getHashKeyForLeaderLock());
    }

    @Test
    void flip_withoutFleetViewReader_staysOnDeployingLeader() throws Exception {
        final FleetSegmentingHandler flipping =
                readyToFlip(Collections.singletonList(workerOnMyVersion("worker1")), Collections.emptyList());
        flipping.setFleetViewReader(null);

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, flipping.getHashKeyForLeaderLock());
    }

    private static final long LEASE_DURATION_MILLIS = 20L;

    /** A handler with a short lease duration, so the flip's confirmation window passes quickly in tests. */
    private FleetSegmentingHandler flippingHandler() {
        final LeaseManagementConfig shortLease = new LeaseManagementConfig(
                "dummyTableName",
                "dummyApplicationName",
                Mockito.mock(DynamoDbAsyncClient.class),
                Mockito.mock(KinesisAsyncClient.class),
                "dummyWorkerId");
        shortLease.enableRollingDeploymentSystem(true);
        shortLease.leaseAssignmentStrategy(LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE);
        shortLease.failoverTimeMillis(LEASE_DURATION_MILLIS);
        return new FleetSegmentingHandler(shortLease, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor);
    }

    /**
     * A DeployingLeader whose last assignment run saw only worker1 (on its version) and lease shard-1 owned by
     * "returned" at counter 7, a lease duration ago. The fresh read returns {@code freshWorkers} and
     * {@code freshLeases}.
     */
    private FleetSegmentingHandler readyToFlip(
            final List<WorkerMetricStats> freshWorkers, final List<software.amazon.kinesis.leases.Lease> freshLeases)
            throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());
        final FleetSegmentingHandler flipping = flippingHandler();
        final WorkerMetricStats w1 = workerOnMyVersion("worker1");
        flipping.setHeldLeaderKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        flipping.setIsVersionEmittedByAllActiveWorkers(
                Collections.singletonList(w1),
                Collections.singletonList(w1),
                Collections.singletonList(lease("shard-1", "returned", 7L)));
        flipping.setFleetViewReader(() -> new FleetSegmentingHandler.FleetView(freshWorkers, freshLeases));
        TimeUnit.MILLISECONDS.sleep(LEASE_DURATION_MILLIS + 5);
        return flipping;
    }

    private WorkerMetricStats workerOnMyVersion(final String workerId) {
        return workerOnVersion(workerId, sampleVersionHash);
    }

    /** An active worker (fresh, valid metrics row) reporting {@code version}. */
    private static WorkerMetricStats workerOnVersion(final String workerId, final String version) {
        final WorkerMetricStats worker = mock(WorkerMetricStats.class);
        when(worker.getWorkerId()).thenReturn(workerId);
        when(worker.isValidWorkerMetric()).thenReturn(true);
        when(worker.isExpired(any())).thenReturn(false);
        final Map<String, String> properties = new HashMap<>();
        properties.put(FleetSegmentingHandler.VERSION_HASH_KEY, version);
        properties.put("versionHashLut", String.valueOf(Instant.now().getEpochSecond()));
        when(worker.getProperties()).thenReturn(properties);
        return worker;
    }

    private static software.amazon.kinesis.leases.Lease lease(
            final String key, final String owner, final long counter) {
        final software.amazon.kinesis.leases.Lease lease = new software.amazon.kinesis.leases.Lease();
        lease.leaseKey(key);
        lease.leaseOwner(owner);
        lease.leaseCounter(counter);
        return lease;
    }

    /** Stubs the version item for a leader lock. {@code lutSeconds} is converted to the item's heartbeat millis. */
    private void mockCoordinatorState(String lockKey, String versionHash, long lutSeconds) throws Exception {
        final LeaderVersionState state = new LeaderVersionState(
                LeaderVersionState.versionKeyForLock(lockKey), versionHash, "someLeader", lutSeconds * 1000L);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(state.getKey()))).thenReturn(state);
    }
}
