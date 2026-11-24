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
import static org.junit.jupiter.api.Assertions.assertNull;
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
        config.enableFleetSegmentingForLeaseAssignment(true);
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
    void getTargetLeaderLockKey_returnsLeaderKey_whenVersionHashMatches() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, sampleVersionHash, Instant.now().getEpochSecond());

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    @Test
    void getTargetLeaderLockKey_returnsDeployingLeaderKey_whenVersionHashDoesNotMatch() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    @Test
    void getTargetLeaderLockKey_returnsLeaderKey_whenItemNotPresent() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(LeaderVersionState.CURRENT_LEADER_VERSION_STATE_KEY))
                .thenReturn(null);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    @Test
    void getTargetLeaderLockKey_returnsLeaderKey_whenLeaderVersionExpired() throws Exception {
        // Two hours since the last heartbeat, past the leader version expiry.
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - 7200);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    @Test
    void getTargetLeaderLockKey_returnsLeaderKey_whenVersionHashKeyMissing() throws Exception {
        Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("someOtherKey", AttributeValue.fromS("value"));
        CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(LeaderVersionState.CURRENT_LEADER_VERSION_STATE_KEY)))
                .thenReturn(state);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    // ---------------- isCurrentVersionLeader (decided by the held lock, no reads) ----------------

    @Test
    void isCurrentVersionLeader_trueWhenHoldingLeader() throws Exception {
        handler.setHeldLeaderLockKey(LeaderLock.LEADER_HASH_KEY);

        assertTrue(handler.isCurrentVersionLeader());
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(any());
    }

    @Test
    void isCurrentVersionLeader_falseWhenHoldingDeployingLeader() throws Exception {
        handler.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);

        assertFalse(handler.isCurrentVersionLeader());
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(any());
    }

    /**
     * A DeployingLeader holder must not become "current" because the Leader item's version hash now
     * matches its own. It only becomes current once it actually holds the Leader lock.
     */
    @Test
    void isCurrentVersionLeader_deployingLeaderStaysNotCurrent_untilItHoldsLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, sampleVersionHash, Instant.now().getEpochSecond());
        handler.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        assertFalse(handler.isCurrentVersionLeader());

        // Losing the lock doesn't make a former deploying leader current either.
        handler.setHeldLeaderLockKey(null);
        assertFalse(handler.isCurrentVersionLeader());

        handler.setHeldLeaderLockKey(LeaderLock.LEADER_HASH_KEY);
        assertTrue(handler.isCurrentVersionLeader());
    }

    /** With a non-lock leader decider (2.x-compatible modes) no lock is ever held; shard sync must run. */
    @Test
    void isCurrentVersionLeader_trueWhenNoLockEverHeld() throws Exception {
        assertTrue(handler.isCurrentVersionLeader());
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(any());
    }

    @Test
    void isCurrentVersionLeader_returnsTrue_whenDisabled() {
        config.enableFleetSegmentingForLeaseAssignment(false);
        FleetSegmentingHandler disabledHandler =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor);
        assertTrue(disabledHandler.isCurrentVersionLeader());
    }

    /** A disabled worker's metrics carry no properties, as before segmenting. */
    @Test
    void generateVersionHashWithLastUpdatedTimeMap_returnsNull_whenDisabled() {
        config.enableFleetSegmentingForLeaseAssignment(false);
        final FleetSegmentingHandler disabledHandler =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor);
        assertNull(disabledHandler.generateVersionHashWithLastUpdatedTimeMap());
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
    void getTargetLeaderLockKey_returnsLeader_whenLeaderLutMissing() throws Exception {
        final Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("versionHash", AttributeValue.fromS("some-other-version"));
        final CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(LeaderVersionState.CURRENT_LEADER_VERSION_STATE_KEY)))
                .thenReturn(state);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    @Test
    void getTargetLeaderLockKey_returnsLeader_whenLeaderLutUnparseable() throws Exception {
        final Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put("versionHash", AttributeValue.fromS("some-other-version"));
        attrs.put(LeaderVersionState.HEARTBEAT_ATTRIBUTE_NAME, AttributeValue.fromN("not-a-number"));
        final CoordinatorState state = mock(CoordinatorState.class);
        when(state.getAttributes()).thenReturn(attrs);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(LeaderVersionState.CURRENT_LEADER_VERSION_STATE_KEY)))
                .thenReturn(state);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    /** Seeing zero active workers must not end the rollout. */
    @Test
    void setAllActiveWorkersOnThisVersion_falseWhenNoActiveWorkers() {
        handler.setAllActiveWorkersOnThisVersion(Collections.emptyList(), Collections.emptyList());

        assertFalse(handler.isAllActiveWorkersOnThisVersion());
    }

    /** The flag is only refreshed by a leader's run, so losing leadership clears it. */
    @Test
    void setHeldLeaderLockKey_null_clearsVersionEmittedFlag() {
        final WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        when(w1.getWorkerId()).thenReturn("worker1");
        handler.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        handler.setAllActiveWorkersOnThisVersion(Collections.singletonList(w1), Collections.singletonList(w1));
        assertTrue(handler.isAllActiveWorkersOnThisVersion());

        handler.setHeldLeaderLockKey(null);

        assertFalse(handler.isAllActiveWorkersOnThisVersion());
    }

    // ---------------- segmenting only in 3X with table migration COMPLETE ----------------

    @Test
    void isEnabled_falseWhenNotAllowed_evenWithFlagOn() throws Exception {
        final FleetSegmentingHandler gated =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor, () -> false);

        assertFalse(gated.isEnabled());
        assertEquals(LeaderLock.LEADER_HASH_KEY, gated.getTargetLeaderLockKey());
        gated.setHeldLeaderLockKey(LeaderLock.LEADER_HASH_KEY);
        assertTrue(gated.isCurrentVersionLeader());
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
    void setAllActiveWorkersOnThisVersion_setsTrue_whenSameWorkerIds() {
        WorkerMetricStats w1 = mock(WorkerMetricStats.class);
        WorkerMetricStats w2 = mock(WorkerMetricStats.class);
        when(w1.getWorkerId()).thenReturn("worker1");
        when(w2.getWorkerId()).thenReturn("worker2");
        List<WorkerMetricStats> active = Arrays.asList(w1, w2);
        List<WorkerMetricStats> onVersion = Arrays.asList(w1, w2);
        handler.setAllActiveWorkersOnThisVersion(active, onVersion);

        assertTrue(handler.isAllActiveWorkersOnThisVersion());
    }

    @Test
    void setAllActiveWorkersOnThisVersion_setsFalse_whenDifferentWorkerIds() {
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
        handler.setAllActiveWorkersOnThisVersion(active, onVersion);

        assertFalse(handler.isAllActiveWorkersOnThisVersion());
    }

    @Test
    void isEnabled_returnsTrue_whenConfigEnabled() {
        assertTrue(handler.isEnabled());
    }

    @Test
    void isEnabled_returnsFalse_whenConfigDisabled() {
        config.enableFleetSegmentingForLeaseAssignment(false);
        FleetSegmentingHandler disabledHandler =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor);
        assertFalse(disabledHandler.isEnabled());
    }

    @Test
    void getTargetLeaderLockKey_returnsLeaderKey_whenDisabled() throws Exception {
        config.enableFleetSegmentingForLeaseAssignment(false);
        FleetSegmentingHandler disabledHandler =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor);

        // Even with a LeaderVersion item that has a different hash, disabled handler returns Leader
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());

        assertEquals(LeaderLock.LEADER_HASH_KEY, disabledHandler.getTargetLeaderLockKey());
    }

    /** The leader version is read from its own item, never from the Leader lock item. */
    @Test
    void getTargetLeaderLockKey_readsLeaderVersionItem_notLockItem() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());

        handler.getTargetLeaderLockKey();

        Mockito.verify(mockCoordinatorStateDAO)
                .getCoordinatorState(LeaderVersionState.CURRENT_LEADER_VERSION_STATE_KEY);
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(LeaderLock.LEADER_HASH_KEY);
    }

    @Test
    void getTargetLeaderLockKey_failsClosed_onReadErrorAfterSuccess() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());
        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());

        when(mockCoordinatorStateDAO.getCoordinatorState(LeaderVersionState.CURRENT_LEADER_VERSION_STATE_KEY))
                .thenThrow(new DependencyException(new RuntimeException("throttled")));

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    @Test
    void getTargetLeaderLockKey_throws_onReadErrorWithNoPreviousSuccess() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(LeaderVersionState.CURRENT_LEADER_VERSION_STATE_KEY))
                .thenThrow(new DependencyException(new RuntimeException("throttled")));

        assertThrows(VersionStateUnavailableException.class, () -> handler.getTargetLeaderLockKey());
    }

    @Test
    void setHeldLeaderLockKey_passesHeldKeyToLeaderVersionDAO_whenEnabled() {
        final LeaderVersionDAO dao = mock(LeaderVersionDAO.class);
        final FleetSegmentingHandler withDao = new FleetSegmentingHandler(config, dao, () -> true);

        withDao.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        Mockito.verify(dao).setHeldLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);

        withDao.setHeldLeaderLockKey(null);
        Mockito.verify(dao).setHeldLockKey(null);
    }

    /** While segmenting isn't allowed, the DAO is told no lock is held, so it writes no version item. */
    @Test
    void setHeldLeaderLockKey_passesNullToLeaderVersionDAO_whenNotAllowed() {
        final LeaderVersionDAO dao = mock(LeaderVersionDAO.class);
        final FleetSegmentingHandler withDao = new FleetSegmentingHandler(config, dao, () -> false);

        withDao.setHeldLeaderLockKey(LeaderLock.LEADER_HASH_KEY);

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

        config.enableFleetSegmentingForLeaseAssignment(false);
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
    void getTargetLeaderLockKey_leaderStoppedHeartbeating_otherVersion_staysOnDeployingLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - 90);

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    @Test
    void getTargetLeaderLockKey_leaderStoppedHeartbeating_ownVersion_contendsForLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, sampleVersionHash, Instant.now().getEpochSecond() - 90);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    /** Once every active worker reports this worker's version (e.g. the last old-version worker died), take Leader. */
    @Test
    void getTargetLeaderLockKey_leaderStoppedHeartbeating_allWorkersOnMyVersion_contendsForLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - 90);
        final List<WorkerMetricStats> fleet = Collections.singletonList(worker("worker1"));
        handler.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        handler.setAllActiveWorkersOnThisVersion(fleet, fleet, 1_000L);
        handler.setAllActiveWorkersOnThisVersion(fleet, fleet, 1_001L + WORKER_VERSION_EXPIRY_MILLIS);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    /** Past the leader version expiry, the item no longer holds other versions back. */
    @Test
    void getTargetLeaderLockKey_pastLeaderVersionExpiry_contendsForLeader() throws Exception {
        final long pastExpirySeconds =
                TimeUnit.MILLISECONDS.toSeconds(FleetSegmentingHandler.LEADER_VERSION_EXPIRY_MILLIS) + 60;
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - pastExpirySeconds);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    @Test
    void getTargetLeaderLockKey_justUnderLeaderVersionExpiry_staysOnDeployingLeader() throws Exception {
        final long underExpirySeconds =
                TimeUnit.MILLISECONDS.toSeconds(FleetSegmentingHandler.LEADER_VERSION_EXPIRY_MILLIS) - 60;
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - underExpirySeconds);

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    /** A read error after holding back keeps the worker on DeployingLeader (fail closed). */
    @Test
    void getTargetLeaderLockKey_leaderStoppedHeartbeating_thenReadError_keepsDeployingLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond() - 90);
        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());

        when(mockCoordinatorStateDAO.getCoordinatorState(LeaderVersionState.CURRENT_LEADER_VERSION_STATE_KEY))
                .thenThrow(new DependencyException(new RuntimeException("throttled")));

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    // ---------------- F2: one enablement value per leadership check ----------------

    /** A key recorded while disabled never reaches the version DAO, even if segmenting is enabled by now. */
    @Test
    void setHeldLeaderLockKey_withDisabledSnapshot_doesNotPassKeyToDao_evenWhenNowEnabled() {
        final LeaderVersionDAO dao = mock(LeaderVersionDAO.class);
        final FleetSegmentingHandler withDao = new FleetSegmentingHandler(config, dao, () -> true);

        withDao.setHeldLeaderLockKey(LeaderLock.LEADER_HASH_KEY, false);

        Mockito.verify(dao).setHeldLockKey(null);
        Mockito.verify(dao, Mockito.never()).setHeldLockKey(LeaderLock.LEADER_HASH_KEY);
    }

    @Test
    void getTargetLeaderLockKey_withDisabledSnapshot_returnsLeaderWithoutReading() throws Exception {
        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey(false));
        Mockito.verify(mockCoordinatorStateDAO, Mockito.never()).getCoordinatorState(any());
    }

    // ---------------- F3: a held DeployingLeader is never current ----------------

    @Test
    void isCurrentVersionLeader_falseWhileHoldingDeployingLeader_evenWhenDisabled() {
        final FleetSegmentingHandler gated =
                new FleetSegmentingHandler(config, mockCoordinatorStateDAO, mockVersionHeartbeatExecutor, () -> false);

        gated.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        assertFalse(gated.isCurrentVersionLeader());

        // Once it holds no lock, a disabled worker behaves as before segmenting again.
        gated.setHeldLeaderLockKey(null);
        assertTrue(gated.isCurrentVersionLeader());
    }

    // ---------------- F5: the flip waits for runs more than a worker-version expiry apart ----------------

    /** Worker-version expiry with the default config: max(2 x 30s heartbeat, 2 x 30s reporter). */
    private static final long WORKER_VERSION_EXPIRY_MILLIS = 60_000L;

    /** One run that sees every worker on this version isn't enough: a returning worker may not have reported yet. */
    @Test
    void flip_singleRun_staysOnDeployingLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());
        final List<WorkerMetricStats> fleet = Collections.singletonList(worker("worker1"));
        handler.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        handler.setAllActiveWorkersOnThisVersion(fleet, fleet, 1_000L);

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    @Test
    void flip_runsExactlyAWorkerVersionExpiryApart_staysOnDeployingLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());
        final List<WorkerMetricStats> fleet = Collections.singletonList(worker("worker1"));
        handler.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        handler.setAllActiveWorkersOnThisVersion(fleet, fleet, 1_000L);
        handler.setAllActiveWorkersOnThisVersion(fleet, fleet, 1_000L + WORKER_VERSION_EXPIRY_MILLIS);

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    @Test
    void flip_runsMoreThanAWorkerVersionExpiryApart_takesLeader() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());
        final List<WorkerMetricStats> fleet = Collections.singletonList(worker("worker1"));
        handler.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        handler.setAllActiveWorkersOnThisVersion(fleet, fleet, 1_000L);
        handler.setAllActiveWorkersOnThisVersion(fleet, fleet, 1_001L + WORKER_VERSION_EXPIRY_MILLIS);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    /** F5: an old-version worker that comes back in between breaks the streak, so the wait starts over. */
    @Test
    void flip_returningWorkerBetweenRuns_restartsTheWait() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());
        final List<WorkerMetricStats> onVersion = Collections.singletonList(worker("worker1"));
        final List<WorkerMetricStats> withReturned = Arrays.asList(worker("worker1"), worker("returned"));
        handler.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        handler.setAllActiveWorkersOnThisVersion(onVersion, onVersion, 1_000L);
        handler.setAllActiveWorkersOnThisVersion(withReturned, onVersion, 31_000L);
        handler.setAllActiveWorkersOnThisVersion(onVersion, onVersion, 62_000L);

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());

        handler.setAllActiveWorkersOnThisVersion(onVersion, onVersion, 62_001L + WORKER_VERSION_EXPIRY_MILLIS);

        assertEquals(LeaderLock.LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    /** Losing the lock ends the streak, so a later DeployingLeader starts its own wait. */
    @Test
    void flip_streakClearedWhenLockReleased() throws Exception {
        mockCoordinatorState(
                LeaderLock.LEADER_HASH_KEY, "differentHash", Instant.now().getEpochSecond());
        final List<WorkerMetricStats> fleet = Collections.singletonList(worker("worker1"));
        handler.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        handler.setAllActiveWorkersOnThisVersion(fleet, fleet, 1_000L);
        handler.setHeldLeaderLockKey(null);
        handler.setHeldLeaderLockKey(LeaderLock.DEPLOYING_LEADER_HASH_KEY);
        handler.setAllActiveWorkersOnThisVersion(fleet, fleet, 1_001L + WORKER_VERSION_EXPIRY_MILLIS);

        assertEquals(LeaderLock.DEPLOYING_LEADER_HASH_KEY, handler.getTargetLeaderLockKey());
    }

    private static WorkerMetricStats worker(final String workerId) {
        final WorkerMetricStats worker = mock(WorkerMetricStats.class);
        when(worker.getWorkerId()).thenReturn(workerId);
        return worker;
    }

    /** Stubs the version item for a leader lock. {@code lutSeconds} is converted to the item's heartbeat millis. */
    private void mockCoordinatorState(String lockKey, String versionHash, long lutSeconds) throws Exception {
        final LeaderVersionState state = new LeaderVersionState(
                LeaderVersionState.versionKeyForLock(lockKey), versionHash, "someLeader", lutSeconds * 1000L);
        when(mockCoordinatorStateDAO.getCoordinatorState(eq(state.getKey()))).thenReturn(state);
    }
}
