package software.amazon.kinesis.segmenting;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leader.LeaderLock;
import software.amazon.kinesis.leases.exceptions.DependencyException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LeaderVersionDAOTest {

    private static final String WORKER_ID = "worker-1";
    private static final String VERSION_HASH = "123";
    private static final long NOW_MILLIS = 1_000_000L;
    private static final long HEARTBEAT_PERIOD_MILLIS = 30_000L;

    private CoordinatorStateDAO coordinatorStateDAO;
    private AmazonDynamoDBLockClient lockClient;
    private ScheduledExecutorService executor;
    private AtomicBoolean enabled;
    private LeaderVersionDAO dao;

    @BeforeEach
    void setup() {
        coordinatorStateDAO = mock(CoordinatorStateDAO.class);
        lockClient = mock(AmazonDynamoDBLockClient.class);
        executor = mock(ScheduledExecutorService.class);
        enabled = new AtomicBoolean(true);
        when(coordinatorStateDAO.getDDBLockClient()).thenReturn(lockClient);
        when(lockClient.getLock(any(), any())).thenReturn(Optional.empty());
        dao = newDao(true);
    }

    private LeaderVersionDAO newDao(final boolean scheduleHeartbeat) {
        return new LeaderVersionDAO(
                coordinatorStateDAO,
                VERSION_HASH,
                WORKER_ID,
                executor,
                HEARTBEAT_PERIOD_MILLIS,
                scheduleHeartbeat,
                enabled::get,
                () -> NOW_MILLIS);
    }

    private void holdLock(final String lockKey) {
        final LockItem lock = mock(LockItem.class);
        when(lock.isExpired()).thenReturn(false);
        when(lock.getOwnerName()).thenReturn(WORKER_ID);
        when(lockClient.getLock(eq(lockKey), any())).thenReturn(Optional.of(lock));
    }

    /** Acquires {@code lockKey} and runs the full write that follows, so later ticks are plain heartbeats. */
    private void acquire(final String lockKey) throws Exception {
        holdLock(lockKey);
        when(coordinatorStateDAO.updateCoordinatorStateWithExpectation(any(), isNull()))
                .thenReturn(true);
        dao.setHeldLockKey(lockKey);
        dao.heartbeat();
    }

    /** Returns the expectations of the conditional heartbeat, skipping the unconditional full write. */
    @SuppressWarnings("unchecked")
    private Map<String, ExpectedAttributeValue> captureConditionalHeartbeatExpectations() throws Exception {
        final ArgumentCaptor<Map<String, ExpectedAttributeValue>> captor = ArgumentCaptor.forClass(Map.class);
        verify(coordinatorStateDAO, times(2)).updateCoordinatorStateWithExpectation(any(), captor.capture());
        assertNull(captor.getAllValues().get(0));
        return captor.getAllValues().get(1);
    }

    // ---------------- reads ----------------

    @Test
    void getVersionState_readsVersionItemForLock() throws Exception {
        final LeaderVersionState stored =
                new LeaderVersionState(LeaderVersionState.DEPLOYING_LEADER_VERSION_HASH_KEY, "9", "w", 5L);
        when(coordinatorStateDAO.getCoordinatorState(LeaderVersionState.DEPLOYING_LEADER_VERSION_HASH_KEY))
                .thenReturn(stored);

        assertSame(stored, dao.getVersionState(LeaderLock.DEPLOYING_LEADER_HASH_KEY));
    }

    @Test
    void getVersionState_returnsNullWhenMissing() throws Exception {
        assertNull(dao.getVersionState(LeaderLock.LEADER_HASH_KEY));
    }

    /** An item read as a generic state (e.g. without entityType) is still parsed as a version item. */
    @Test
    void getVersionState_parsesGenericState() throws Exception {
        final Map<String, AttributeValue> attrs = new HashMap<>();
        attrs.put(LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME, AttributeValue.fromS("9"));
        attrs.put(LeaderVersionState.HEARTBEAT_ATTRIBUTE_NAME, AttributeValue.fromN("5"));
        when(coordinatorStateDAO.getCoordinatorState(LeaderVersionState.LEADER_VERSION_HASH_KEY))
                .thenReturn(CoordinatorState.builder()
                        .key(LeaderVersionState.LEADER_VERSION_HASH_KEY)
                        .attributes(attrs)
                        .build());

        final LeaderVersionState read = dao.getVersionState(LeaderLock.LEADER_HASH_KEY);

        assertEquals("9", read.getVersionHash());
        assertEquals(5L, read.getHeartbeatMillis());
    }

    // ---------------- scheduling ----------------

    @Test
    void constructor_schedulesHeartbeatAtLeaderHeartbeatPeriod() {
        verify(executor)
                .scheduleAtFixedRate(
                        any(Runnable.class), eq(0L), eq(HEARTBEAT_PERIOD_MILLIS), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void constructor_doesNotScheduleWhenDisabledByConfig() {
        final ScheduledExecutorService other = mock(ScheduledExecutorService.class);
        executor = other;
        newDao(false);

        verify(other, never()).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any());
    }

    // ---------------- write on acquire ----------------

    @Test
    void setHeldLockKey_newKey_writesFullItemOnHeartbeatThread() throws Exception {
        holdLock(LeaderLock.LEADER_HASH_KEY);
        when(coordinatorStateDAO.updateCoordinatorStateWithExpectation(any(), isNull()))
                .thenReturn(true);

        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);
        verify(executor).execute(any(Runnable.class));
        dao.heartbeat();

        final ArgumentCaptor<CoordinatorState> state = ArgumentCaptor.forClass(CoordinatorState.class);
        verify(coordinatorStateDAO).updateCoordinatorStateWithExpectation(state.capture(), isNull());
        final LeaderVersionState written = (LeaderVersionState) state.getValue();
        assertEquals(LeaderVersionState.LEADER_VERSION_HASH_KEY, written.getKey());
        assertEquals(VERSION_HASH, written.getVersionHash());
        assertEquals(WORKER_ID, written.getOwnerName());
        assertEquals(NOW_MILLIS, written.getHeartbeatMillis());
    }

    @Test
    void setHeldLockKey_deployingLeader_writesDeployingLeaderVersionItem() throws Exception {
        acquire(LeaderLock.DEPLOYING_LEADER_HASH_KEY);

        final ArgumentCaptor<CoordinatorState> state = ArgumentCaptor.forClass(CoordinatorState.class);
        verify(coordinatorStateDAO).updateCoordinatorStateWithExpectation(state.capture(), isNull());
        assertEquals(
                LeaderVersionState.DEPLOYING_LEADER_VERSION_HASH_KEY,
                state.getValue().getKey());
    }

    @Test
    void fullWrite_createsItemWhenMissing() throws Exception {
        holdLock(LeaderLock.LEADER_HASH_KEY);
        when(coordinatorStateDAO.updateCoordinatorStateWithExpectation(any(), isNull()))
                .thenReturn(false);
        when(coordinatorStateDAO.createCoordinatorStateIfNotExists(any())).thenReturn(true);

        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);
        dao.heartbeat();

        verify(coordinatorStateDAO).createCoordinatorStateIfNotExists(any(LeaderVersionState.class));
    }

    @Test
    void setHeldLockKey_sameKey_doesNotRewrite() {
        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);
        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);

        verify(executor, times(1)).execute(any(Runnable.class));
    }

    @Test
    void setHeldLockKey_null_doesNotWrite() {
        dao.setHeldLockKey(null);

        verify(executor, never()).execute(any(Runnable.class));
    }

    @Test
    void setHeldLockKey_toleratesRejectedExecution() {
        doThrow(new RejectedExecutionException()).when(executor).execute(any(Runnable.class));

        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);
    }

    /** A failed write after acquiring is retried by the next heartbeat. */
    @Test
    void fullWrite_retriedOnNextTickAfterFailure() throws Exception {
        holdLock(LeaderLock.LEADER_HASH_KEY);
        when(coordinatorStateDAO.updateCoordinatorStateWithExpectation(any(), isNull()))
                .thenThrow(new DependencyException(new RuntimeException("throttled")))
                .thenReturn(true);

        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);
        dao.heartbeat();
        dao.heartbeat();

        verify(coordinatorStateDAO, times(2)).updateCoordinatorStateWithExpectation(any(), isNull());
    }

    // ---------------- heartbeat ----------------

    @Test
    void heartbeat_isConditionalOnOwnerAndVersion() throws Exception {
        acquire(LeaderLock.LEADER_HASH_KEY);
        when(coordinatorStateDAO.updateCoordinatorStateWithExpectation(any(), anyMap()))
                .thenReturn(true);

        dao.heartbeat();

        final Map<String, ExpectedAttributeValue> expectations = captureConditionalHeartbeatExpectations();
        assertEquals(
                WORKER_ID,
                expectations
                        .get(LeaderVersionState.OWNER_NAME_ATTRIBUTE_NAME)
                        .value()
                        .s());
        assertEquals(
                VERSION_HASH,
                expectations
                        .get(LeaderVersionState.VERSION_HASH_ATTRIBUTE_NAME)
                        .value()
                        .s());
    }

    @Test
    void heartbeat_doesNothingWhenNoLockHeld() throws Exception {
        dao.heartbeat();

        verify(coordinatorStateDAO, never()).updateCoordinatorStateWithExpectation(any(), any());
        verify(coordinatorStateDAO, never()).createCoordinatorStateIfNotExists(any());
    }

    @Test
    void heartbeat_doesNothingWhenNotEnabled() throws Exception {
        holdLock(LeaderLock.LEADER_HASH_KEY);
        enabled.set(false);
        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);

        dao.heartbeat();

        verify(coordinatorStateDAO, never()).updateCoordinatorStateWithExpectation(any(), any());
    }

    /** Only the lock holder writes: if the lock client doesn't hold the lock, nothing is written. */
    @Test
    void heartbeat_doesNothingWhenLockClientDoesNotHoldLock() throws Exception {
        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);

        dao.heartbeat();

        verify(coordinatorStateDAO, never()).updateCoordinatorStateWithExpectation(any(), any());
        verify(coordinatorStateDAO, never()).createCoordinatorStateIfNotExists(any());
    }

    @Test
    void heartbeat_doesNothingWhenLockExpired() throws Exception {
        final LockItem lock = mock(LockItem.class);
        when(lock.isExpired()).thenReturn(true);
        when(lock.getOwnerName()).thenReturn(WORKER_ID);
        when(lockClient.getLock(eq(LeaderLock.LEADER_HASH_KEY), any())).thenReturn(Optional.of(lock));
        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);

        dao.heartbeat();

        verify(coordinatorStateDAO, never()).updateCoordinatorStateWithExpectation(any(), any());
    }

    @Test
    void heartbeat_doesNothingWhenLockOwnedByAnotherWorker() throws Exception {
        final LockItem lock = mock(LockItem.class);
        when(lock.isExpired()).thenReturn(false);
        when(lock.getOwnerName()).thenReturn("worker-2");
        when(lockClient.getLock(eq(LeaderLock.LEADER_HASH_KEY), any())).thenReturn(Optional.of(lock));
        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);

        dao.heartbeat();

        verify(coordinatorStateDAO, never()).updateCoordinatorStateWithExpectation(any(), any());
    }

    @Test
    void heartbeat_doesNothingWhenLockClientMissing() throws Exception {
        when(coordinatorStateDAO.getDDBLockClient()).thenReturn(null);
        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);

        dao.heartbeat();

        verify(coordinatorStateDAO, never()).updateCoordinatorStateWithExpectation(any(), any());
    }

    @Test
    void heartbeat_rewritesItemWhenMissing() throws Exception {
        acquire(LeaderLock.LEADER_HASH_KEY);
        when(coordinatorStateDAO.updateCoordinatorStateWithExpectation(any(), anyMap()))
                .thenReturn(false);
        when(coordinatorStateDAO.getCoordinatorState(LeaderVersionState.LEADER_VERSION_HASH_KEY))
                .thenReturn(null);

        dao.heartbeat();

        // Once for the write after acquiring, once for the rewrite.
        verify(coordinatorStateDAO, times(2)).updateCoordinatorStateWithExpectation(any(), isNull());
    }

    /** A new leader owns the item, so this worker stops heartbeating it instead of fighting the owner. */
    @Test
    void heartbeat_stopsWhenItemOwnedByAnotherWorker() throws Exception {
        acquire(LeaderLock.LEADER_HASH_KEY);
        when(coordinatorStateDAO.updateCoordinatorStateWithExpectation(any(), anyMap()))
                .thenReturn(false);
        when(coordinatorStateDAO.getCoordinatorState(LeaderVersionState.LEADER_VERSION_HASH_KEY))
                .thenReturn(new LeaderVersionState(
                        LeaderVersionState.LEADER_VERSION_HASH_KEY, "456", "worker-2", NOW_MILLIS));

        dao.heartbeat();
        dao.heartbeat();

        // One conditional attempt, then nothing more; the full write after acquiring is the only unconditional one.
        verify(coordinatorStateDAO, times(1)).updateCoordinatorStateWithExpectation(any(), anyMap());
        verify(coordinatorStateDAO, times(1)).updateCoordinatorStateWithExpectation(any(), isNull());
    }

    /** Re-acquiring after being superseded writes the item again. */
    @Test
    void heartbeat_resumesAfterReacquiring() throws Exception {
        heartbeat_stopsWhenItemOwnedByAnotherWorker();

        dao.setHeldLockKey(null);
        dao.setHeldLockKey(LeaderLock.LEADER_HASH_KEY);
        dao.heartbeat();

        verify(coordinatorStateDAO, times(2)).updateCoordinatorStateWithExpectation(any(), isNull());
    }

    @Test
    void heartbeat_rewritesWhenOwnedByThisWorkerWithAnotherVersion() throws Exception {
        acquire(LeaderLock.LEADER_HASH_KEY);
        when(coordinatorStateDAO.updateCoordinatorStateWithExpectation(any(), anyMap()))
                .thenReturn(false);
        when(coordinatorStateDAO.getCoordinatorState(LeaderVersionState.LEADER_VERSION_HASH_KEY))
                .thenReturn(new LeaderVersionState(
                        LeaderVersionState.LEADER_VERSION_HASH_KEY, "456", WORKER_ID, NOW_MILLIS));

        dao.heartbeat();

        verify(coordinatorStateDAO, times(2)).updateCoordinatorStateWithExpectation(any(), isNull());
    }

    @Test
    void heartbeat_swallowsExceptions() throws Exception {
        acquire(LeaderLock.LEADER_HASH_KEY);
        when(coordinatorStateDAO.updateCoordinatorStateWithExpectation(any(), anyMap()))
                .thenThrow(new RuntimeException("boom"));

        dao.heartbeat();
    }

    /** Losing the lock stops writes; the item is left to expire rather than deleted. */
    @Test
    void heartbeat_stopsAfterLockReleased() throws Exception {
        acquire(LeaderLock.LEADER_HASH_KEY);

        dao.setHeldLockKey(null);
        dao.heartbeat();

        verify(coordinatorStateDAO, never()).updateCoordinatorStateWithExpectation(any(), anyMap());
        verify(coordinatorStateDAO, never()).deleteCoordinatorState(any());
    }

    // ---------------- shutdown ----------------

    @Test
    void shutdown_stopsExecutorAndWrites() throws Exception {
        acquire(LeaderLock.LEADER_HASH_KEY);

        dao.shutdown();
        when(executor.isShutdown()).thenReturn(true);
        dao.shutdown();
        dao.heartbeat();

        verify(executor, times(1)).shutdownNow();
        verify(coordinatorStateDAO, never()).updateCoordinatorStateWithExpectation(any(), anyMap());
    }
}
