package software.amazon.kinesis.leader;

import java.util.Optional;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.model.LockCurrentlyUnavailableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.migration.TableMigrationStateMachine;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DynamoDBLockBasedLeaderDeciderAcquisitionTest {
    private static final String WORKER_ID = "contender";
    private final CoordinatorStateDAO dao = mock(CoordinatorStateDAO.class);
    private final AmazonDynamoDBLockClient lockClient = mock(AmazonDynamoDBLockClient.class);
    private final TableMigrationStateMachine migrationStateMachine = mock(TableMigrationStateMachine.class);
    private DynamoDBLockBasedLeaderDecider decider;

    @BeforeEach
    void setup() {
        when(dao.getDDBLockClient()).thenReturn(lockClient);
        // Disable result caching so each call represents a scheduled leadership check.
        decider =
                new DynamoDBLockBasedLeaderDecider(dao, 0L, WORKER_ID, new NullMetricsFactory(), migrationStateMachine);
    }

    @Test
    void nonOwnerAttemptsAcquisitionEvenWhenRetrievedLockIsNotExpired() throws Exception {
        final LockItem remoteLock = lockItem("previous-leader", false);
        final LockItem acquiredLock = lockItem(WORKER_ID, false);
        when(lockClient.getLock(LeaderLock.LEADER_HASH_KEY, Optional.empty())).thenReturn(Optional.of(remoteLock));
        when(lockClient.tryAcquireLock(any(AcquireLockOptions.class)))
                .thenThrow(new LockCurrentlyUnavailableException("Observing the RVN"))
                .thenReturn(Optional.of(acquiredLock));

        // A retrieved nonowned item's isExpired() is not evidence of the remote owner's liveness.
        // The acquisition path must run on successive checks to track an unchanged RVN across a lease.
        assertFalse(decider.isLeader(WORKER_ID));
        assertTrue(decider.isLeader(WORKER_ID));

        final ArgumentCaptor<AcquireLockOptions> options = ArgumentCaptor.forClass(AcquireLockOptions.class);
        verify(lockClient, times(2)).tryAcquireLock(options.capture());
        assertTrue(options.getValue().shouldSkipBlockingWait());
        verify(migrationStateMachine).handleLeaderLockResult(false);
        verify(migrationStateMachine).handleLeaderLockResult(true);
    }

    @Test
    void nonOwnerDoesNotAssumeLeadershipWhenAcquisitionFails() throws Exception {
        final LockItem remoteLock = lockItem("healthy-leader", false);
        when(lockClient.getLock(LeaderLock.LEADER_HASH_KEY, Optional.empty())).thenReturn(Optional.of(remoteLock));
        when(lockClient.tryAcquireLock(any(AcquireLockOptions.class))).thenReturn(Optional.empty());

        assertFalse(decider.isLeader(WORKER_ID));
        verify(lockClient).tryAcquireLock(any(AcquireLockOptions.class));
        verify(migrationStateMachine).handleLeaderLockResult(false);
    }

    @Test
    void currentOwnerDoesNotReacquireValidLock() throws Exception {
        final LockItem ownedLock = lockItem(WORKER_ID, false);
        when(lockClient.getLock(LeaderLock.LEADER_HASH_KEY, Optional.empty())).thenReturn(Optional.of(ownedLock));

        assertTrue(decider.isLeader(WORKER_ID));
        verify(lockClient, never()).tryAcquireLock(any(AcquireLockOptions.class));
    }

    @Test
    void currentOwnerMustReacquireExpiredLocalLock() throws Exception {
        final LockItem ownedLock = lockItem(WORKER_ID, true);
        when(lockClient.getLock(LeaderLock.LEADER_HASH_KEY, Optional.empty())).thenReturn(Optional.of(ownedLock));
        when(lockClient.tryAcquireLock(any(AcquireLockOptions.class))).thenReturn(Optional.empty());

        assertFalse(decider.isLeader(WORKER_ID));
        verify(lockClient).tryAcquireLock(any(AcquireLockOptions.class));
    }

    private LockItem lockItem(final String owner, final boolean expired) {
        final LockItem item = mock(LockItem.class);
        when(item.getOwnerName()).thenReturn(owner);
        when(item.isExpired()).thenReturn(expired);
        return item;
    }
}
