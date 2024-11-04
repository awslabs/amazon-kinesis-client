package software.amazon.kinesis.leases;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class LeaseTest {

    private static final long MOCK_CURRENT_TIME = 10000000000L;
    private static final long LEASE_DURATION_MILLIS = 1000L;

    private static final long LEASE_DURATION_NANOS = TimeUnit.MILLISECONDS.toNanos(LEASE_DURATION_MILLIS);

    private static final long LEASE_CHECKPOINT_TIMEOUT = 1000;
    private final Lease shutdownRequestedLease = createShutdownRequestedLease();
    private final Lease eligibleForGracefulShutdownLease = createisEligibleForGracefulShutdownLease();

    // Write a unit test for software.amazon.kinesis.leases.Lease to test leaseOwner as null and epired
    @Test
    public void testLeaseOwnerNullAndExpired() {
        long expiredTime = MOCK_CURRENT_TIME - LEASE_DURATION_NANOS - 1;
        Lease lease = createLease(null, "leaseKey", expiredTime);
        assertTrue(lease.isAvailable(LEASE_DURATION_NANOS, MOCK_CURRENT_TIME));
        Assert.assertNull(lease.leaseOwner());
    }

    @Test
    public void testLeaseOwnerNotNullAndExpired() {
        long expiredTime = MOCK_CURRENT_TIME - LEASE_DURATION_NANOS - 1;
        Lease lease = createLease("leaseOwner", "leaseKey", expiredTime);
        assertTrue(lease.isAvailable(LEASE_DURATION_NANOS, MOCK_CURRENT_TIME));
        Assert.assertEquals("leaseOwner", lease.leaseOwner());
    }

    @Test
    public void testLeaseOwnerNotNullAndNotExpired() {
        long notExpiredTime = MOCK_CURRENT_TIME - LEASE_DURATION_NANOS + 1;
        Lease lease = createLease("leaseOwner", "leaseKey", notExpiredTime);
        assertFalse(lease.isAvailable(LEASE_DURATION_NANOS, MOCK_CURRENT_TIME));
        Assert.assertEquals("leaseOwner", lease.leaseOwner());
    }

    @Test
    public void testLeaseOwnerNullAndNotExpired() {
        long notExpiredTime = MOCK_CURRENT_TIME - LEASE_DURATION_NANOS + 1;
        Lease lease = createLease(null, "leaseKey", notExpiredTime);
        assertTrue(lease.isAvailable(LEASE_DURATION_NANOS, MOCK_CURRENT_TIME));
        Assert.assertNull(lease.leaseOwner());
    }

    @Test
    public void testBlockedOnPendingCheckpoint_LeaseAssignedAndCheckpointNotExpired_assertTrue() {
        assertTrue(shutdownRequestedLease.blockedOnPendingCheckpoint(LEASE_CHECKPOINT_TIMEOUT - 1));
    }

    @Test
    public void testBlockedOnPendingCheckpoint_LeaseUnassigned_assertFalse() {
        shutdownRequestedLease.isExpiredOrUnassigned(true);
        assertFalse(shutdownRequestedLease.blockedOnPendingCheckpoint(LEASE_CHECKPOINT_TIMEOUT));
    }

    @Test
    public void testBlockedOnPendingCheckpoint_ShardEnd_assertFalse() {
        shutdownRequestedLease.checkpoint(ExtendedSequenceNumber.SHARD_END);
        assertFalse(shutdownRequestedLease.blockedOnPendingCheckpoint(LEASE_CHECKPOINT_TIMEOUT));
    }

    @Test
    public void testBlockedOnPendingCheckpoint_ShutdownNotRequested_assertFalse() {
        shutdownRequestedLease.checkpointOwner(null);
        assertFalse(shutdownRequestedLease.blockedOnPendingCheckpoint(LEASE_CHECKPOINT_TIMEOUT));
    }

    @Test
    public void testBlockedOnPendingCheckpoint_CheckpointTimeoutExpired_assertFalse() {
        assertFalse(shutdownRequestedLease.blockedOnPendingCheckpoint(LEASE_CHECKPOINT_TIMEOUT + 1000));
    }

    @Test
    public void testIsEligibleForGracefulShutdown_leaseNotExpiredNotShuttingDownAndNotShardEnd_assertTrue() {
        assertTrue(eligibleForGracefulShutdownLease.isEligibleForGracefulShutdown());
    }

    @Test
    public void testIsEligibleForGracefulShutdownFalse_shardEnd_assertFalse() {
        eligibleForGracefulShutdownLease.checkpoint(ExtendedSequenceNumber.SHARD_END);
        assertFalse(shutdownRequestedLease.isEligibleForGracefulShutdown());
    }

    @Test
    public void testIsEligibleForGracefulShutdownFalse_leaseUnassigned_assertFalse() {
        eligibleForGracefulShutdownLease.isExpiredOrUnassigned(true);
        assertFalse(shutdownRequestedLease.isEligibleForGracefulShutdown());
    }

    @Test
    public void testIsEligibleForGracefulShutdownFalse_shutdownRequested_assertFalse() {
        eligibleForGracefulShutdownLease.checkpointOwner("owner");
        assertFalse(shutdownRequestedLease.isEligibleForGracefulShutdown());
    }

    private static Lease createLease(String leaseOwner, String leaseKey, long lastCounterIncrementNanos) {
        final Lease lease = new Lease();
        lease.checkpoint(new ExtendedSequenceNumber("checkpoint"));
        lease.ownerSwitchesSinceCheckpoint(0L);
        lease.leaseCounter(0L);
        lease.leaseOwner(leaseOwner);
        lease.parentShardIds(Collections.singleton("parentShardId"));
        lease.childShardIds(new HashSet<>());
        lease.leaseKey(leaseKey);
        lease.lastCounterIncrementNanos(lastCounterIncrementNanos);
        return lease;
    }

    private static Lease createShutdownRequestedLease() {
        final Lease lease = createLease("leaseOwner", "leaseKey", 0);
        lease.checkpointOwner("checkpointOwner");
        lease.checkpointOwnerTimeoutTimestampMillis(LEASE_CHECKPOINT_TIMEOUT);
        lease.isExpiredOrUnassigned(false);
        return lease;
    }

    private static Lease createisEligibleForGracefulShutdownLease() {
        final Lease lease = createLease("leaseOwner", "leaseKey", 0);
        lease.isExpiredOrUnassigned(false);
        lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        return lease;
    }
}
