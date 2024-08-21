package software.amazon.kinesis.leases;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@RunWith(MockitoJUnitRunner.class)
public class LeaseTest {

    private static final long MOCK_CURRENT_TIME = 10000000000L;
    private static final long LEASE_DURATION_MILLIS = 1000L;

    private static final long LEASE_DURATION_NANOS = TimeUnit.MILLISECONDS.toNanos(LEASE_DURATION_MILLIS);

    // Write a unit test for software.amazon.kinesis.leases.Lease to test leaseOwner as null and epired
    @Test
    public void testLeaseOwnerNullAndExpired() {
        long expiredTime = MOCK_CURRENT_TIME - LEASE_DURATION_NANOS - 1;
        Lease lease = createLease(null, "leaseKey", expiredTime);
        Assert.assertTrue(lease.isAvailable(LEASE_DURATION_NANOS, MOCK_CURRENT_TIME));
        Assert.assertNull(lease.leaseOwner());
    }

    @Test
    public void testLeaseOwnerNotNullAndExpired() {
        long expiredTime = MOCK_CURRENT_TIME - LEASE_DURATION_NANOS - 1;
        Lease lease = createLease("leaseOwner", "leaseKey", expiredTime);
        Assert.assertTrue(lease.isAvailable(LEASE_DURATION_NANOS, MOCK_CURRENT_TIME));
        Assert.assertEquals("leaseOwner", lease.leaseOwner());
    }

    @Test
    public void testLeaseOwnerNotNullAndNotExpired() {
        long notExpiredTime = MOCK_CURRENT_TIME - LEASE_DURATION_NANOS + 1;
        Lease lease = createLease("leaseOwner", "leaseKey", notExpiredTime);
        Assert.assertFalse(lease.isAvailable(LEASE_DURATION_NANOS, MOCK_CURRENT_TIME));
        Assert.assertEquals("leaseOwner", lease.leaseOwner());
    }

    @Test
    public void testLeaseOwnerNullAndNotExpired() {
        long notExpiredTime = MOCK_CURRENT_TIME - LEASE_DURATION_NANOS + 1;
        Lease lease = createLease(null, "leaseKey", notExpiredTime);
        Assert.assertTrue(lease.isAvailable(LEASE_DURATION_NANOS, MOCK_CURRENT_TIME));
        Assert.assertNull(lease.leaseOwner());
    }

    private Lease createLease(String leaseOwner, String leaseKey, long lastCounterIncrementNanos) {
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
}
