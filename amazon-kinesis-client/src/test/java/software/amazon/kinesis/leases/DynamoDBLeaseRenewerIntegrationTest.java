/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License. 
 */
package software.amazon.kinesis.leases;

import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;

public class DynamoDBLeaseRenewerIntegrationTest extends LeaseIntegrationTest {

    // This test case's leases last 2 seconds
    private static final long LEASE_DURATION_MILLIS = 2000L;

    private LeaseRenewer<KinesisClientLease> renewer;

    @Before
    public void setUp() {
        renewer = new DynamoDBLeaseRenewer<KinesisClientLease>(
                leaseManager, "foo", LEASE_DURATION_MILLIS, Executors.newCachedThreadPool());
    }

    @Test
    public void testSimpleRenew() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");
    }

    @Test
    public void testLeaseLoss() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "foo").withLease("2", "foo").build();

        builder.addLeasesToRenew(renewer, "1", "2");
        KinesisClientLease renewedLease = builder.renewMutateAssert(renewer, "1", "2").get("2");

        // lose lease 2
        leaseManager.takeLease(renewedLease, "bar");

        builder.renewMutateAssert(renewer, "1");
    }

    @Test
    public void testClear() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "foo").build();
        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        renewer.clearCurrentlyHeldLeases();
        builder.renewMutateAssert(renewer);
    }

    @Test
    public void testGetCurrentlyHeldLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "foo").build();
        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        // this should be a copy that doesn't get updated
        KinesisClientLease lease = renewer.getCurrentlyHeldLease("1");
        Assert.assertEquals((Long) 1L, lease.getLeaseCounter());

        // do one renewal and make sure the old copy doesn't get updated
        builder.renewMutateAssert(renewer, "1");

        Assert.assertEquals((Long) 1L, lease.getLeaseCounter());
    }

    @Test
    public void testGetCurrentlyHeldLeases() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "foo").withLease("2", "foo").build();
        builder.addLeasesToRenew(renewer, "1", "2");
        KinesisClientLease lease2 =  builder.renewMutateAssert(renewer, "1", "2").get("2");

        // This should be a copy that doesn't get updated
        Map<String, KinesisClientLease> heldLeases = renewer.getCurrentlyHeldLeases();
        Assert.assertEquals(2, heldLeases.size());
        Assert.assertEquals((Long) 1L, heldLeases.get("1").getLeaseCounter());
        Assert.assertEquals((Long) 1L, heldLeases.get("2").getLeaseCounter());

        // lose lease 2
        leaseManager.takeLease(lease2, "bar");

        // Do another renewal and make sure the copy doesn't change
        builder.renewMutateAssert(renewer, "1");

        Assert.assertEquals(2, heldLeases.size());
        Assert.assertEquals((Long) 1L, heldLeases.get("1").getLeaseCounter());
        Assert.assertEquals((Long) 1L, heldLeases.get("2").getLeaseCounter());
    }

    @Test
    public void testUpdateLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        KinesisClientLease expected = renewer.getCurrentlyHeldLease("1");
        expected.setCheckpoint(new ExtendedSequenceNumber("new checkpoint"));
        Assert.assertTrue(renewer.updateLease(expected, expected.getConcurrencyToken()));

        // Assert that the counter and data have changed immediately after the update...
        KinesisClientLease actual = renewer.getCurrentlyHeldLease("1");
        expected.setLeaseCounter(expected.getLeaseCounter() + 1);
        Assert.assertEquals(expected, actual);

        // ...and after another round of renewal
        renewer.renewLeases();
        actual = renewer.getCurrentlyHeldLease("1");
        expected.setLeaseCounter(expected.getLeaseCounter() + 1);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testUpdateLostLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        KinesisClientLease lease = renewer.getCurrentlyHeldLease("1");

        // cause lease loss such that the renewer doesn't realize he's lost the lease when update is called
        leaseManager.renewLease(lease);

        // renewer still thinks he has the lease
        Assert.assertNotNull(renewer.getCurrentlyHeldLease("1"));
        lease.setCheckpoint(new ExtendedSequenceNumber("new checkpoint"));

        // update fails
        Assert.assertFalse(renewer.updateLease(lease, lease.getConcurrencyToken()));
        // renewer no longer thinks he has the lease
        Assert.assertNull(renewer.getCurrentlyHeldLease("1"));
    }

    @Test
    public void testUpdateOldLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        KinesisClientLease lease = renewer.getCurrentlyHeldLease("1");

        // cause lease loss such that the renewer knows the lease has been lost when update is called
        leaseManager.takeLease(lease, "bar");
        builder.renewMutateAssert(renewer);

        lease.setCheckpoint(new ExtendedSequenceNumber("new checkpoint"));
        Assert.assertFalse(renewer.updateLease(lease, lease.getConcurrencyToken()));
    }

    @Test
    public void testUpdateRegainedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        KinesisClientLease lease = renewer.getCurrentlyHeldLease("1");

        // cause lease loss such that the renewer knows the lease has been lost when update is called
        leaseManager.takeLease(lease, "bar");
        builder.renewMutateAssert(renewer);

        // regain the lease
        builder.addLeasesToRenew(renewer, "1");

        lease.setCheckpoint(new ExtendedSequenceNumber("new checkpoint"));
        Assert.assertFalse(renewer.updateLease(lease, lease.getConcurrencyToken()));
    }

    @Test
    public void testIgnoreNoRenewalTimestamp() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        KinesisClientLease lease = builder.withLease("1", "foo").build().get("1");
        lease.setLastCounterIncrementNanos(null);

        renewer.addLeasesToRenew(Collections.singleton(lease));

        Assert.assertEquals(0, renewer.getCurrentlyHeldLeases().size());
    }

    @Test
    public void testLeaseTimeout() throws LeasingException, InterruptedException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        // TODO: Worth eliminating this sleep using the same pattern we used on LeaseTaker?
        Thread.sleep(LEASE_DURATION_MILLIS); // Wait for the lease to timeout

        Assert.assertEquals(0, renewer.getCurrentlyHeldLeases().size());
    }

    @Test
    public void testInitialize() throws LeasingException {
        final String shardId = "shd-0-0";
        final String owner = "foo:8000";

        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        builder.withLease(shardId, owner);
        Map<String, KinesisClientLease> leases = builder.build();
        DynamoDBLeaseRenewer<KinesisClientLease> renewer =new DynamoDBLeaseRenewer<KinesisClientLease>(
                leaseManager, owner, 30000L, Executors.newCachedThreadPool());
        renewer.initialize();
        Map<String, KinesisClientLease> heldLeases = renewer.getCurrentlyHeldLeases();
        Assert.assertEquals(leases.size(), heldLeases.size());
        Assert.assertEquals(leases.keySet(), heldLeases.keySet());
    }
}
