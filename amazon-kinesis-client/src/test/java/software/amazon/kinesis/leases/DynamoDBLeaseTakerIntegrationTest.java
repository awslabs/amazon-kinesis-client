/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import software.amazon.kinesis.leases.exceptions.LeasingException;

public class DynamoDBLeaseTakerIntegrationTest extends LeaseIntegrationTest {

    private static final long LEASE_DURATION_MILLIS = 1000L;
    private DynamoDBLeaseTaker<KinesisClientLease> taker;

    @Before
    public void setUp() {
        taker = new DynamoDBLeaseTaker<KinesisClientLease>(leaseManager, "foo", LEASE_DURATION_MILLIS);
    }

    @Test
    public void testSimpleLeaseTake() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", null).build();

        builder.takeMutateAssert(taker, "1");
    }

    @Test
    public void testNotTakeUpdatedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "bar").build();

        builder.takeMutateAssert(taker); // do a first scan to learn the state of the world
        builder.renewAllLeases(); // renew leases
        builder.passTime(LEASE_DURATION_MILLIS + 1);

        builder.takeMutateAssert(taker); // do a second scan
    }

    @Test
    public void testTakeOwnLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", taker.getWorkerIdentifier()).build();

        builder.takeMutateAssert(taker); // do a first scan to learn the state of the world
        builder.passTime(LEASE_DURATION_MILLIS + 1);
        builder.takeMutateAssert(taker, "1"); // do a second scan, assert that we didn't take anything
    }

    @Test
    public void testNotTakeNewOwnedLease() throws LeasingException, InterruptedException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "bar").build();

        builder.takeMutateAssert(taker); // This should not take anything because the lease is new and owned.
        builder.passTime(LEASE_DURATION_MILLIS + 1);

        // This should take because the lease is old
        builder.takeMutateAssert(taker, "1");
    }

    /**
     * Verify that we take leases non-greedily by setting up an environment where there are 4 leases and 2 workers,
     * only one of which holds a lease. This leaves 3 free leases, but LeaseTaker should decide it needs 2 leases and
     * only take 2.
     */
    @Test
    public void testNonGreedyTake() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        for (int i = 0; i < 3; i++) {
            builder.withLease(Integer.toString(i), null);
        }

        builder.withLease("4", "bar").build();

        builder.takeMutateAssert(taker, 2);
    }

    /**
     * Verify that LeaseTaker does not steal when it's only short 1 lease and the other worker is at target. Set up a
     * scenario where there are 4 leases held by two servers, and a third server with one lease. The third server should
     * not steal.
     */
    @Test
    public void testNoStealWhenOffByOne() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "bar")
                .withLease("2", "bar")
                .withLease("3", "baz")
                .withLease("4", "baz")
                .withLease("5", "foo")
                .build();

        // Takes nothing since all leases are new and owned and we won't steal if we're short by 1.
        builder.takeMutateAssert(taker);
    }

    /**
     * Verify that one activity is stolen from the highest loaded server when a server needs more than one lease and no
     * expired leases are available. Setup: 4 leases, server foo holds 0, bar holds 1, baz holds 5.
     * 
     * Foo should steal from baz.
     */
    @Test
    public void testSteal() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", "bar");
        for (int i = 2; i <= 6; i++) {
            String shardId = Integer.toString(i);
            builder.withLease(shardId, "baz");
        }

        builder.build();

        // Assert that one lease was stolen from baz.
        Map<String, KinesisClientLease> takenLeases = builder.takeMutateAssert(taker, 1);

        // Assert that it was one of baz's leases (shardId != 1)
        String shardIdStolen = takenLeases.keySet().iterator().next();
        Assert.assertFalse(shardIdStolen.equals("1"));
    }

    /**
     * Verify that stealing does not happen if LeaseTaker takes at least one expired lease, even if it needs more than
     * one.
     */
    @Test
    public void testNoStealWhenExpiredLeases() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        builder.withLease("1", null);
        for (int i = 2; i <= 4; i++) {
            String shardId = Integer.toString(i);
            builder.withLease(shardId, "bar");
        }

        builder.build();

        // Assert that the unowned lease was taken and we did not steal anything from bar
        builder.takeMutateAssert(taker, "1");
    }
}
