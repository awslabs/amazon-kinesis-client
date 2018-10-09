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
package software.amazon.kinesis.leases.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseIntegrationTest;
import software.amazon.kinesis.leases.exceptions.LeasingException;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBLeaseRefresherIntegrationTest extends LeaseIntegrationTest {

    @Before
    public void setup() {
        doNothing().when(tableCreatorCallback).performAction(
                eq(TableCreatorCallbackInput.builder().dynamoDbClient(ddbClient).tableName(tableName).build()));
    }

    /**
     * Test listLeases when no records are present.
     */
    @Test
    public void testListNoRecords() throws LeasingException {
        List<Lease> leases = leaseRefresher.listLeases();
        assertTrue(leases.isEmpty());
    }

    /**
     * Tests listLeases when records are present. Exercise dynamo's paging functionality.
     */
    @Test
    public void testListWithRecords() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        int numRecordsToPut = 10;

        for (int i = 0; i < numRecordsToPut; i++) {
            builder.withLease(Integer.toString(i));
        }

        Collection<Lease> expected = builder.build().values();

        // The / 3 here ensures that we will test Dynamo's paging mechanics.
        List<Lease> actual = leaseRefresher.list(numRecordsToPut / 3);

        for (Lease lease : actual) {
            assertNotNull(expected.remove(lease));
        }

        assertTrue(expected.isEmpty());
    }

    /**
     * Tests getLease when a record is present.
     */
    @Test
    public void testGetLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        Lease expected = builder.withLease("1").build().get("1");

        Lease actual = leaseRefresher.getLease(expected.leaseKey());
        assertEquals(expected, actual);
    }

    /**
     * Tests leaseRefresher.get() when the looked-for record is absent.
     */
    @Test
    public void testGetNull() throws LeasingException {
        Lease actual = leaseRefresher.getLease("bogusShardId");
        assertNull(actual);
    }

    /**
     * Tests leaseRefresher.holdLease's success scenario.
     */
    @Test
    public void testRenewLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        Lease lease = builder.withLease("1").build().get("1");
        Long originalLeaseCounter = lease.leaseCounter();

        leaseRefresher.renewLease(lease);
        assertTrue(originalLeaseCounter + 1 == lease.leaseCounter());

        Lease fromDynamo = leaseRefresher.getLease(lease.leaseKey());

        assertEquals(lease, fromDynamo);
    }

    /**
     * Tests leaseRefresher.holdLease when the lease has changed out from under us.
     */
    @Test
    public void testHoldUpdatedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        Lease lease = builder.withLease("1").build().get("1");

        Lease leaseCopy = leaseRefresher.getLease(lease.leaseKey());

        // lose lease
        leaseRefresher.takeLease(lease, "bar");

        assertFalse(leaseRefresher.renewLease(leaseCopy));
    }

    /**
     * Tests takeLease when the lease is not already owned.
     */
    @Test
    public void testTakeUnownedLease() throws LeasingException {
        testTakeLease(false);
    }

    /**
     * Tests takeLease when the lease is already owned.
     */
    @Test
    public void testTakeOwnedLease() throws LeasingException {
        testTakeLease(true);
    }

    private void testTakeLease(boolean owned) throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        Lease lease = builder.withLease("1", owned ? "originalOwner" : null).build().get("1");
        Long originalLeaseCounter = lease.leaseCounter();

        String newOwner = "newOwner";
        leaseRefresher.takeLease(lease, newOwner);
        assertTrue(originalLeaseCounter + 1 == lease.leaseCounter());
        assertTrue((owned ? 1 : 0) == lease.ownerSwitchesSinceCheckpoint());
        assertEquals(newOwner, lease.leaseOwner());

        Lease fromDynamo = leaseRefresher.getLease(lease.leaseKey());

        assertEquals(lease, fromDynamo);
    }

    /**
     * Tests takeLease when the lease has changed out from under us.
     */
    @Test
    public void testTakeUpdatedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        Lease lease = builder.withLease("1").build().get("1");

        Lease leaseCopy = leaseRefresher.getLease(lease.leaseKey());

        String newOwner = "newOwner";
        leaseRefresher.takeLease(lease, newOwner);

        assertFalse(leaseRefresher.takeLease(leaseCopy, newOwner));
    }

    /**
     * Tests evictLease when the lease is currently unowned.
     */
    public void testEvictUnownedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        Lease lease = builder.withLease("1", null).build().get("1");

        assertFalse(leaseRefresher.evictLease(lease));
    }

    /**
     * Tests evictLease when the lease is currently owned.
     */
    @Test
    public void testEvictOwnedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        Lease lease = builder.withLease("1").build().get("1");
        Long originalLeaseCounter = lease.leaseCounter();

        leaseRefresher.evictLease(lease);
        assertNull(lease.leaseOwner());
        assertTrue(originalLeaseCounter + 1 == lease.leaseCounter());

        Lease fromDynamo = leaseRefresher.getLease(lease.leaseKey());

        assertEquals(lease, fromDynamo);
    }

    /**
     * Tests evictLease when the lease has changed out from under us. Note that evicting leases
     * is conditional on the lease owner, unlike everything else which is conditional on the
     * lease counter.
     */
    @Test
    public void testEvictChangedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        Lease lease = builder.withLease("1").build().get("1");

        // Change the owner only - this should cause our optimistic lock to fail.
        lease.leaseOwner("otherOwner");
        assertFalse(leaseRefresher.evictLease(lease));
    }

    /**
     * Tests deleteLease when a lease exists.
     */
    @Test
    public void testDeleteLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        Lease lease = builder.withLease("1").build().get("1");

        leaseRefresher.deleteLease(lease);

        Lease newLease = leaseRefresher.getLease(lease.leaseKey());
        assertNull(newLease);
    }

    /**
     * Tests deleteLease when a lease does not exist.
     */
    @Test
    public void testDeleteNonexistentLease() throws LeasingException {
        Lease lease = new Lease();
        lease.leaseKey("1");
        // The lease has not been written to DDB - try to delete it and expect success.

        leaseRefresher.deleteLease(lease);
    }

    @Test
    public void testWaitUntilLeaseTableExists() throws LeasingException {
        DynamoDBLeaseRefresher refresher = new DynamoDBLeaseRefresher("nagl_ShardProgress", ddbClient,
                new DynamoDBLeaseSerializer(), true, tableCreatorCallback) {
            @Override
            long sleep(long timeToSleepMillis) {
                fail("Should not sleep");
                return 0L;
            }

        };

        assertTrue(refresher.waitUntilLeaseTableExists(1, 1));
    }

    @Test
    public void testWaitUntilLeaseTableExistsTimeout() throws LeasingException {
        /*
         * Just using AtomicInteger for the indirection it provides.
         */
        final AtomicInteger sleepCounter = new AtomicInteger(0);
        DynamoDBLeaseRefresher refresher = new DynamoDBLeaseRefresher("nonexistentTable", ddbClient,
                new DynamoDBLeaseSerializer(), true, tableCreatorCallback) {
            @Override
            long sleep(long timeToSleepMillis) {
                assertEquals(1000L, timeToSleepMillis);
                sleepCounter.incrementAndGet();
                return 1000L;
            }

        };

        assertFalse(refresher.waitUntilLeaseTableExists(2, 1));
        assertEquals(1, sleepCounter.get());
    }

    @Test
    public void testTableCreatorCallback() throws Exception {
        DynamoDBLeaseRefresher refresher = new DynamoDBLeaseRefresher(tableName, ddbClient,
                new DynamoDBLeaseSerializer(), true, tableCreatorCallback);

        refresher.performPostTableCreationAction();

        verify(tableCreatorCallback).performAction(
                eq(TableCreatorCallbackInput.builder().dynamoDbClient(ddbClient).tableName(tableName).build()));
    }
}
