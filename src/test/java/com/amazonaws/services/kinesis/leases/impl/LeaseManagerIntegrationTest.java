/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.kinesis.leases.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import junit.framework.Assert;

import org.junit.Test;

import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import org.mockito.Mockito;

public class LeaseManagerIntegrationTest extends LeaseIntegrationTest {

    /**
     * Test listLeases when no records are present.
     */
    @Test
    public void testListNoRecords() throws LeasingException {
        List<KinesisClientLease> leases = leaseManager.listLeases();
        Assert.assertTrue(leases.isEmpty());
    }

    /**
     * Tests listLeases when records are present. Exercise dynamo's paging functionality.
     */
    @Test
    public void testListWithRecords() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);

        int numRecordsToPut = 10;

        for (int i = 0; i < numRecordsToPut; i++) {
            builder.withLease(Integer.toString(i));
        }

        Collection<KinesisClientLease> expected = builder.build().values();

        // The / 3 here ensures that we will test Dynamo's paging mechanics.
        List<KinesisClientLease> actual = leaseManager.list(numRecordsToPut / 3);

        for (KinesisClientLease lease : actual) {
            Assert.assertNotNull(expected.remove(lease));
        }

        Assert.assertTrue(expected.isEmpty());
    }

    /**
     * Tests getLease when a record is present.
     */
    @Test
    public void testGetLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        Lease expected = builder.withLease("1").build().get("1");

        Lease actual = leaseManager.getLease(expected.getLeaseKey());
        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests leaseManager.get() when the looked-for record is absent.
     */
    @Test
    public void testGetNull() throws LeasingException {
        Lease actual = leaseManager.getLease("bogusShardId");
        Assert.assertNull(actual);
    }

    /**
     * Tests leaseManager.holdLease's success scenario.
     */
    @Test
    public void testRenewLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        KinesisClientLease lease = builder.withLease("1").build().get("1");
        Long originalLeaseCounter = lease.getLeaseCounter();

        leaseManager.renewLease(lease);
        Assert.assertTrue(originalLeaseCounter + 1 == lease.getLeaseCounter());

        Lease fromDynamo = leaseManager.getLease(lease.getLeaseKey());

        Assert.assertEquals(lease, fromDynamo);
    }

    /**
     * Tests leaseManager.holdLease when the lease has changed out from under us.
     */
    @Test
    public void testHoldUpdatedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        KinesisClientLease lease = builder.withLease("1").build().get("1");

        KinesisClientLease leaseCopy = leaseManager.getLease(lease.getLeaseKey());

        // lose lease
        leaseManager.takeLease(lease, "bar");

        Assert.assertFalse(leaseManager.renewLease(leaseCopy));
    }

    /**
     * Tests leaseManager.updateLeaseWithMetaInfo() when the lease is deleted before updating it with meta info
     */
    @Test
    public void testDeleteLeaseThenUpdateLeaseWithMetaInfo() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        KinesisClientLease lease = builder.withLease("1").build().get("1");
        final String leaseKey = lease.getLeaseKey();
        leaseManager.deleteLease(lease);
        leaseManager.updateLeaseWithMetaInfo(lease, UpdateField.HASH_KEY_RANGE);
        final KinesisClientLease deletedLease = leaseManager.getLease(leaseKey);
        Assert.assertNull(deletedLease);
    }

    /**
     * Tests leaseManager.updateLeaseWithMetaInfo() on hashKeyRange update
     */
    @Test
    public void testUpdateLeaseWithMetaInfo() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        KinesisClientLease lease = builder.withLease("1").build().get("1");
        final String leaseKey = lease.getLeaseKey();
        final HashKeyRangeForLease hashKeyRangeForLease = HashKeyRangeForLease.fromHashKeyRange(new HashKeyRange()
                                                                                                   .withStartingHashKey("1")
                                                                                                   .withEndingHashKey("2"));
        lease.setHashKeyRange(hashKeyRangeForLease);
        leaseManager.updateLeaseWithMetaInfo(lease, UpdateField.HASH_KEY_RANGE);
        final KinesisClientLease updatedLease = leaseManager.getLease(leaseKey);
        Assert.assertEquals(lease, updatedLease);
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
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        KinesisClientLease lease = builder.withLease("1", owned ? "originalOwner" : null).build().get("1");
        Long originalLeaseCounter = lease.getLeaseCounter();

        String newOwner = "newOwner";
        leaseManager.takeLease(lease, newOwner);
        Assert.assertTrue(originalLeaseCounter + 1 == lease.getLeaseCounter());
        Assert.assertTrue((owned ? 1 : 0) == lease.getOwnerSwitchesSinceCheckpoint());
        Assert.assertEquals(newOwner, lease.getLeaseOwner());

        Lease fromDynamo = leaseManager.getLease(lease.getLeaseKey());

        Assert.assertEquals(lease, fromDynamo);
    }

    /**
     * Tests takeLease when the lease has changed out from under us.
     */
    @Test
    public void testTakeUpdatedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        KinesisClientLease lease = builder.withLease("1").build().get("1");

        KinesisClientLease leaseCopy = leaseManager.getLease(lease.getLeaseKey());

        String newOwner = "newOwner";
        leaseManager.takeLease(lease, newOwner);

        Assert.assertFalse(leaseManager.takeLease(leaseCopy, newOwner));
    }

    /**
     * Tests evictLease when the lease is currently unowned.
     */
    public void testEvictUnownedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        KinesisClientLease lease = builder.withLease("1", null).build().get("1");

        Assert.assertFalse(leaseManager.evictLease(lease));
    }

    /**
     * Tests evictLease when the lease is currently owned.
     */
    @Test
    public void testEvictOwnedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        KinesisClientLease lease = builder.withLease("1").build().get("1");
        Long originalLeaseCounter = lease.getLeaseCounter();

        leaseManager.evictLease(lease);
        Assert.assertNull(lease.getLeaseOwner());
        Assert.assertTrue(originalLeaseCounter + 1 == lease.getLeaseCounter());

        Lease fromDynamo = leaseManager.getLease(lease.getLeaseKey());

        Assert.assertEquals(lease, fromDynamo);
    }

    /**
     * Tests evictLease when the lease has changed out from under us. Note that evicting leases
     * is conditional on the lease owner, unlike everything else which is conditional on the
     * lease counter.
     */
    @Test
    public void testEvictChangedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        KinesisClientLease lease = builder.withLease("1").build().get("1");

        // Change the owner only - this should cause our optimistic lock to fail.
        lease.setLeaseOwner("otherOwner");
        Assert.assertFalse(leaseManager.evictLease(lease));
    }

    /**
     * Tests deleteLease when a lease exists.
     */
    @Test
    public void testDeleteLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseManager);
        KinesisClientLease lease = builder.withLease("1").build().get("1");

        leaseManager.deleteLease(lease);

        KinesisClientLease newLease = leaseManager.getLease(lease.getLeaseKey());
        Assert.assertNull(newLease);
    }

    /**
     * Tests deleteLease when a lease does not exist.
     */
    @Test
    public void testDeleteNonexistentLease() throws LeasingException {
        KinesisClientLease lease = new KinesisClientLease();
        lease.setLeaseKey("1");
        // The lease has not been written to DDB - try to delete it and expect success.

        leaseManager.deleteLease(lease);
    }

    @Test
    public void testWaitUntilLeaseTableExists() throws LeasingException {
        AmazonDynamoDBClient ddbMock = Mockito.mock(ddbClient.getClass());
        DescribeTableResult result = Mockito.mock(DescribeTableResult.class);
        TableDescription description = Mockito.mock(TableDescription.class);
        Mockito.when(description.getTableStatus()).thenReturn(TableStatus.ACTIVE.name());
        Mockito.when(result.getTable()).thenReturn(description);
        Mockito.when(ddbMock.describeTable(Mockito.any(DescribeTableRequest.class))).thenReturn(result);
        KinesisClientLeaseManager manager = new KinesisClientLeaseManager("existing_table", ddbMock, true,
                KinesisClientLibConfiguration.DEFAULT_DDB_BILLING_MODE) {

            @Override
            long sleep(long timeToSleepMillis) {
                Assert.fail("Should not sleep");
                return 0L;
            }

        };


        Assert.assertTrue(manager.waitUntilLeaseTableExists(1, 1));
    }

    @Test
    public void testWaitUntilLeaseTableExistsUpdatingStatus() throws LeasingException {
        AmazonDynamoDBClient ddbMock = Mockito.mock(ddbClient.getClass());
        DescribeTableResult result = Mockito.mock(DescribeTableResult.class);
        TableDescription description = Mockito.mock(TableDescription.class);
        Mockito.when(description.getTableStatus()).thenReturn(TableStatus.UPDATING.name());
        Mockito.when(result.getTable()).thenReturn(description);
        Mockito.when(ddbMock.describeTable(Mockito.any(DescribeTableRequest.class))).thenReturn(result);
        KinesisClientLeaseManager manager = new KinesisClientLeaseManager("existing_table", ddbMock, true,
                KinesisClientLibConfiguration.DEFAULT_DDB_BILLING_MODE) {

            @Override
            long sleep(long timeToSleepMillis) {
                Assert.fail("Should not sleep");
                return 0L;
            }

        };


        Assert.assertTrue(manager.waitUntilLeaseTableExists(1, 1));
    }

    @Test
    public void testWaitUntilLeaseTableExistsPayPerRequest() throws LeasingException {
        AmazonDynamoDBClient ddbMock = Mockito.mock(ddbClient.getClass());
        DescribeTableResult result = Mockito.mock(DescribeTableResult.class);
        TableDescription description = Mockito.mock(TableDescription.class);
        Mockito.when(description.getTableStatus()).thenReturn(TableStatus.ACTIVE.name());
        Mockito.when(result.getTable()).thenReturn(description);
        Mockito.when(ddbMock.describeTable(Mockito.any(DescribeTableRequest.class))).thenReturn(result);
        KinesisClientLeaseManager manager = new KinesisClientLeaseManager("existing_table", ddbMock, true,
                BillingMode.PAY_PER_REQUEST) {

            @Override
            long sleep(long timeToSleepMillis) {
                Assert.fail("Should not sleep");
                return 0L;
            }

        };

        Assert.assertTrue(manager.waitUntilLeaseTableExists(1, 1));
    }

    @Test
    public void testWaitUntilLeaseTableExistsTimeout() throws LeasingException {
        /*
         * Just using AtomicInteger for the indirection it provides.
         */
        final AtomicInteger sleepCounter = new AtomicInteger(0);
        KinesisClientLeaseManager manager = new KinesisClientLeaseManager("nonexistentTable", ddbClient, true,
                KinesisClientLibConfiguration.DEFAULT_DDB_BILLING_MODE) {

            @Override
            long sleep(long timeToSleepMillis) {
                Assert.assertEquals(1000L, timeToSleepMillis);
                sleepCounter.incrementAndGet();
                return 1000L;
            }

        };

        Assert.assertFalse(manager.waitUntilLeaseTableExists(2, 1));
        Assert.assertEquals(1, sleepCounter.get());
    }
}
