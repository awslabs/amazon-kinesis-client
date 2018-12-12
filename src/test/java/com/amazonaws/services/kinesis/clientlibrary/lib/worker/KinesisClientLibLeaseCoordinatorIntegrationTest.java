/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.leases.impl.Lease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseRenewer;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class KinesisClientLibLeaseCoordinatorIntegrationTest {

    private static KinesisClientLeaseManager leaseManager;
    private KinesisClientLibLeaseCoordinator coordinator;
    private static final String TABLE_NAME = KinesisClientLibLeaseCoordinatorIntegrationTest.class.getSimpleName();
    private static final String WORKER_ID = UUID.randomUUID().toString();
    private final String leaseKey = "shd-1";


    @Before
    public void setUp() throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        final boolean useConsistentReads = true;
        if (leaseManager == null) {
            AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
            leaseManager =
                    new KinesisClientLeaseManager(TABLE_NAME, ddb, useConsistentReads);
        }
        leaseManager.createLeaseTableIfNotExists(10L, 10L);
        leaseManager.deleteAll();
        coordinator = new KinesisClientLibLeaseCoordinator(leaseManager, WORKER_ID, 5000L, 50L);
        coordinator.start();
    }

    /**
     * Tests update checkpoint success.
     */
    @Test
    public void testUpdateCheckpoint() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder();
        builder.withLease(leaseKey, null).build();

        // Run the taker and renewer in-between getting the Lease object and calling setCheckpoint
        coordinator.runLeaseTaker();
        coordinator.runLeaseRenewer();

        KinesisClientLease lease = coordinator.getCurrentlyHeldLease(leaseKey);
        if (lease == null) {
            List<KinesisClientLease> leases = leaseManager.listLeases();
            for (KinesisClientLease kinesisClientLease : leases) {
                System.out.println(kinesisClientLease);
            }
        }

        assertThat(lease, notNullValue());
        ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber("newCheckpoint");
        // lease's leaseCounter is wrong at this point, but it shouldn't matter.
        Assert.assertTrue(coordinator.setCheckpoint(lease.getLeaseKey(), newCheckpoint, lease.getConcurrencyToken()));

        Lease fromDynamo = leaseManager.getLease(lease.getLeaseKey());

        lease.setLeaseCounter(lease.getLeaseCounter() + 1);
        lease.setCheckpoint(newCheckpoint);
        lease.setLeaseOwner(coordinator.getWorkerIdentifier());
        Assert.assertEquals(lease, fromDynamo);
    }

    /**
     * Tests updateCheckpoint when the lease has changed out from under us.
     */
    @Test
    public void testUpdateCheckpointLeaseUpdated() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder();
        builder.withLease(leaseKey, null).build();

        coordinator.runLeaseTaker();
        coordinator.runLeaseRenewer();
        KinesisClientLease lease = coordinator.getCurrentlyHeldLease(leaseKey);

        assertThat(lease, notNullValue());
        leaseManager.renewLease(coordinator.getCurrentlyHeldLease(leaseKey));

        ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber("newCheckpoint");
        Assert.assertFalse(coordinator.setCheckpoint(lease.getLeaseKey(), newCheckpoint, lease.getConcurrencyToken()));

        Lease fromDynamo = leaseManager.getLease(lease.getLeaseKey());

        lease.setLeaseCounter(lease.getLeaseCounter() + 1);
        // Counter and owner changed, but checkpoint did not.
        lease.setLeaseOwner(coordinator.getWorkerIdentifier());
        Assert.assertEquals(lease, fromDynamo);
    }

    /**
     * Tests updateCheckpoint with a bad concurrency token.
     */
    @Test
    public void testUpdateCheckpointBadConcurrencyToken() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder();
        builder.withLease(leaseKey, null).build();

        coordinator.runLeaseTaker();
        coordinator.runLeaseRenewer();
        KinesisClientLease lease = coordinator.getCurrentlyHeldLease(leaseKey);

        assertThat(lease, notNullValue());

        ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber("newCheckpoint");
        Assert.assertFalse(coordinator.setCheckpoint(lease.getLeaseKey(), newCheckpoint, UUID.randomUUID()));

        Lease fromDynamo = leaseManager.getLease(lease.getLeaseKey());

        // Owner should be the only thing that changed.
        lease.setLeaseOwner(coordinator.getWorkerIdentifier());
        Assert.assertEquals(lease, fromDynamo);
    }

    public static class TestHarnessBuilder {

        private long currentTimeNanos;

        private Map<String, KinesisClientLease> leases = new HashMap<String, KinesisClientLease>();

        private Callable<Long> timeProvider = new Callable<Long>() {

            @Override
            public Long call() throws Exception {
                return currentTimeNanos;
            }

        };

        public TestHarnessBuilder withLease(String shardId) {
            return withLease(shardId, "leaseOwner");
        }

        public TestHarnessBuilder withLease(String shardId, String owner) {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setCheckpoint(new ExtendedSequenceNumber("checkpoint"));
            lease.setOwnerSwitchesSinceCheckpoint(0L);
            lease.setLeaseCounter(0L);
            lease.setLeaseOwner(owner);
            lease.setParentShardIds(Collections.singleton("parentShardId"));
            lease.setLeaseKey(shardId);

            leases.put(shardId, lease);
            return this;
        }

        public Map<String, KinesisClientLease> build() throws LeasingException {
            for (KinesisClientLease lease : leases.values()) {
                leaseManager.createLeaseIfNotExists(lease);
                if (lease.getLeaseOwner() != null) {
                    lease.setLastCounterIncrementNanos(System.nanoTime());
                }
            }

            currentTimeNanos = System.nanoTime();

            return leases;
        }

        public void passTime(long millis) {
            currentTimeNanos += millis * 1000000;
        }

        private void mutateAssert(String newWorkerIdentifier, KinesisClientLease original, KinesisClientLease actual) {
            original.setLeaseCounter(original.getLeaseCounter() + 1);
            if (original.getLeaseOwner() != null && !newWorkerIdentifier.equals(original.getLeaseOwner())) {
                original.setOwnerSwitchesSinceCheckpoint(original.getOwnerSwitchesSinceCheckpoint() + 1);
            }
            original.setLeaseOwner(newWorkerIdentifier);

            Assert.assertEquals(original, actual); // Assert the contents of the lease
        }

        public void addLeasesToRenew(ILeaseRenewer<KinesisClientLease> renewer, String... shardIds)
            throws DependencyException, InvalidStateException {
            List<KinesisClientLease> leasesToRenew = new ArrayList<KinesisClientLease>();

            for (String shardId : shardIds) {
                KinesisClientLease lease = leases.get(shardId);
                Assert.assertNotNull(lease);
                leasesToRenew.add(lease);
            }

            renewer.addLeasesToRenew(leasesToRenew);
        }

        public Map<String, KinesisClientLease> renewMutateAssert(ILeaseRenewer<KinesisClientLease> renewer,
                String... renewedShardIds) throws DependencyException, InvalidStateException {
            renewer.renewLeases();

            Map<String, KinesisClientLease> heldLeases = renewer.getCurrentlyHeldLeases();
            Assert.assertEquals(renewedShardIds.length, heldLeases.size());

            for (String shardId : renewedShardIds) {
                KinesisClientLease original = leases.get(shardId);
                Assert.assertNotNull(original);

                KinesisClientLease actual = heldLeases.get(shardId);
                Assert.assertNotNull(actual);

                original.setLeaseCounter(original.getLeaseCounter() + 1);
                Assert.assertEquals(original, actual);
            }

            return heldLeases;
        }

        public void renewAllLeases() throws LeasingException {
            for (KinesisClientLease lease : leases.values()) {
                leaseManager.renewLease(lease);
            }
        }
    }

}
