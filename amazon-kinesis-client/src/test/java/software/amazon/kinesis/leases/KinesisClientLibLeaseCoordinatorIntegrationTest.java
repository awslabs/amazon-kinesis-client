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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

import software.amazon.kinesis.checkpoint.DynamoDBCheckpointer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@RunWith(MockitoJUnitRunner.class)
public class KinesisClientLibLeaseCoordinatorIntegrationTest {
    private static final String TABLE_NAME = KinesisClientLibLeaseCoordinatorIntegrationTest.class.getSimpleName();
    private static final String WORKER_ID = UUID.randomUUID().toString();
    private static final long LEASE_DURATION_MILLIS = 5000L;
    private static final long EPSILON_MILLIS = 25L;
    private static final int MAX_LEASES_FOR_WORKER = Integer.MAX_VALUE;
    private static final int MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1;
    private static final int MAX_LEASE_RENEWER_THREAD_COUNT = 20;
    private static KinesisClientDynamoDBLeaseManager leaseManager;
    private static DynamoDBCheckpointer dynamoDBCheckpointer;

    private KinesisClientLibLeaseCoordinator coordinator;
    private final String leaseKey = "shd-1";
    private final IMetricsFactory metricsFactory = new NullMetricsFactory();

    @Before
    public void setUp() throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        final boolean useConsistentReads = true;
        if (leaseManager == null) {
            AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
            leaseManager =
                    new KinesisClientDynamoDBLeaseManager(TABLE_NAME, ddb, useConsistentReads);
        }
        leaseManager.createLeaseTableIfNotExists(10L, 10L);
        leaseManager.deleteAll();
        coordinator = new KinesisClientLibLeaseCoordinator(leaseManager, WORKER_ID, LEASE_DURATION_MILLIS,
                EPSILON_MILLIS, MAX_LEASES_FOR_WORKER, MAX_LEASES_TO_STEAL_AT_ONE_TIME, MAX_LEASE_RENEWER_THREAD_COUNT,
                metricsFactory);
        dynamoDBCheckpointer = new DynamoDBCheckpointer(coordinator, leaseManager, metricsFactory);

        coordinator.start();
    }

    /**
     * Tests update checkpoint success.
     */
    @Test
    public void testUpdateCheckpoint() throws Exception {
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

        assertNotNull(lease);
        ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber("newCheckpoint");
        // lease's leaseCounter is wrong at this point, but it shouldn't matter.
        assertTrue(dynamoDBCheckpointer.setCheckpoint(lease.getLeaseKey(), newCheckpoint, lease.getConcurrencyToken()));

        Lease fromDynamo = leaseManager.getLease(lease.getLeaseKey());

        lease.setLeaseCounter(lease.getLeaseCounter() + 1);
        lease.setCheckpoint(newCheckpoint);
        lease.setLeaseOwner(coordinator.getWorkerIdentifier());
        assertEquals(lease, fromDynamo);
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

        assertNotNull(lease);
        leaseManager.renewLease(coordinator.getCurrentlyHeldLease(leaseKey));

        ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber("newCheckpoint");
        assertFalse(dynamoDBCheckpointer.setCheckpoint(lease.getLeaseKey(), newCheckpoint, lease.getConcurrencyToken()));

        Lease fromDynamo = leaseManager.getLease(lease.getLeaseKey());

        lease.setLeaseCounter(lease.getLeaseCounter() + 1);
        // Counter and owner changed, but checkpoint did not.
        lease.setLeaseOwner(coordinator.getWorkerIdentifier());
        assertEquals(lease, fromDynamo);
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

        assertNotNull(lease);

        ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber("newCheckpoint");
        assertFalse(dynamoDBCheckpointer.setCheckpoint(lease.getLeaseKey(), newCheckpoint, UUID.randomUUID()));

        Lease fromDynamo = leaseManager.getLease(lease.getLeaseKey());

        // Owner should be the only thing that changed.
        lease.setLeaseOwner(coordinator.getWorkerIdentifier());
        assertEquals(lease, fromDynamo);
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

            assertEquals(original, actual); // Assert the contents of the lease
        }

        public void addLeasesToRenew(LeaseRenewer<KinesisClientLease> renewer, String... shardIds)
            throws DependencyException, InvalidStateException {
            List<KinesisClientLease> leasesToRenew = new ArrayList<KinesisClientLease>();

            for (String shardId : shardIds) {
                KinesisClientLease lease = leases.get(shardId);
                assertNotNull(lease);
                leasesToRenew.add(lease);
            }

            renewer.addLeasesToRenew(leasesToRenew);
        }

        public Map<String, KinesisClientLease> renewMutateAssert(LeaseRenewer<KinesisClientLease> renewer,
                String... renewedShardIds) throws DependencyException, InvalidStateException {
            renewer.renewLeases();

            Map<String, KinesisClientLease> heldLeases = renewer.getCurrentlyHeldLeases();
            assertEquals(renewedShardIds.length, heldLeases.size());

            for (String shardId : renewedShardIds) {
                KinesisClientLease original = leases.get(shardId);
                assertNotNull(original);

                KinesisClientLease actual = heldLeases.get(shardId);
                assertNotNull(actual);

                original.setLeaseCounter(original.getLeaseCounter() + 1);
                assertEquals(original, actual);
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
