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
package software.amazon.kinesis.leases.dynamodb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.checkpoint.dynamodb.DynamoDBCheckpointer;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBLeaseCoordinatorIntegrationTest {
    private static final int ATTEMPTS = 20;
    private static final String OPERATION = "TestOperation";

    private static final String TABLE_NAME = DynamoDBLeaseCoordinatorIntegrationTest.class.getSimpleName();
    private static final String WORKER_ID = UUID.randomUUID().toString();
    private static final long LEASE_DURATION_MILLIS = 5000L;
    private static final long EPSILON_MILLIS = 25L;
    private static final int MAX_LEASES_FOR_WORKER = Integer.MAX_VALUE;
    private static final int MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1;
    private static final int MAX_LEASE_RENEWER_THREAD_COUNT = 20;
    private static final long INITIAL_LEASE_TABLE_READ_CAPACITY = 10L;
    private static final long INITIAL_LEASE_TABLE_WRITE_CAPACITY = 10L;

    private static DynamoDBLeaseRefresher leaseRefresher;
    private static DynamoDBCheckpointer dynamoDBCheckpointer;

    private LeaseCoordinator coordinator;
    private final String leaseKey = "shd-1";
    private final MetricsFactory metricsFactory = new NullMetricsFactory();

    @Before
    public void setup() throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        final boolean useConsistentReads = true;
        if (leaseRefresher == null) {
            DynamoDbAsyncClient dynamoDBClient = DynamoDbAsyncClient.builder()
                    .credentialsProvider(DefaultCredentialsProvider.create()).build();
            leaseRefresher = new DynamoDBLeaseRefresher(TABLE_NAME, dynamoDBClient, new DynamoDBLeaseSerializer(),
                    useConsistentReads, TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK);
        }
        leaseRefresher.createLeaseTableIfNotExists(10L, 10L);

        int retryLeft = ATTEMPTS;

        while (!leaseRefresher.leaseTableExists()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Sleep called.
            }
            retryLeft--;
            if (retryLeft == 0) {
                if (!leaseRefresher.leaseTableExists()) {
                    fail("Failed to create table");
                }
            }
        }

        leaseRefresher.deleteAll();
        coordinator = new DynamoDBLeaseCoordinator(leaseRefresher, WORKER_ID, LEASE_DURATION_MILLIS,
                EPSILON_MILLIS, MAX_LEASES_FOR_WORKER, MAX_LEASES_TO_STEAL_AT_ONE_TIME, MAX_LEASE_RENEWER_THREAD_COUNT,
                INITIAL_LEASE_TABLE_READ_CAPACITY, INITIAL_LEASE_TABLE_WRITE_CAPACITY, metricsFactory);
        dynamoDBCheckpointer = new DynamoDBCheckpointer(coordinator, leaseRefresher);
        dynamoDBCheckpointer.operation(OPERATION);

        coordinator.start();
    }

    /**
     * Tests update checkpoint success.
     */
    @Test
    public void testUpdateCheckpoint() throws Exception {
        TestHarnessBuilder builder = new TestHarnessBuilder();
        builder.withLease(leaseKey, null).build();

        // Run the taker and renewer in-between getting the Lease object and calling checkpoint
        coordinator.runLeaseTaker();
        coordinator.runLeaseRenewer();

        Lease lease = coordinator.getCurrentlyHeldLease(leaseKey);
        if (lease == null) {
            List<Lease> leases = leaseRefresher.listLeases();
            for (Lease kinesisClientLease : leases) {
                System.out.println(kinesisClientLease);
            }
        }

        assertNotNull(lease);
        ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber("newCheckpoint");
        // lease's leaseCounter is wrong at this point, but it shouldn't matter.
        assertTrue(dynamoDBCheckpointer.setCheckpoint(lease.leaseKey(), newCheckpoint, lease.concurrencyToken()));

        Lease fromDynamo = leaseRefresher.getLease(lease.leaseKey());

        lease.leaseCounter(lease.leaseCounter() + 1);
        lease.checkpoint(newCheckpoint);
        lease.leaseOwner(coordinator.workerIdentifier());
        assertEquals(lease, fromDynamo);
    }

    /**
     * Tests if getAllAssignments() returns all leases
     */
    @Test
    public void testGetAllAssignments() throws Exception {
        TestHarnessBuilder builder = new TestHarnessBuilder();

        Map<String, Lease> addedLeases = builder.withLease("1", WORKER_ID)
                .withLease("2", WORKER_ID)
                .withLease("3", WORKER_ID)
                .withLease("4", WORKER_ID)
                .withLease("5", WORKER_ID)
                .build();

        // Run the taker
        coordinator.runLeaseTaker();

        List<Lease> allLeases = coordinator.allLeases();
        assertThat(allLeases.size(), equalTo(addedLeases.size()));
        assertThat(allLeases.containsAll(addedLeases.values()), equalTo(true));
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
        Lease lease = coordinator.getCurrentlyHeldLease(leaseKey);

        assertNotNull(lease);
        leaseRefresher.renewLease(coordinator.getCurrentlyHeldLease(leaseKey));

        ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber("newCheckpoint");
        assertFalse(dynamoDBCheckpointer.setCheckpoint(lease.leaseKey(), newCheckpoint, lease.concurrencyToken()));

        Lease fromDynamo = leaseRefresher.getLease(lease.leaseKey());

        lease.leaseCounter(lease.leaseCounter() + 1);
        // Counter and owner changed, but checkpoint did not.
        lease.leaseOwner(coordinator.workerIdentifier());
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
        Lease lease = coordinator.getCurrentlyHeldLease(leaseKey);

        assertNotNull(lease);

        ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber("newCheckpoint");
        assertFalse(dynamoDBCheckpointer.setCheckpoint(lease.leaseKey(), newCheckpoint, UUID.randomUUID()));

        Lease fromDynamo = leaseRefresher.getLease(lease.leaseKey());

        // Owner should be the only thing that changed.
        lease.leaseOwner(coordinator.workerIdentifier());
        assertEquals(lease, fromDynamo);
    }

    public static class TestHarnessBuilder {

        private Map<String, Lease> leases = new HashMap<>();

        public TestHarnessBuilder withLease(String shardId, String owner) {
            Lease lease = new Lease();
            lease.checkpoint(new ExtendedSequenceNumber("checkpoint"));
            lease.ownerSwitchesSinceCheckpoint(0L);
            lease.leaseCounter(0L);
            lease.leaseOwner(owner);
            lease.parentShardIds(Collections.singleton("parentShardId"));
            lease.leaseKey(shardId);
            leases.put(shardId, lease);
            return this;
        }

        public Map<String, Lease> build() throws LeasingException {
            for (Lease lease : leases.values()) {
                leaseRefresher.createLeaseIfNotExists(lease);
                if (lease.leaseOwner() != null) {
                    lease.lastCounterIncrementNanos(System.nanoTime());
                }
            }
            return leases;
        }
    }

}
