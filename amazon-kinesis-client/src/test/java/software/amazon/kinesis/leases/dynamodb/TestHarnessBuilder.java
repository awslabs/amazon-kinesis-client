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
package software.amazon.kinesis.leases.dynamodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRenewer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestHarnessBuilder {

    private long currentTimeNanos;

    private Map<String, Lease> leases = new HashMap<>();
    private DynamoDBLeaseRefresher leaseRefresher;
    private Map<String, Lease> originalLeases = new HashMap<>();

    private Callable<Long> timeProvider = new Callable<Long>() {

        @Override
        public Long call() throws Exception {
            return currentTimeNanos;
        }
    };

    public TestHarnessBuilder(final DynamoDBLeaseRefresher leaseRefresher) {
        this.leaseRefresher = leaseRefresher;
    }

    public TestHarnessBuilder withLease(String shardId) {
        return withLease(shardId, "leaseOwner");
    }

    public TestHarnessBuilder withLease(String shardId, String owner) {
        Lease lease = createLease(shardId, owner);
        Lease originalLease = createLease(shardId, owner);

        leases.put(shardId, lease);
        originalLeases.put(shardId, originalLease);
        return this;
    }

    private Lease createLease(String shardId, String owner) {
        Lease lease = new Lease();
        lease.checkpoint(new ExtendedSequenceNumber("checkpoint"));
        lease.ownerSwitchesSinceCheckpoint(0L);
        lease.leaseCounter(0L);
        lease.leaseOwner(owner);
        lease.parentShardIds(Collections.singleton("parentShardId"));
        lease.childShardIds(new HashSet<>());
        lease.leaseKey(shardId);

        return lease;
    }

    public Map<String, Lease> build() throws LeasingException {
        for (Lease lease : leases.values()) {
            leaseRefresher.createLeaseIfNotExists(lease);
            if (lease.leaseOwner() != null) {
                lease.lastCounterIncrementNanos(System.nanoTime());
            }
        }

        currentTimeNanos = System.nanoTime();

        return leases;
    }

    public void passTime(long millis) {
        currentTimeNanos += millis * 1000000;
    }

    public Map<String, Lease> takeMutateAssert(DynamoDBLeaseTaker taker, int numToTake) throws LeasingException {
        Map<String, Lease> result = taker.takeLeases(timeProvider);
        assertEquals(numToTake, result.size());

        for (Lease actual : result.values()) {
            Lease original = leases.get(actual.leaseKey());
            assertNotNull(original);

            mutateAssert(taker.getWorkerIdentifier(), original, actual);
        }

        return result;
    }

    public Map<String, Lease> stealMutateAssert(DynamoDBLeaseTaker taker, int numToTake) throws LeasingException {
        Map<String, Lease> result = taker.takeLeases(timeProvider);
        assertEquals(numToTake, result.size());

        for (Lease actual : result.values()) {
            Lease original = leases.get(actual.leaseKey());
            assertNotNull(original);

            original.isMarkedForLeaseSteal(true).lastCounterIncrementNanos(actual.lastCounterIncrementNanos());
            mutateAssert(taker.getWorkerIdentifier(), original, actual);
        }

        return result;
    }

    public Map<String, Lease> takeMutateAssert(DynamoDBLeaseTaker taker, String... takenShardIds)
            throws LeasingException {
        Map<String, Lease> result = taker.takeLeases(timeProvider);
        assertEquals(takenShardIds.length, result.size());

        for (String shardId : takenShardIds) {
            Lease original = leases.get(shardId);
            assertNotNull(original);

            Lease actual = result.get(shardId);
            assertNotNull(actual);

            mutateAssert(taker.getWorkerIdentifier(), original, actual);
        }

        return result;
    }

    private void mutateAssert(String newWorkerIdentifier, Lease original, Lease actual) {
        original.leaseCounter(original.leaseCounter() + 1);
        if (original.leaseOwner() != null && !newWorkerIdentifier.equals(original.leaseOwner())) {
            original.ownerSwitchesSinceCheckpoint(original.ownerSwitchesSinceCheckpoint() + 1);
        }
        original.leaseOwner(newWorkerIdentifier);

        assertEquals(original, actual); // Assert the contents of the lease
    }

    public void addLeasesToRenew(LeaseRenewer renewer, String... shardIds)
            throws DependencyException, InvalidStateException {
        List<Lease> leasesToRenew = new ArrayList<Lease>();

        for (String shardId : shardIds) {
            Lease lease = leases.get(shardId);
            assertNotNull(lease);
            leasesToRenew.add(lease);
        }

        renewer.addLeasesToRenew(leasesToRenew);
    }

    public Map<String, Lease> renewMutateAssert(LeaseRenewer renewer, String... renewedShardIds)
            throws DependencyException, InvalidStateException {
        renewer.renewLeases();

        Map<String, Lease> heldLeases = renewer.getCurrentlyHeldLeases();
        assertEquals(renewedShardIds.length, heldLeases.size());

        for (String shardId : renewedShardIds) {
            Lease original = originalLeases.get(shardId);
            assertNotNull(original);

            Lease actual = heldLeases.get(shardId);
            assertNotNull(actual);

            original.leaseCounter(original.leaseCounter() + 1);
            assertEquals(original, actual);
        }

        return heldLeases;
    }

    public void renewAllLeases() throws Exception {
        for (Lease lease : leases.values()) {
            leaseRefresher.renewLease(lease);
        }
    }
}
