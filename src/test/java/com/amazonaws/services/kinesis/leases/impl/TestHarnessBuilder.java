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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Assert;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseRenewer;

public class TestHarnessBuilder {

    private long currentTimeNanos;

    private Map<String, KinesisClientLease> leases = new HashMap<String, KinesisClientLease>();
    private KinesisClientLeaseManager leaseManager;
    private Map<String, KinesisClientLease> originalLeases = new HashMap<>();

    private Callable<Long> timeProvider = new Callable<Long>() {

        @Override
        public Long call() throws Exception {
            return currentTimeNanos;
        }

    };

    public TestHarnessBuilder(KinesisClientLeaseManager leaseManager) {
        this.leaseManager = leaseManager;
    }

    public TestHarnessBuilder withLease(String shardId) {
        return withLease(shardId, "leaseOwner");
    }

    public TestHarnessBuilder withLease(String shardId, String owner) {
        KinesisClientLease lease = createLease(shardId, owner);
        KinesisClientLease originalLease = createLease(shardId, owner);

        leases.put(shardId, lease);
        originalLeases.put(shardId, originalLease);
        return this;
    }

    private KinesisClientLease createLease(String shardId, String owner) {
        KinesisClientLease lease = new KinesisClientLease();
        lease.setCheckpoint(new ExtendedSequenceNumber("checkpoint"));
        lease.setOwnerSwitchesSinceCheckpoint(0L);
        lease.setLeaseCounter(0L);
        lease.setLeaseOwner(owner);
        lease.setParentShardIds(Collections.singleton("parentShardId"));
        lease.setLeaseKey(shardId);

        return lease;
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

    public Map<String, KinesisClientLease> takeMutateAssert(LeaseTaker<KinesisClientLease> taker, int numToTake)
        throws LeasingException {
        Map<String, KinesisClientLease> result = taker.takeLeases(timeProvider);
        Assert.assertEquals(numToTake, result.size());

        for (KinesisClientLease actual : result.values()) {
            KinesisClientLease original = leases.get(actual.getLeaseKey());
            Assert.assertNotNull(original);

            mutateAssert(taker.getWorkerIdentifier(), original, actual);
        }

        return result;
    }

    public Map<String, KinesisClientLease> takeMutateAssert(LeaseTaker<KinesisClientLease> taker, String... takenShardIds)
        throws LeasingException {
        Map<String, KinesisClientLease> result = taker.takeLeases(timeProvider);
        Assert.assertEquals(takenShardIds.length, result.size());

        for (String shardId : takenShardIds) {
            KinesisClientLease original = leases.get(shardId);
            Assert.assertNotNull(original);

            KinesisClientLease actual = result.get(shardId);
            Assert.assertNotNull(actual);

            mutateAssert(taker.getWorkerIdentifier(), original, actual);
        }

        return result;
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

    public Map<String, KinesisClientLease> renewMutateAssert(ILeaseRenewer<KinesisClientLease> renewer, String... renewedShardIds)
        throws DependencyException, InvalidStateException {
        renewer.renewLeases();

        Map<String, KinesisClientLease> heldLeases = renewer.getCurrentlyHeldLeases();
        Assert.assertEquals(renewedShardIds.length, heldLeases.size());

        for (String shardId : renewedShardIds) {
            KinesisClientLease original = originalLeases.get(shardId);
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
