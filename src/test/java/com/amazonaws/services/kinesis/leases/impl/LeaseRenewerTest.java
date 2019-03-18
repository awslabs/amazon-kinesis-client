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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

public class LeaseRenewerTest {

    ILeaseManager<Lease> leaseManager;
    String workerIdentifier;
    long leaseDurationMillis;
    ExecutorService leaseRenewalExecService;
    LeaseRenewer<Lease> renewer;
    List<Lease> leasesToRenew;

    private static Lease newLease(String leaseKey,
            String leaseOwner,
            Long leaseCounter,
            UUID concurrencyToken,
            Long lastCounterIncrementNanos) {
        Lease lease = new Lease();
        lease.setLeaseKey(leaseKey);
        lease.setLeaseOwner(leaseOwner);
        lease.setLeaseCounter(leaseCounter);
        lease.setConcurrencyToken(concurrencyToken);
        lease.setLastCounterIncrementNanos(lastCounterIncrementNanos);
        return lease;
    }

    private static Lease newLease(String leaseKey) {
        return newLease(leaseKey, "leaseOwner", 0L, UUID.randomUUID(), System.nanoTime());
    }

    @SuppressWarnings("unchecked")
    @Before
    public void before() {
        leaseManager = Mockito.mock(ILeaseManager.class);
        workerIdentifier = "workerId";
        leaseDurationMillis = 10000;
        leaseRenewalExecService = Executors.newSingleThreadExecutor();
        leasesToRenew = null;
        renewer = new LeaseRenewer<>(leaseManager,
                workerIdentifier,
                leaseDurationMillis,
                Executors.newCachedThreadPool());
    }

    @After
    public void after() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (leasesToRenew == null) {
            return;
        }
        for (Lease l : leasesToRenew) {
            Mockito.verify(leaseManager, Mockito.times(1)).renewLease(l);
        }
    }

    @Test
    public void testLeaseRenewerHoldsGoodLeases()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        /*
         * Prepare leases to be renewed
         * 2 Good
         */
        Lease lease1 = newLease("1");
        Lease lease2 = newLease("2");
        leasesToRenew =
                Arrays.asList(lease1,lease2);
        renewer.addLeasesToRenew(leasesToRenew);

        Mockito.doReturn(true).when(leaseManager).renewLease(lease1);
        Mockito.doReturn(true).when(leaseManager).renewLease(lease2);

        renewer.renewLeases();

        Assert.assertEquals(2, renewer.getCurrentlyHeldLeases().size());
    }

    @Test
    public void testLeaseRenewerDoesNotRenewExpiredLease() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        String leaseKey = "expiredLease";
        long initialCounterIncrementNanos = 5L; // "expired" time.
        Lease lease1 = newLease(leaseKey);
        lease1.setLastCounterIncrementNanos(initialCounterIncrementNanos);

        leasesToRenew = new ArrayList<>();
        leasesToRenew.add(lease1);
        Mockito.doReturn(true).when(leaseManager).renewLease(lease1);
        renewer.addLeasesToRenew(leasesToRenew);

        Assert.assertTrue(lease1.isExpired(1, System.nanoTime()));
        Assert.assertNull(renewer.getCurrentlyHeldLease(leaseKey));
        renewer.renewLeases();
        // Don't renew lease(s) with same key if getCurrentlyHeldLease returned null previously
        Assert.assertNull(renewer.getCurrentlyHeldLease(leaseKey));
        Assert.assertFalse(renewer.getCurrentlyHeldLeases().containsKey(leaseKey));

        // Clear the list to avoid triggering expectation mismatch in after().
        leasesToRenew.clear();
    }
}
