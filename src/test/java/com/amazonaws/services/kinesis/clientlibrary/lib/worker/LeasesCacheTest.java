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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.ILeasesCache;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LeasesCacheTest extends PeriodicShardSyncTestBase {

    private static final int TTL = 10;
    private static final int SHORTER_TTL = 1;
    private static final TimeUnit TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

    @Mock
    ILeaseManager<KinesisClientLease> leaseManager;
    
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testDependencyExceptionFromLeaseManager() throws Exception {
        when(leaseManager.listLeases()).thenThrow(DependencyException.class);
        exceptionRule.expect(LeasesCacheException.class);
        exceptionRule.expectCause(IsInstanceOf.<Throwable> instanceOf(DependencyException.class));
        ILeasesCache<KinesisClientLease> leasesCache = new LeasesCache(leaseManager, TTL, TTL_TIME_UNIT);
        leasesCache.getLeases(DeterministicShuffleLeaderElection.LEAES_CACHE_KEY);
    }

    @Test
    public void testInvalidStateExceptionFromLeaseManager() throws Exception {
        when(leaseManager.listLeases()).thenThrow(InvalidStateException.class);
        exceptionRule.expect(LeasesCacheException.class);
        exceptionRule.expectCause(IsInstanceOf.<Throwable> instanceOf(InvalidStateException.class));
        ILeasesCache<KinesisClientLease> leasesCache = new LeasesCache(leaseManager, TTL, TTL_TIME_UNIT);
        leasesCache.getLeases(DeterministicShuffleLeaderElection.LEAES_CACHE_KEY);
    }

    @Test
    public void testProvisionedThroughputExceptionFromLeaseManager() throws Exception {
        when(leaseManager.listLeases()).thenThrow(ProvisionedThroughputException.class);
        exceptionRule.expect(LeasesCacheException.class);
        exceptionRule.expectCause(IsInstanceOf.<Throwable> instanceOf(ProvisionedThroughputException.class));
        ILeasesCache<KinesisClientLease> leasesCache = new LeasesCache(leaseManager, TTL, TTL_TIME_UNIT);
        leasesCache.getLeases(DeterministicShuffleLeaderElection.LEAES_CACHE_KEY);
    }

    @Test
    public void testCacheMiss() throws Exception {
        List<KinesisClientLease> expectedLeases = getLeases(5, false /* duplicateLeaseOwner */,
                true /* activeLeases */);
        when(leaseManager.listLeases()).thenReturn(expectedLeases);
        ILeasesCache<KinesisClientLease> leasesCache = new LeasesCache(leaseManager, TTL, TTL_TIME_UNIT);

        List<KinesisClientLease> actualLeases;
        actualLeases = leasesCache.getLeases(DeterministicShuffleLeaderElection.LEAES_CACHE_KEY);
        assertEquals("Expected and Actual leases are not equal", expectedLeases, actualLeases);

        // verify cached value used and lease manger not invoked. 1 list leases
        // call from the previous getLeases invocation
        actualLeases = leasesCache.getLeases(DeterministicShuffleLeaderElection.LEAES_CACHE_KEY);
        assertEquals("Expected and Actual leases are not equal", expectedLeases, actualLeases);
        verify(leaseManager, times(1)).listLeases();
    }

    @Test
    public void testCachedvalueExpiresAfterTTL() throws Exception {
        List<KinesisClientLease> expectedLeases = getLeases(5, false /* duplicateLeaseOwner */,
                true /* activeLeases */);
        when(leaseManager.listLeases()).thenReturn(expectedLeases);
        ILeasesCache<KinesisClientLease> leasesCache = new LeasesCache(leaseManager, SHORTER_TTL, TTL_TIME_UNIT);

        List<KinesisClientLease> actualLeases;
        actualLeases = leasesCache.getLeases(DeterministicShuffleLeaderElection.LEAES_CACHE_KEY);
        assertEquals("Expected and Actual leases are not equal", expectedLeases, actualLeases);

        // Sleep so that the next cache access is after the cache ttl passes and
        // the cached entry is expired
        Thread.sleep(SHORTER_TTL);
        actualLeases = leasesCache.getLeases(DeterministicShuffleLeaderElection.LEAES_CACHE_KEY);
        assertEquals("Expected and Actual leases are not equal", expectedLeases, actualLeases);
        verify(leaseManager, times(2)).listLeases();

    }

}
