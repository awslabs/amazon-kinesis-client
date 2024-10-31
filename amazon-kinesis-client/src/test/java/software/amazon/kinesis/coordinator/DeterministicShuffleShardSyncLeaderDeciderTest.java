/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.coordinator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.coordinator.DeterministicShuffleShardSyncLeaderDecider.DETERMINISTIC_SHUFFLE_SEED;

@RunWith(MockitoJUnitRunner.class)
public class DeterministicShuffleShardSyncLeaderDeciderTest {
    private static final String LEASE_KEY = "lease_key";
    private static final String LEASE_OWNER = "lease_owner";
    private static final String WORKER_ID = "worker-id";
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    private DeterministicShuffleShardSyncLeaderDecider leaderDecider;

    @Mock
    private LeaseRefresher leaseRefresher;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    @Mock
    private ReadWriteLock readWriteLock;

    private int numShardSyncWorkers;

    @Before
    public void setup() {
        numShardSyncWorkers = 1;
        leaderDecider = new DeterministicShuffleShardSyncLeaderDecider(
                leaseRefresher, scheduledExecutorService, numShardSyncWorkers, readWriteLock, NULL_METRICS_FACTORY);

        when(readWriteLock.readLock()).thenReturn(mock(ReentrantReadWriteLock.ReadLock.class));
        when(readWriteLock.writeLock()).thenReturn(mock(ReentrantReadWriteLock.WriteLock.class));
    }

    @Test
    public void testLeaderElectionWithNullLeases() {
        boolean isLeader = leaderDecider.isLeader(WORKER_ID);
        assertTrue("IsLeader should return true if leaders is null", isLeader);
    }

    @Test
    public void testLeaderElectionWithEmptyLeases() throws Exception {
        when(leaseRefresher.listLeases()).thenReturn(new ArrayList<>());
        boolean isLeader = leaderDecider.isLeader(WORKER_ID);
        assertTrue("IsLeader should return true if no leases are returned", isLeader);
    }

    @Test
    public void testLeaderElectionDoesNotUseLocksOnListLeasesException() throws Exception {
        when(leaseRefresher.listLeases()).thenThrow(new DependencyException("error", new Throwable()));
        leaderDecider.isLeader(WORKER_ID);
        verify(leaseRefresher, times(1)).listLeases();
        verify(readWriteLock.writeLock(), times(0)).lock();
        verify(readWriteLock.writeLock(), times(0)).unlock();
    }

    @Test
    public void testleaderElectionWithEmptyOwnerLeases() throws Exception {
        List<Lease> leases = getLeases(5, true, true, true);
        when(leaseRefresher.listLeases()).thenReturn(leases);
        boolean isLeader = leaderDecider.isLeader(WORKER_ID);
        assertTrue("IsLeader should return true if leases have no owner", isLeader);
    }

    @Test
    public void testElectedLeadersAsPerExpectedShufflingOrder() throws Exception {
        List<Lease> leases =
                getLeases(5, false /*emptyLeaseOwner */, false /* duplicateLeaseOwner */, true /* activeLeases */);
        when(leaseRefresher.listLeases()).thenReturn(leases);
        Set<String> expectedLeaders = getExpectedLeaders(leases);
        for (String leader : expectedLeaders) {
            assertTrue(leaderDecider.isLeader(leader));
        }
        for (Lease lease : leases) {
            if (!expectedLeaders.contains(lease.leaseOwner())) {
                assertFalse(leaderDecider.isLeader(lease.leaseOwner()));
            }
        }
    }

    @Test
    public void testElectedLeadersAsPerExpectedShufflingOrderWhenUniqueWorkersLessThanMaxLeaders() {
        this.numShardSyncWorkers = 5; // More than number of unique lease owners
        leaderDecider = new DeterministicShuffleShardSyncLeaderDecider(
                leaseRefresher, scheduledExecutorService, numShardSyncWorkers, readWriteLock, NULL_METRICS_FACTORY);
        List<Lease> leases =
                getLeases(3, false /*emptyLeaseOwner */, false /* duplicateLeaseOwner */, true /* activeLeases */);
        Set<String> expectedLeaders = getExpectedLeaders(leases);
        // All lease owners should be present in expected leaders set, and they should all be leaders.
        for (Lease lease : leases) {
            assertTrue(leaderDecider.isLeader(lease.leaseOwner()));
            assertTrue(expectedLeaders.contains(lease.leaseOwner()));
        }
    }

    private List<Lease> getLeases(
            int count, boolean emptyLeaseOwner, boolean duplicateLeaseOwner, boolean activeLeases) {
        List<Lease> leases = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Lease lease = new Lease();
            lease.leaseKey(LEASE_KEY + i);
            lease.checkpoint(activeLeases ? ExtendedSequenceNumber.LATEST : ExtendedSequenceNumber.SHARD_END);
            lease.leaseCounter(new Random().nextLong());
            if (!emptyLeaseOwner) {
                lease.leaseOwner(LEASE_OWNER + (duplicateLeaseOwner ? "" : i));
            }
            leases.add(lease);
        }
        return leases;
    }

    private Set<String> getExpectedLeaders(List<Lease> leases) {
        List<String> uniqueHosts = leases.stream()
                .filter(lease -> lease.leaseOwner() != null)
                .map(Lease::leaseOwner)
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        Collections.shuffle(uniqueHosts, new Random(DETERMINISTIC_SHUFFLE_SEED));
        int numWorkers = Math.min(uniqueHosts.size(), this.numShardSyncWorkers);
        return new HashSet<>(uniqueHosts.subList(0, numWorkers));
    }
}
