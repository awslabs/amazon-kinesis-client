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

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.DeterministicShuffleShardSyncLeaderDecider.DETERMINISTIC_SHUFFLE_SEED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DeterministicShuffleShardSyncLeaderDeciderTest {
    private static final String LEASE_KEY = "lease_key";
    private static final String LEASE_OWNER = "lease_owner";
    private static final String WORKER_ID = "worker-id";

    private DeterministicShuffleShardSyncLeaderDecider leaderDecider;

    @Mock
    private ILeaseManager<KinesisClientLease> leaseManager;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    private int numShardSyncWorkers;

    @Before
    public void setup() {
        numShardSyncWorkers = 1;
        leaderDecider = new DeterministicShuffleShardSyncLeaderDecider(leaseManager, scheduledExecutorService, numShardSyncWorkers);
    }

    @Test
    public void testLeaderElectionWithNullLeases() {
        boolean isLeader = leaderDecider.isLeader(WORKER_ID);
        assertTrue("IsLeader should return true if leaders is null", isLeader);
    }

    @Test
    public void testLeaderElectionWithEmptyLeases() throws Exception{
        when(leaseManager.listLeases()).thenReturn(new ArrayList<>());
        boolean isLeader = leaderDecider.isLeader(WORKER_ID);
        assertTrue("IsLeader should return true if no leases are returned", isLeader);
    }

    @Test
    public void testElectedLeadersAsPerExpectedShufflingOrder() throws Exception {
        List<KinesisClientLease> leases = getLeases(5, false /* duplicateLeaseOwner */, true /* activeLeases */);
        when(leaseManager.listLeases()).thenReturn(leases);
        Set<String> expectedLeaders = getExpectedLeaders(leases);
        for (String leader : expectedLeaders) {
            assertTrue(leaderDecider.isLeader(leader));
        }
        for (KinesisClientLease lease : leases) {
            if (!expectedLeaders.contains(lease.getLeaseOwner())) {
                assertFalse(leaderDecider.isLeader(lease.getLeaseOwner()));
            }
        }
    }

    @Test
    public void testElectedLeadersAsPerExpectedShufflingOrderWhenUniqueWorkersLessThanMaxLeaders() {
        this.numShardSyncWorkers = 5; // More than number of unique lease owners
        leaderDecider = new DeterministicShuffleShardSyncLeaderDecider(leaseManager, scheduledExecutorService, numShardSyncWorkers);
        List<KinesisClientLease> leases = getLeases(3, false /* duplicateLeaseOwner */, true /* activeLeases */);
        Set<String> expectedLeaders = getExpectedLeaders(leases);
        // All lease owners should be present in expected leaders set, and they should all be leaders.
        for (KinesisClientLease lease : leases) {
            assertTrue(leaderDecider.isLeader(lease.getLeaseOwner()));
            assertTrue(expectedLeaders.contains(lease.getLeaseOwner()));
        }
    }

    private List<KinesisClientLease> getLeases(int count, boolean duplicateLeaseOwner, boolean activeLeases) {
        List<KinesisClientLease> leases = new ArrayList<>();
        for (int i=0;i<count;i++) {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setLeaseKey(LEASE_KEY + i);
            lease.setCheckpoint(activeLeases ? ExtendedSequenceNumber.LATEST : ExtendedSequenceNumber.SHARD_END);
            lease.setLeaseCounter(new Random().nextLong());
            lease.setLeaseOwner(LEASE_OWNER + (duplicateLeaseOwner ? "" : i));
            leases.add(lease);
        }
        return leases;
    }

    private Set<String> getExpectedLeaders(List<KinesisClientLease> leases) {
        List<String> uniqueHosts = leases.stream().filter(lease -> lease.getLeaseOwner() != null)
                .map(KinesisClientLease::getLeaseOwner).distinct().sorted().collect(Collectors.toList());

        Collections.shuffle(uniqueHosts, new Random(DETERMINISTIC_SHUFFLE_SEED));
        int numWorkers = Math.min(uniqueHosts.size(), this.numShardSyncWorkers);
        return new HashSet<>(uniqueHosts.subList(0, numWorkers));
    }
}
