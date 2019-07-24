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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DeterministicShuffleLeaderElectionTest extends PeriodicShardSyncTestBase {
    private static final Log LOG = LogFactory.getLog(DeterministicShuffleLeaderElectionTest.class);

    private KinesisClientLibConfiguration config;
    private DeterministicShuffleLeaderElection leaderElection;

    @Mock
    private ILeaseManager<KinesisClientLease> leaseManager;
    private Clock clock;

    @Before
    public void setup() {
        leaderElection = spy(new DeterministicShuffleLeaderElection(config, leaseManager));
        config = spy(new KinesisClientLibConfiguration("Test", null, null, null));
        clock = Clock.systemUTC();
    }

    @Test
    public void testLeaderElectionWithNullLeases() {
        Set<String> leaders = leaderElection.electLeaders(null);
        assertTrue("Leaders should not be null", leaders != null);
        assertTrue("Leaders should be empty", leaders.isEmpty());
    }

    @Test
    public void testLeaderElectionWithEmptyLeases() {
        Set<String> leaders = leaderElection.electLeaders(new ArrayList<KinesisClientLease>());
        assertTrue("Leaders should not be null", leaders != null);
        assertTrue("Leaders should be empty", leaders.isEmpty());
    }

    @Test
    public void testElectedLeadersAsPerExpectedShufflingOrder() {
        List<KinesisClientLease> leases = getLeases(5, false /* duplicateLeaseOwner */, true /* activeLeases */);
        Set<String> actualLeaders = leaderElection.electLeaders(leases);
        Collections.shuffle(leases,
                new Random(DeterministicShuffleLeaderElection.DETERMINISTIC_SHUFFLE_SEED));
        Set<String> expectedLeaders = leases.stream().map(lease -> lease.getLeaseOwner()).collect(Collectors.toSet());

        assertEquals("Expected and actual leaders are not same", expectedLeaders, actualLeaders);
    }

    @Test
    public void testElectedLeadersAsPerExpectedShufflingOrderWhenUniqueWorkersLessThanMaxLeaders() {
        List<KinesisClientLease> leases = getLeases(3, false /* duplicateLeaseOwner */, true /* activeLeases */);
        Set<String> actualLeaders = leaderElection.electLeaders(leases);

        Collections.shuffle(leases, new Random(DeterministicShuffleLeaderElection.DETERMINISTIC_SHUFFLE_SEED));
        Set<String> expectedLeaders = leases.stream().limit(3).map(lease -> lease.getLeaseOwner())
                .collect(Collectors.toSet());

        assertEquals("Expected and actual leaders are not same", expectedLeaders, actualLeaders);
    }

    @Test
    public void testSingleLeaderElectedForLeasesWithSameOwner() {
        List<KinesisClientLease> leases = getLeases(3, true /* duplicateLeaseOwner */, true /* activeLeases */);
        Set<String> actualLeaders = leaderElection.electLeaders(leases);

        Collections.shuffle(leases, new Random(DeterministicShuffleLeaderElection.DETERMINISTIC_SHUFFLE_SEED));
        Set<String> expectedLeaders = leases.stream().map(lease -> lease.getLeaseOwner())
                .collect(Collectors.toSet());

        assertEquals("Expected and actual leaders are not same", expectedLeaders, actualLeaders);
    }

    @Test
    public void testNotLeader() {
        when(leaderElection.electLeaders(any())).thenReturn(new HashSet<>(Arrays.asList("dummyWorker")));
        assertTrue(!leaderElection.isLeader("WORKER"));
    }

    @Test
    public void testIsLeader() {
        when(leaderElection.electLeaders(any())).thenReturn(new HashSet<>(Arrays.asList("WORKER")));
        assertTrue(leaderElection.isLeader("WORKER"));
    }

    @Test
    public void testLeasesUpdatedAfterTTL() throws Exception {
        long lastfetchedTime = clock.instant().toEpochMilli() - 120000;
        when(leaderElection.getLeasesLastFetchTime()).thenReturn(lastfetchedTime);
        leaderElection.updateLeases();
        verify(leaseManager, times(1)).listLeases();
    }

    @Test
    public void testLeasesNotUpdatedBeforeTTL() throws Exception {
        long lastfetchedTime = clock.instant().toEpochMilli() - 50000;
        when(leaderElection.getLeasesLastFetchTime()).thenReturn(lastfetchedTime);
        leaderElection.updateLeases();
        verify(leaseManager, times(0)).listLeases();
    }

}
