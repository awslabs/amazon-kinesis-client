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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeaderElectionStrategy;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.LeaseManager;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.util.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This implementation of the {@code LeaderElectionStrategy} elects the leaders
 * by firstly getting all the leases. The leases are then shuffled using a
 * predetermined constant seed so that the lease ordering is preserved across
 * workers. This strategy helps in randomly shuffling the leases in order to
 * reduce the probability of choosing the leader workers co-located on the same
 * host in case the lease owner field in the lease doesn't distinguish between workers
 * on the same host e.g using ip address of the host for the lease owner for all workers
 * on a host. The number of leaders to elect is configurable here
 * {@code config.getPeriodicShardSyncMaxWorkers()}. Once the leaders are
 * elected, all the registered listeners are informed about the new election
 */
class DeterministicShuffleLeaderElection
        implements LeaderElectionStrategy<KinesisClientLease>, Comparator<KinesisClientLease> {

    static final String LEAES_CACHE_KEY = "all_leases";
    // Fixed seed so that the shuffle order is preserved across workers
    static final int DETERMINISTIC_SHUFFLE_SEED = 1947;
    private static final int PERIODIC_SHARD_SYNC_MAX_WORKERS = 5;
    private long leasesLastFetchTime;
    private List<KinesisClientLease> leases;
    private Clock clock;

    private static final int LEADER_ELECTION_LEASES_CACHE_TTL_MILLIS = 60000;

    private static final Log LOG = LogFactory.getLog(DeterministicShuffleLeaderElection.class);
    private KinesisClientLibConfiguration config;
    private final ILeaseManager<KinesisClientLease> leaseManager;

    DeterministicShuffleLeaderElection(KinesisClientLibConfiguration config,
        ILeaseManager<KinesisClientLease> leaseManager) {
        this.config = config;
        this.leaseManager = leaseManager;
        clock = Clock.systemUTC();
        leasesLastFetchTime = clock.instant().toEpochMilli();
        updateLeases();
    }

    @Override
    public void run() {
        updateLeases();
    }

    synchronized long getLeasesLastFetchTime() {
        return leasesLastFetchTime;
    }

    synchronized void setLeasesLastFetchTime(long instant) {
        leasesLastFetchTime = instant;
    }

    /*
     * shuffles the leases deterministically and elects configured no of workers
     * as leaders
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.
     * interfaces.LeaderElectionStrategy#electLeaders(java.util.List)
     */
    @Override
    public Set<String> electLeaders(List<KinesisClientLease> leases) {
        if (CollectionUtils.isNullOrEmpty(leases)) {
            return new HashSet<String>();
        }
        leases = leases.stream().filter(lease -> lease.getLeaseOwner() != null).sorted(this)
                       .collect(Collectors.toList());
        Collections.shuffle(leases, new Random(DETERMINISTIC_SHUFFLE_SEED));
        Set<String> leaders = leases.stream()
                                    .limit(Math.min(PERIODIC_SHARD_SYNC_MAX_WORKERS, getUniqueWorkerIds(leases).size()))
                                    .map(lease -> lease.getLeaseOwner()).collect(Collectors.toSet());
        return leaders;
    }

    @Override
    public Boolean isLeader(String workerId) {
        Set<String> leaders = electLeaders(leases);
        return leaders.contains(workerId);
    }

    private Set<String> getUniqueWorkerIds(List<KinesisClientLease> leases) {
        Set<String> workers = new HashSet<String>();
        for (KinesisClientLease lease : leases) {
            if (StringUtils.isNotBlank(lease.getLeaseOwner())) {
                workers.add(lease.getLeaseOwner());
            }
        }
        return workers;
    }

    void updateLeases() {
        try {
            if (clock.instant().toEpochMilli() - getLeasesLastFetchTime() > LEADER_ELECTION_LEASES_CACHE_TTL_MILLIS) {
                leases = leaseManager.listLeases();
                setLeasesLastFetchTime(clock.instant().toEpochMilli());
            }
        } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
            LOG.error("Exception occurred while trying to fetch all leases for leader election", e);
        }
    }

    @Override
    public int compare(KinesisClientLease lease1, KinesisClientLease lease2) {
        return lease1.getLeaseOwner().compareTo(lease2.getLeaseOwner());
    }

}
