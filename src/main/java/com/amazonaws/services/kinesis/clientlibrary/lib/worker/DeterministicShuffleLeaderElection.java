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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.ILeasesCache;
import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeaderElectionStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeadersElectionListener;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
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

    private static final Log LOG = LogFactory.getLog(DeterministicShuffleLeaderElection.class);
    private KinesisClientLibConfiguration config;
    private Set<LeadersElectionListener> leadersElectionListeners;
    private ILeasesCache<KinesisClientLease> leasesCache;

    DeterministicShuffleLeaderElection(KinesisClientLibConfiguration config,
                                       ILeasesCache<KinesisClientLease> leasesCache) {
        this.config = config;
        this.leasesCache = leasesCache;

        this.leadersElectionListeners = new HashSet<LeadersElectionListener>();
    }

    @Override
    public void run() {
        List<KinesisClientLease> leases = getLeases();
        if (leases != null) {
            electLeaders(leases);
        }
    }

    @Override
    public void registerLeadersElectionListener(LeadersElectionListener listener) {
        leadersElectionListeners.add(listener);
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
        leadersElectionListeners.forEach(listener -> listener.leadersElected(leaders));
        return leaders;
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

    private List<KinesisClientLease> getLeases() {
        List<KinesisClientLease> leases = null;
        try {
            //The leases are cached to keep a check on the IOPS consumption on the leases table
            // caching is feasible as the worker composition should remain fairly constant 
            leases = leasesCache.getLeases(LEAES_CACHE_KEY);
        } catch (LeasesCacheException e) {
            LOG.error("Exception occurred while trying to fetch all leases for leader election", e.getCause());
        }
        return leases;
    }

    @Override
    public int compare(KinesisClientLease lease1, KinesisClientLease lease2) {
        return lease1.getLeaseOwner().compareTo(lease2.getLeaseOwner());
    }

}
