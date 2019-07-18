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

import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.ILeaderElectionStrategyFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.ILeasesCache;
import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeaderElectionStrategy;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

/**
 * Factory implementation to vend singleton instance of leaderElectionStrategy
 */
class LeaderElectionStrategyFactory implements ILeaderElectionStrategyFactory<KinesisClientLease> {
    private ILeaseManager<KinesisClientLease> leaseManager;
    private KinesisClientLibConfiguration config;
    int vendedInstanceCount;
    private static final int LEADER_ELECTION_LEASES_CACHE_TTL_MINS = 5;

    private volatile LeaderElectionStrategy<KinesisClientLease> leaderElectionStrategy;

    LeaderElectionStrategyFactory(KinesisClientLibConfiguration config,
                                  ILeaseManager<KinesisClientLease> leaseManager) {
        this.config = config;
        this.leaseManager = leaseManager;
    }

    /*
     * Vends a singleton DeterministicShuffleLeaderElection strategy shared across different workers
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.
     * interfaces.ILeaderElectionStrategyFactory#getLeaderElectionStrategy()
     */
    @Override
    public LeaderElectionStrategy<KinesisClientLease> getLeaderElectionStrategy() {
        if (leaderElectionStrategy == null) {
            synchronized (this) {
                if (leaderElectionStrategy == null) {
                    ILeasesCache<KinesisClientLease> leasesCache = new LeasesCache(leaseManager,
                                                                                   LEADER_ELECTION_LEASES_CACHE_TTL_MINS /* ttl */, TimeUnit.MINUTES);
                    leaderElectionStrategy = new DeterministicShuffleLeaderElection(config, leasesCache);
                    vendedInstanceCount++;
                }
            }
        }
        return leaderElectionStrategy;
    }
}
