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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeaderElectionStrategy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ShardSyncStrategyDecider implementation which looks at the active shard count
 * to decide the strategy for shard sync
 */
class ActiveShardCountBasedShardSyncStrategyDecider implements ShardSyncStrategyDecider {

    private static final Object LOCK = new Object();
    private static final Log LOG = LogFactory.getLog(ActiveShardCountBasedShardSyncStrategyDecider.class);
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private final LeaderElectionStrategy leaderElectionStrategy;
    private final IKinesisProxy kinesisProxy;
    private final KinesisClientLibConfiguration config;
    private final String workerId;
    private final ShardSyncer shardSyncer;
    private final IMetricsFactory metricsFactory;
    private final ExecutorService executorService;
    private ScheduledLeaseLeaderPoller leaderPoller;
    private volatile List<KinesisClientLease> leases;

    static final int MIN_ACTIVE_SHARDS_FOR_PERIODIC_SYNC = 3000;
    private static final int LEADER_POLLER_THREAD_COUNT = 1;
    private static final int PERIODIC_SHARD_SYNC_THREAD_COUNT = 2;

    private ActiveShardCountBasedShardSyncStrategyDecider(Builder builder) {
        this.leaseManager = builder.leaseManager;
        this.leaderElectionStrategy = builder.leaderElectionStrategy;
        this.kinesisProxy = builder.kinesisProxy;
        this.config = builder.config;
        this.workerId = builder.workerId;
        this.shardSyncer = builder.shardSyncer;
        this.metricsFactory = builder.metricsFactory;
        this.executorService = builder.executorService;
    }

    static Builder getBuilder() {
        return new Builder();
    }

    static class Builder {
        private ILeaseManager<KinesisClientLease> leaseManager;
        private LeaderElectionStrategy leaderElectionStrategy;
        private IKinesisProxy kinesisProxy;
        private KinesisClientLibConfiguration config;
        private String workerId;
        private ShardSyncer shardSyncer;
        private IMetricsFactory metricsFactory;
        private ExecutorService executorService;

        public Builder withLeaseManager(ILeaseManager<KinesisClientLease> leaseManager) {
            this.leaseManager = leaseManager;
            return this;
        }

        public Builder withLeaderElectionStrategy(LeaderElectionStrategy leaderElectionStrategy) {
            this.leaderElectionStrategy = leaderElectionStrategy;
            return this;
        }

        public Builder withkinesisProxy(IKinesisProxy kinesisProxy) {
            this.kinesisProxy = kinesisProxy;
            return this;
        }

        public Builder withConfig(KinesisClientLibConfiguration config) {
            this.config = config;
            return this;
        }

        public Builder withWorkerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        public Builder withShardSyncer(ShardSyncer shardSyncer) {
            this.shardSyncer = shardSyncer;
            return this;
        }

        public Builder withMetricsFactory(IMetricsFactory metricsFactory) {
            this.metricsFactory = metricsFactory;
            return this;
        }

        public Builder withExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public ActiveShardCountBasedShardSyncStrategyDecider build() {
            return new ActiveShardCountBasedShardSyncStrategyDecider(this);
        }

    }

    /*
     * Gets the active shards count and decides the shard sync strategy based on the threshold
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.
     * ShardSyncStrategyDecider#getShardSyncStrategy()
     */
    @Override
    public ShardSyncStrategy getShardSyncStrategy()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (!config.getEnablePeriodicShardSync()) {
            return createShardEndShardSyncStrategy();
        }
        ShardSyncStrategy shardSyncStrategy = null;
        int activeShardleasesCount = getActiveShardleasesCount();
        if (activeShardleasesCount != 0) {
            shardSyncStrategy = activeShardleasesCount >= MIN_ACTIVE_SHARDS_FOR_PERIODIC_SYNC ? createPeriodicShardSyncStrategy()
                                        : createShardEndShardSyncStrategy();
        }
        LOG.debug(String.format("ActiveShardLeasesCount: %d, ShardSyncStrategy: %s", activeShardleasesCount, shardSyncStrategy.getName()));
        return shardSyncStrategy;
    }

    private ScheduledLeaseLeaderPoller getLeaderPoller() {
        if (leaderPoller == null) {
            leaderPoller =
                new ScheduledLeaseLeaderPoller(leaderElectionStrategy,
                    config,
                    new ScheduledThreadPoolExecutor(LEADER_POLLER_THREAD_COUNT));
        }
        return leaderPoller;
    }

    private PeriodicShardSyncScheduler getShardSyncScheduler() {
        return new PeriodicShardSyncScheduler(kinesisProxy,
            leaseManager,
            config.getInitialPositionInStreamExtended(),
            config,
            new ScheduledThreadPoolExecutor(PERIODIC_SHARD_SYNC_THREAD_COUNT),
            shardSyncer,
            getLeaderPoller(),
            workerId);
    }

    private PeriodicShardSyncStrategy createPeriodicShardSyncStrategy() {
        return new PeriodicShardSyncStrategy(PeriodicShardSyncManager.getBuilder()
                                                 .withWorkerId(workerId)
                                                 .withLeaderPoller(getLeaderPoller())
                                                 .withPeriodicShardSyncScheduler(getShardSyncScheduler())
                                                 .build());
    }


    private ShardEndShardSyncStrategy createShardEndShardSyncStrategy() {
        return new ShardEndShardSyncStrategy(new ShardSyncTaskManager(kinesisProxy,
            leaseManager,
            config.getInitialPositionInStreamExtended(),
            config.shouldCleanupLeasesUponShardCompletion(),
            config.shouldIgnoreUnexpectedChildShards(),
            config.getShardSyncIntervalMillis(),
            metricsFactory,
            executorService,
            shardSyncer));
    }

    /**
     * Gets the leases and filters out the SHARD_END leases to get the count of
     * active leases
     *
     * @return Active leases count
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     */
    private int getActiveShardleasesCount()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        // double checked locking to ensure only a single thread populates the
        // leases as all threads will get roughly the same view of the leases at
        // startup. This keeps a check on IOPS utilisation of Leases table. Also
        // once populated, the strategy will never move back from Periodic to
        // Shard sync as the number of shards never reduce. For the other case
        // of increasing shards, its a fair assumption that there would be at
        // least one deployment and the workers would initialize again before
        // that happens
        if(leases == null) {
            synchronized (LOCK) {
                if (leases == null) {
                    getLeases();
                }
            }
        }
        return leases == null ? 0
                       : (int) leases.stream().filter(lease -> lease.getCheckpoint() != null && lease.getCheckpoint() != ExtendedSequenceNumber.SHARD_END)
                                     .count();
    }

    private void getLeases() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        leases = leaseManager.listLeases();
    }

}
