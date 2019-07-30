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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.util.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An implementation of the {@code LeaderDecider} to elect leader(s) based on workerId.
 * Leases are shuffled using a predetermined constant seed so that lease ordering is
 * preserved across workers.
 * This reduces the probability of choosing the leader workers co-located on the same
 * host in case workerId starts with a common string (e.g. IP Address).
 * Hence if a host has 3 workers, IPADDRESS_Worker1, IPADDRESS_Worker2, and IPADDRESS_Worker3,
 * we don't end up choosing all 3 for shard sync as a result of natural ordering of Strings.
 * This ensures redundancy for shard-sync during host failures.
 */
public class DeterministicShuffleShardSyncLeaderDecider implements LeaderDecider {
    // Fixed seed so that the shuffle order is preserved across workers
    static final int DETERMINISTIC_SHUFFLE_SEED = 1947;
    static final int PERIODIC_SHARD_SYNC_MAX_WORKERS_DEFAULT = 1; //Default for KCL.
    private static final Log LOG = LogFactory.getLog(DeterministicShuffleShardSyncLeaderDecider.class);
    private static final int LEADER_DECIDER_THREAD_COUNT = 1;
    private static final long ELECTION_INITIAL_DELAY_MILLIS = 60 * 1000;
    private static final long ELECTION_SCHEDULING_INTERVAL = 5 * 60 * 1000;
    private static final int AWAIT_TERMINATION_MILLIS = 5000;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final KinesisClientLibConfiguration config;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private final int numPeriodicShardSyncWorkers;
    private final ScheduledExecutorService leaderElectionThreadPool;

    private volatile Set<String> leaders;

    /**
     * Create an instance using the default periodic shard sync worker count (1).
     * This constructor is package-private to ensure external users use the public
     * constructor and are thereby aware of the number of periodic shard sync workers
     * they want in their application.
     *
     * @param config       KinesisClientLibConfiguration instance
     * @param leaseManager LeaseManager instance.
     */
    DeterministicShuffleShardSyncLeaderDecider(KinesisClientLibConfiguration config,
            ILeaseManager<KinesisClientLease> leaseManager) {
        this(config, leaseManager, PERIODIC_SHARD_SYNC_MAX_WORKERS_DEFAULT);
    }

    /**
     * Create an instance overriding the default periodic shard sync worker count.
     * This is public for use outside KCL.
     *
     * @param config                      KinesisClientLibConfiguration instance
     * @param leaseManager                LeaseManager instance.
     * @param numPeriodicShardSyncWorkers Max number of periodic shard sync workers.
     */
    public DeterministicShuffleShardSyncLeaderDecider(KinesisClientLibConfiguration config,
            ILeaseManager<KinesisClientLease> leaseManager, int numPeriodicShardSyncWorkers) {
        this(config, leaseManager, Executors.newScheduledThreadPool(LEADER_DECIDER_THREAD_COUNT),
                numPeriodicShardSyncWorkers);
    }

    DeterministicShuffleShardSyncLeaderDecider(KinesisClientLibConfiguration config,
            ILeaseManager<KinesisClientLease> leaseManager, ScheduledExecutorService leaderElectionThreadPool,
            int numPeriodicShardSyncWorkers) {
        this.config = config;
        this.leaseManager = leaseManager;
        this.leaderElectionThreadPool = leaderElectionThreadPool;
        this.numPeriodicShardSyncWorkers = numPeriodicShardSyncWorkers;
    }

    /*
     * Shuffles the leases deterministically and elects numPeriodicShardSyncWorkers number of workers
     * as leaders (workers that will perform shard sync).
     */
    private void electLeaders() {
        try {
            LOG.debug("Started leader election: " + System.currentTimeMillis());
            List<KinesisClientLease> leases = leaseManager.listLeases();
            List<String> uniqueHosts = leases.stream().map(KinesisClientLease::getLeaseOwner)
                    .filter(owner -> owner != null).distinct().sorted().collect(Collectors.toList());

            Collections.shuffle(uniqueHosts, new Random(DETERMINISTIC_SHUFFLE_SEED));
            int numShardSyncWorkers = Math.min(uniqueHosts.size(), numPeriodicShardSyncWorkers);
            // In case value is currently being read, we wait for reading to complete before updating the variable.
            // This is to prevent any ConcurrentModificationException exceptions.
            readWriteLock.writeLock().lock();
            leaders = new HashSet<>(uniqueHosts.subList(0, numShardSyncWorkers));
            LOG.debug("Elected leaders: " + String.join(", ", leaders));
            LOG.debug("Completed leader election: " + System.currentTimeMillis());
        } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
            LOG.error("Exception occurred while trying to fetch all leases for leader election", e);
        } catch (Throwable t) {
            LOG.error("Unknown exception during leader election.", t);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    // Utility methods to ensure we acquire a readLock in case an election has completed
    // and some thread is trying to update leaders variable.
    private boolean leadersNullOrEmpty() {
        try {
            readWriteLock.readLock().lock();
            return CollectionUtils.isNullOrEmpty(leaders);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private boolean workerIdInLeaders(String workerId) {
        try {
            readWriteLock.readLock().lock();
            // If leaders is still null or empty fall back to this host being a "leader". This ensures that a brief
            // unavailability or throttling on the leases table does not cause a stall.
            return CollectionUtils.isNullOrEmpty(leaders) || (!CollectionUtils.isNullOrEmpty(leaders) && leaders
                    .contains(workerId));
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override public synchronized Boolean isLeader(String workerId) {
        // if no leaders yet, synchronously get leaders. This will happen at first Shard Sync.
        if (leadersNullOrEmpty()) {
            electLeaders();
            // start a scheduled executor that will periodically update leaders.
            // The first run will be after a minute.
            // We don't need jitter since it is scheduled with a fixed delay and time taken to scan leases
            // will be different at different times and on different hosts/workers.
            leaderElectionThreadPool.scheduleWithFixedDelay(this::electLeaders, ELECTION_INITIAL_DELAY_MILLIS,
                    ELECTION_SCHEDULING_INTERVAL, TimeUnit.MILLISECONDS);
        }
        return workerIdInLeaders(workerId);
    }

    @Override public synchronized void shutdown() {
        try {
            leaderElectionThreadPool.shutdown();
            if (leaderElectionThreadPool.awaitTermination(AWAIT_TERMINATION_MILLIS, TimeUnit.MILLISECONDS)) {
                LOG.info("Successfully stopped leader election on the worker");
            } else {
                leaderElectionThreadPool.shutdownNow();
                LOG.info(String.format("Stopped leader election thread after awaiting termination for %d milliseconds",
                        AWAIT_TERMINATION_MILLIS));
            }

        } catch (InterruptedException e) {
            LOG.debug("Encountered InterruptedException while awaiting leader election threadPool termination");
        }
    }
}
