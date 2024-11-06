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

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

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
@Slf4j
class DeterministicShuffleShardSyncLeaderDecider implements LeaderDecider {
    // Fixed seed so that the shuffle order is preserved across workers
    static final int DETERMINISTIC_SHUFFLE_SEED = 1947;

    private static final long ELECTION_INITIAL_DELAY_MILLIS = 60 * 1000;
    private static final long ELECTION_SCHEDULING_INTERVAL_MILLIS = 5 * 60 * 1000;
    private static final int AWAIT_TERMINATION_MILLIS = 5000;

    private final ReadWriteLock readWriteLock;

    private final LeaseRefresher leaseRefresher;
    private final int numPeriodicShardSyncWorkers;
    private final ScheduledExecutorService leaderElectionThreadPool;
    private final MetricsFactory metricsFactory;

    private volatile Set<String> leaders;

    /**
     * @param leaseRefresher              LeaseManager instance used to fetch leases.
     * @param leaderElectionThreadPool    Thread-pool to be used for leaderElection.
     * @param numPeriodicShardSyncWorkers Number of leaders that will be elected to perform periodic shard syncs.
     */
    DeterministicShuffleShardSyncLeaderDecider(
            LeaseRefresher leaseRefresher,
            ScheduledExecutorService leaderElectionThreadPool,
            int numPeriodicShardSyncWorkers,
            MetricsFactory metricsFactory) {
        this(
                leaseRefresher,
                leaderElectionThreadPool,
                numPeriodicShardSyncWorkers,
                new ReentrantReadWriteLock(),
                metricsFactory);
    }

    /**
     * @param leaseRefresher              LeaseManager instance used to fetch leases.
     * @param leaderElectionThreadPool    Thread-pool to be used for leaderElection.
     * @param numPeriodicShardSyncWorkers Number of leaders that will be elected to perform periodic shard syncs.
     * @param readWriteLock               Mechanism to lock for reading and writing of critical components
     */
    DeterministicShuffleShardSyncLeaderDecider(
            LeaseRefresher leaseRefresher,
            ScheduledExecutorService leaderElectionThreadPool,
            int numPeriodicShardSyncWorkers,
            ReadWriteLock readWriteLock,
            MetricsFactory metricsFactory) {
        this.leaseRefresher = leaseRefresher;
        this.leaderElectionThreadPool = leaderElectionThreadPool;
        this.numPeriodicShardSyncWorkers = numPeriodicShardSyncWorkers;
        this.readWriteLock = readWriteLock;
        this.metricsFactory = metricsFactory;
    }

    /*
     * Shuffles the leases deterministically and elects numPeriodicShardSyncWorkers number of workers
     * as leaders (workers that will perform shard sync).
     */
    private void electLeaders() {
        boolean acquiredLock = false;
        try {
            log.debug("Started leader election at: " + Instant.now());
            List<Lease> leases = leaseRefresher.listLeases();
            List<String> uniqueHosts = leases.stream()
                    .map(Lease::leaseOwner)
                    .filter(Objects::nonNull)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());

            Collections.shuffle(uniqueHosts, new Random(DETERMINISTIC_SHUFFLE_SEED));
            int numShardSyncWorkers = Math.min(uniqueHosts.size(), numPeriodicShardSyncWorkers);
            // In case value is currently being read, we wait for reading to complete before updating the variable.
            // This is to prevent any ConcurrentModificationException exceptions.
            readWriteLock.writeLock().lock();
            acquiredLock = true;
            leaders = new HashSet<>(uniqueHosts.subList(0, numShardSyncWorkers));
            log.info("Elected leaders: " + String.join(", ", leaders));
            log.debug("Completed leader election at: " + Instant.now());
        } catch (DependencyException | InvalidStateException | ProvisionedThroughputException e) {
            log.error("Exception occurred while trying to fetch all leases for leader election", e);
        } catch (Throwable t) {
            log.error("Unknown exception during leader election.", t);
        } finally {
            if (acquiredLock) {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    private boolean isWorkerLeaderForShardSync(String workerId) {
        return CollectionUtils.isNullOrEmpty(leaders) || leaders.contains(workerId);
    }

    @Override
    public synchronized Boolean isLeader(String workerId) {
        // if no leaders yet, synchronously get leaders. This will happen at first Shard Sync.
        if (executeConditionCheckWithReadLock(() -> CollectionUtils.isNullOrEmpty(leaders))) {
            electLeaders();
            // start a scheduled executor that will periodically update leaders.
            // The first run will be after a minute.
            // We don't need jitter since it is scheduled with a fixed delay and time taken to scan leases
            // will be different at different times and on different hosts/workers.
            leaderElectionThreadPool.scheduleWithFixedDelay(
                    this::electLeaders,
                    ELECTION_INITIAL_DELAY_MILLIS,
                    ELECTION_SCHEDULING_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS);
        }
        final boolean response = executeConditionCheckWithReadLock(() -> isWorkerLeaderForShardSync(workerId));
        final MetricsScope metricsScope =
                MetricsUtil.createMetricsWithOperation(metricsFactory, METRIC_OPERATION_LEADER_DECIDER);
        metricsScope.addData(
                METRIC_OPERATION_LEADER_DECIDER_IS_LEADER, response ? 1 : 0, StandardUnit.COUNT, MetricsLevel.DETAILED);
        MetricsUtil.endScope(metricsScope);
        return response;
    }

    @Override
    public synchronized void shutdown() {
        try {
            leaderElectionThreadPool.shutdown();
            if (leaderElectionThreadPool.awaitTermination(AWAIT_TERMINATION_MILLIS, TimeUnit.MILLISECONDS)) {
                log.info("Successfully stopped leader election on the worker");
            } else {
                leaderElectionThreadPool.shutdownNow();
                log.info(String.format(
                        "Stopped leader election thread after awaiting termination for %d milliseconds",
                        AWAIT_TERMINATION_MILLIS));
            }

        } catch (InterruptedException e) {
            log.debug("Encountered InterruptedException while awaiting leader election threadPool termination");
        }
    }

    // Execute condition checks using shared variables under a read-write lock.
    private boolean executeConditionCheckWithReadLock(BooleanSupplier action) {
        try {
            readWriteLock.readLock().lock();
            return action.getAsBoolean();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }
}
