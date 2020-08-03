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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.HashKeyRangeForLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.UpdateField;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.util.CollectionUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.amazonaws.services.kinesis.leases.impl.HashKeyRangeForLease.fromHashKeyRange;

/**
 * The top level orchestrator for coordinating the periodic shard sync related activities. If the configured
 * {@link ShardSyncStrategyType} is PERIODIC, this class will be the main shard sync orchestrator. For non-PERIODIC
 * strategies, this class will serve as an internal auditor that periodically checks if the full hash range is covered
 * by currently held leases, and initiates a recovery shard sync if not.
 */
@Getter
@EqualsAndHashCode
class PeriodicShardSyncManager {
    private static final Log LOG = LogFactory.getLog(PeriodicShardSyncManager.class);
    private static final long INITIAL_DELAY = 0;

    /** DEFAULT interval is used for PERIODIC {@link ShardSyncStrategyType}. */
    private static final long DEFAULT_PERIODIC_SHARD_SYNC_INTERVAL_MILLIS = 1000L;

    /** Parameters for validating hash range completeness when running in auditor mode. */
    @VisibleForTesting
    static final BigInteger MIN_HASH_KEY = BigInteger.ZERO;
    @VisibleForTesting
    static final BigInteger MAX_HASH_KEY = new BigInteger("2").pow(128).subtract(BigInteger.ONE);
    static final String PERIODIC_SHARD_SYNC_MANAGER = "PeriodicShardSyncManager";
    private final HashRangeHoleTracker hashRangeHoleTracker = new HashRangeHoleTracker();

    private final String workerId;
    private final LeaderDecider leaderDecider;
    private final ITask metricsEmittingShardSyncTask;
    private final ScheduledExecutorService shardSyncThreadPool;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private final IKinesisProxy kinesisProxy;
    private final boolean isAuditorMode;
    private final long periodicShardSyncIntervalMillis;
    private boolean isRunning;
    private final IMetricsFactory metricsFactory;
    private final int leasesRecoveryAuditorInconsistencyConfidenceThreshold;


    PeriodicShardSyncManager(String workerId,
                             LeaderDecider leaderDecider,
                             ShardSyncTask shardSyncTask,
                             IMetricsFactory metricsFactory,
                             ILeaseManager<KinesisClientLease> leaseManager,
                             IKinesisProxy kinesisProxy,
                             boolean isAuditorMode,
                             long leasesRecoveryAuditorExecutionFrequencyMillis,
                             int leasesRecoveryAuditorInconsistencyConfidenceThreshold) {
       this(workerId, leaderDecider, shardSyncTask, Executors.newSingleThreadScheduledExecutor(), metricsFactory,
            leaseManager, kinesisProxy, isAuditorMode, leasesRecoveryAuditorExecutionFrequencyMillis,
            leasesRecoveryAuditorInconsistencyConfidenceThreshold);
    }

    PeriodicShardSyncManager(String workerId,
                             LeaderDecider leaderDecider,
                             ShardSyncTask shardSyncTask,
                             ScheduledExecutorService shardSyncThreadPool,
                             IMetricsFactory metricsFactory,
                             ILeaseManager<KinesisClientLease> leaseManager,
                             IKinesisProxy kinesisProxy,
                             boolean isAuditorMode,
                             long leasesRecoveryAuditorExecutionFrequencyMillis,
                             int leasesRecoveryAuditorInconsistencyConfidenceThreshold) {
        Validate.notBlank(workerId, "WorkerID is required to initialize PeriodicShardSyncManager.");
        Validate.notNull(leaderDecider, "LeaderDecider is required to initialize PeriodicShardSyncManager.");
        Validate.notNull(shardSyncTask, "ShardSyncTask is required to initialize PeriodicShardSyncManager.");
        this.workerId = workerId;
        this.leaderDecider = leaderDecider;
        this.metricsEmittingShardSyncTask = new MetricsCollectingTaskDecorator(shardSyncTask, metricsFactory);
        this.shardSyncThreadPool = shardSyncThreadPool;
        this.leaseManager = leaseManager;
        this.kinesisProxy = kinesisProxy;
        this.metricsFactory = metricsFactory;
        this.isAuditorMode = isAuditorMode;
        this.leasesRecoveryAuditorInconsistencyConfidenceThreshold = leasesRecoveryAuditorInconsistencyConfidenceThreshold;
        if (isAuditorMode) {
            Validate.notNull(this.leaseManager, "LeaseManager is required for non-PERIODIC shard sync strategies.");
            Validate.notNull(this.kinesisProxy, "KinesisProxy is required for non-PERIODIC shard sync strategies.");
            this.periodicShardSyncIntervalMillis = leasesRecoveryAuditorExecutionFrequencyMillis;
        } else {
            this.periodicShardSyncIntervalMillis = DEFAULT_PERIODIC_SHARD_SYNC_INTERVAL_MILLIS;
        }
    }

    public synchronized TaskResult start() {
        if (!isRunning) {
            final Runnable periodicShardSyncer = () -> {
                try {
                    runShardSync();
                } catch (Throwable t) {
                    LOG.error("Error running shard sync.", t);
                }
            };

            shardSyncThreadPool
                    .scheduleWithFixedDelay(periodicShardSyncer, INITIAL_DELAY, periodicShardSyncIntervalMillis,
                            TimeUnit.MILLISECONDS);
            isRunning = true;
        }
        return new TaskResult(null);
    }

    /**
     * Runs ShardSync once, without scheduling further periodic ShardSyncs.
     * @return TaskResult from shard sync
     */
    public synchronized TaskResult syncShardsOnce() {
        LOG.info("Syncing shards once from worker " + workerId);
        return metricsEmittingShardSyncTask.call();
    }

    public void stop() {
        if (isRunning) {
            LOG.info(String.format("Shutting down leader decider on worker %s", workerId));
            leaderDecider.shutdown();
            LOG.info(String.format("Shutting down periodic shard sync task scheduler on worker %s", workerId));
            shardSyncThreadPool.shutdown();
            isRunning = false;
        }
    }

    private void runShardSync() {
        if (leaderDecider.isLeader(workerId)) {
            LOG.debug("WorkerId " + workerId + " is a leader, running the shard sync task");

            MetricsHelper.startScope(metricsFactory, PERIODIC_SHARD_SYNC_MANAGER);
            boolean isRunSuccess = false;
            final long runStartMillis = System.currentTimeMillis();

            try {
                final ShardSyncResponse shardSyncResponse = checkForShardSync();
                MetricsHelper.getMetricsScope().addData("NumStreamsToSync", shardSyncResponse.shouldDoShardSync() ? 1 : 0, StandardUnit.Count, MetricsLevel.SUMMARY);
                MetricsHelper.getMetricsScope().addData("NumStreamsWithPartialLeases", shardSyncResponse.isHoleDetected() ? 1 : 0, StandardUnit.Count, MetricsLevel.SUMMARY);
                if (shardSyncResponse.shouldDoShardSync()) {
                    LOG.info("Periodic shard syncer initiating shard sync due to the reason - " +
                            shardSyncResponse.reasonForDecision());
                    metricsEmittingShardSyncTask.call();
                } else {
                    LOG.info("Skipping shard sync due to the reason - " + shardSyncResponse.reasonForDecision());
                }
                isRunSuccess = true;
            } catch (Exception e) {
                LOG.error("Caught exception while running periodic shard syncer.", e);
            } finally {
                MetricsHelper.addSuccessAndLatency(runStartMillis, isRunSuccess, MetricsLevel.SUMMARY);
                MetricsHelper.endScope();
            }
        } else {
            LOG.debug("WorkerId " + workerId + " is not a leader, not running the shard sync task");
        }
    }

    @VisibleForTesting
    ShardSyncResponse checkForShardSync() throws DependencyException, InvalidStateException,
            ProvisionedThroughputException {

        if (!isAuditorMode) {
            // If we are running with PERIODIC shard sync strategy, we should sync every time.
            return new ShardSyncResponse(true, false, "Syncing every time with PERIODIC shard sync strategy.");
        }

        // Get current leases from DynamoDB.
        final List<KinesisClientLease> currentLeases = leaseManager.listLeases();

        if (CollectionUtils.isNullOrEmpty(currentLeases)) {
            // If the current leases are null or empty, then we need to initiate a shard sync.
            LOG.info("No leases found. Will trigger a shard sync.");
            return new ShardSyncResponse(true, false, "No leases found.");
        }

        // Check if there are any holes in the hash range covered by current leases. Return the first hole if present.
        Optional<HashRangeHole> hashRangeHoleOpt = hasHoleInLeases(currentLeases);
        if (hashRangeHoleOpt.isPresent()) {
            // If hole is present, check if the hole is detected consecutively in previous occurrences. If hole is
            // determined with high confidence, return true; return false otherwise. We use the high confidence factor
            // to avoid shard sync on any holes during resharding and lease cleanups, or other intermittent issues.
            final boolean hasHoleWithHighConfidence =
                    hashRangeHoleTracker.hashHighConfidenceOfHoleWith(hashRangeHoleOpt.get());

            return new ShardSyncResponse(hasHoleWithHighConfidence, true,
                    "Detected the same hole for " + hashRangeHoleTracker.getNumConsecutiveHoles() + " times. " +
                    "Will initiate shard sync after reaching threshold: " + leasesRecoveryAuditorInconsistencyConfidenceThreshold);
        } else {
            // If hole is not present, clear any previous hole tracking and return false.
            hashRangeHoleTracker.reset();
            return new ShardSyncResponse(false, false, "Hash range is complete.");
        }
    }

    @VisibleForTesting
    Optional<HashRangeHole> hasHoleInLeases(List<KinesisClientLease> leases) {
        // Filter out any leases with checkpoints other than SHARD_END
        final List<KinesisClientLease> activeLeases = leases.stream()
                .filter(lease -> lease.getCheckpoint() != null && !lease.getCheckpoint().isShardEnd())
                .collect(Collectors.toList());

        final List<KinesisClientLease> activeLeasesWithHashRanges = fillWithHashRangesIfRequired(activeLeases);
        return checkForHoleInHashKeyRanges(activeLeasesWithHashRanges);
    }

    private List<KinesisClientLease> fillWithHashRangesIfRequired(List<KinesisClientLease> activeLeases) {
        final List<KinesisClientLease> activeLeasesWithNoHashRanges = activeLeases.stream()
                .filter(lease -> lease.getHashKeyRange() == null).collect(Collectors.toList());

        if (activeLeasesWithNoHashRanges.isEmpty()) {
            return activeLeases;
        }

        // Fetch shards from Kinesis to fill in the in-memory hash ranges
        final Map<String, Shard> kinesisShards = kinesisProxy.getShardList().stream()
                .collect(Collectors.toMap(Shard::getShardId, shard -> shard));

        return activeLeases.stream().map(lease -> {
            if (lease.getHashKeyRange() == null) {
                final String shardId = lease.getLeaseKey();
                final Shard shard = kinesisShards.get(shardId);
                if (shard == null) {
                    return lease;
                }
                lease.setHashKeyRange(fromHashKeyRange(shard.getHashKeyRange()));

                try {
                    leaseManager.updateLeaseWithMetaInfo(lease, UpdateField.HASH_KEY_RANGE);
                } catch (Exception e) {
                    LOG.warn("Unable to update hash range information for lease " + lease.getLeaseKey() +
                            ". This may result in explicit lease sync.");
                }
            }
            return lease;
        }).filter(lease -> lease.getHashKeyRange() != null).collect(Collectors.toList());
    }

    @VisibleForTesting
    static Optional<HashRangeHole> checkForHoleInHashKeyRanges(List<KinesisClientLease> leasesWithHashKeyRanges) {
        // Sort the hash ranges by starting hash key
        final List<KinesisClientLease> sortedLeasesWithHashKeyRanges = sortLeasesByHashRange(leasesWithHashKeyRanges);
        if (sortedLeasesWithHashKeyRanges.isEmpty()) {
            LOG.error("No leases with valid hash ranges found.");
            return Optional.of(new HashRangeHole());
        }

        // Validate the hash range bounds
        final KinesisClientLease minHashKeyLease = sortedLeasesWithHashKeyRanges.get(0);
        final KinesisClientLease maxHashKeyLease =
                sortedLeasesWithHashKeyRanges.get(sortedLeasesWithHashKeyRanges.size() - 1);
        if (!minHashKeyLease.getHashKeyRange().startingHashKey().equals(MIN_HASH_KEY) ||
            !maxHashKeyLease.getHashKeyRange().endingHashKey().equals(MAX_HASH_KEY)) {
            LOG.error("Incomplete hash range found between " + minHashKeyLease + " and " + maxHashKeyLease);
            return Optional.of(new HashRangeHole(minHashKeyLease.getHashKeyRange(), maxHashKeyLease.getHashKeyRange()));
        }

        // Check for any holes in the sorted hash range intervals
        if (sortedLeasesWithHashKeyRanges.size() > 1) {
            KinesisClientLease leftmostLeaseToReportInCaseOfHole = minHashKeyLease;
            HashKeyRangeForLease leftLeaseHashRange = leftmostLeaseToReportInCaseOfHole.getHashKeyRange();

            for (int i = 1; i < sortedLeasesWithHashKeyRanges.size(); i++) {
                final KinesisClientLease rightLease = sortedLeasesWithHashKeyRanges.get(i);
                final HashKeyRangeForLease rightLeaseHashRange = rightLease.getHashKeyRange();
                final BigInteger rangeDiff =
                        rightLeaseHashRange.startingHashKey().subtract(leftLeaseHashRange.endingHashKey());
                // We have overlapping leases when rangeDiff is 0 or negative.
                // signum() will be -1 for negative and 0 if value is 0.
                // Merge the ranges for further tracking.
                if (rangeDiff.signum() <= 0) {
                    leftLeaseHashRange = new HashKeyRangeForLease(leftLeaseHashRange.startingHashKey(),
                            leftLeaseHashRange.endingHashKey().max(rightLeaseHashRange.endingHashKey()));
                } else {
                    // We have non-overlapping leases when rangeDiff is positive. signum() will be 1 in this case.
                    // If rangeDiff is 1, then it is a continuous hash range. If not, there is a hole.
                    if (!rangeDiff.equals(BigInteger.ONE)) {
                        LOG.error("Incomplete hash range found between " + leftmostLeaseToReportInCaseOfHole +
                                " and " + rightLease);
                        return Optional.of(new HashRangeHole(leftmostLeaseToReportInCaseOfHole.getHashKeyRange(),
                                rightLease.getHashKeyRange()));
                    }

                    leftmostLeaseToReportInCaseOfHole = rightLease;
                    leftLeaseHashRange = rightLeaseHashRange;
                }
            }
        }

        return Optional.empty();
    }

    @VisibleForTesting
    static List<KinesisClientLease> sortLeasesByHashRange(List<KinesisClientLease> leasesWithHashKeyRanges) {
        if (leasesWithHashKeyRanges.size() == 0 || leasesWithHashKeyRanges.size() == 1) {
            return leasesWithHashKeyRanges;
        }
        Collections.sort(leasesWithHashKeyRanges, new HashKeyRangeComparator());
        return leasesWithHashKeyRanges;
    }

    @Value
    @Accessors(fluent = true)
    @VisibleForTesting
    static class ShardSyncResponse {
        private final boolean shouldDoShardSync;
        private final boolean isHoleDetected;
        private final String reasonForDecision;
    }

    @Value
    private static class HashRangeHole {
        private final HashKeyRangeForLease hashRangeAtStartOfPossibleHole;
        private final HashKeyRangeForLease hashRangeAtEndOfPossibleHole;

        HashRangeHole() {
            hashRangeAtStartOfPossibleHole = hashRangeAtEndOfPossibleHole = null;
        }

        HashRangeHole(HashKeyRangeForLease hashRangeAtStartOfPossibleHole,
                      HashKeyRangeForLease hashRangeAtEndOfPossibleHole) {
            this.hashRangeAtStartOfPossibleHole = hashRangeAtStartOfPossibleHole;
            this.hashRangeAtEndOfPossibleHole = hashRangeAtEndOfPossibleHole;
        }
    }

    private class HashRangeHoleTracker {
        private HashRangeHole hashRangeHole;
        @Getter
        private Integer numConsecutiveHoles;

        public boolean hashHighConfidenceOfHoleWith(@NonNull HashRangeHole hashRangeHole) {
            if (hashRangeHole.equals(this.hashRangeHole)) {
                ++this.numConsecutiveHoles;
            } else {
                this.hashRangeHole = hashRangeHole;
                this.numConsecutiveHoles = 1;
            }

            return numConsecutiveHoles >= leasesRecoveryAuditorInconsistencyConfidenceThreshold;
        }

        public void reset() {
            this.hashRangeHole = null;
            this.numConsecutiveHoles = 0;
        }
    }

    private static class HashKeyRangeComparator implements Comparator<KinesisClientLease>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(KinesisClientLease lease, KinesisClientLease otherLease) {
            Validate.notNull(lease);
            Validate.notNull(otherLease);
            Validate.notNull(lease.getHashKeyRange());
            Validate.notNull(otherLease.getHashKeyRange());
            return ComparisonChain.start()
                    .compare(lease.getHashKeyRange().startingHashKey(), otherLease.getHashKeyRange().startingHashKey())
                    .compare(lease.getHashKeyRange().endingHashKey(), otherLease.getHashKeyRange().endingHashKey())
                    .result();
        }
    }
}
