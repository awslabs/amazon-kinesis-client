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
package software.amazon.kinesis.coordinator;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.HashKeyRangeForLease;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.leases.UpdateField;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

import static software.amazon.kinesis.common.HashKeyRangeForLease.fromHashKeyRange;

/**
 * The top level orchestrator for coordinating the periodic shard sync related
 * activities.
 */
@Getter
@EqualsAndHashCode
@Slf4j
@KinesisClientInternalApi
class PeriodicShardSyncManager {
    private static final long INITIAL_DELAY = 60 * 1000L;

    @VisibleForTesting
    static final BigInteger MIN_HASH_KEY = BigInteger.ZERO;

    @VisibleForTesting
    static final BigInteger MAX_HASH_KEY = new BigInteger("2").pow(128).subtract(BigInteger.ONE);

    static final String PERIODIC_SHARD_SYNC_MANAGER = "PeriodicShardSyncManager";
    private final Map<StreamIdentifier, HashRangeHoleTracker> hashRangeHoleTrackerMap = new HashMap<>();

    private final String workerId;
    private LeaderDecider leaderDecider;
    private final LeaseRefresher leaseRefresher;
    private final Map<StreamIdentifier, StreamConfig> currentStreamConfigMap;
    private final Function<StreamConfig, ShardSyncTaskManager> shardSyncTaskManagerProvider;
    private final Map<StreamConfig, ShardSyncTaskManager> streamToShardSyncTaskManagerMap;
    private final ScheduledExecutorService shardSyncThreadPool;
    private final boolean isMultiStreamingMode;
    private final MetricsFactory metricsFactory;
    private final long leasesRecoveryAuditorExecutionFrequencyMillis;
    private final int leasesRecoveryAuditorInconsistencyConfidenceThreshold;

    @Getter(AccessLevel.NONE)
    private final AtomicBoolean leaderSynced;

    private boolean isRunning;

    PeriodicShardSyncManager(
            String workerId,
            LeaseRefresher leaseRefresher,
            Map<StreamIdentifier, StreamConfig> currentStreamConfigMap,
            Function<StreamConfig, ShardSyncTaskManager> shardSyncTaskManagerProvider,
            Map<StreamConfig, ShardSyncTaskManager> streamToShardSyncTaskManagerMap,
            boolean isMultiStreamingMode,
            MetricsFactory metricsFactory,
            long leasesRecoveryAuditorExecutionFrequencyMillis,
            int leasesRecoveryAuditorInconsistencyConfidenceThreshold,
            AtomicBoolean leaderSynced) {
        this(
                workerId,
                leaseRefresher,
                currentStreamConfigMap,
                shardSyncTaskManagerProvider,
                streamToShardSyncTaskManagerMap,
                Executors.newSingleThreadScheduledExecutor(),
                isMultiStreamingMode,
                metricsFactory,
                leasesRecoveryAuditorExecutionFrequencyMillis,
                leasesRecoveryAuditorInconsistencyConfidenceThreshold,
                leaderSynced);
    }

    PeriodicShardSyncManager(
            String workerId,
            LeaseRefresher leaseRefresher,
            Map<StreamIdentifier, StreamConfig> currentStreamConfigMap,
            Function<StreamConfig, ShardSyncTaskManager> shardSyncTaskManagerProvider,
            Map<StreamConfig, ShardSyncTaskManager> streamToShardSyncTaskManagerMap,
            ScheduledExecutorService shardSyncThreadPool,
            boolean isMultiStreamingMode,
            MetricsFactory metricsFactory,
            long leasesRecoveryAuditorExecutionFrequencyMillis,
            int leasesRecoveryAuditorInconsistencyConfidenceThreshold,
            AtomicBoolean leaderSynced) {
        Validate.notBlank(workerId, "WorkerID is required to initialize PeriodicShardSyncManager.");
        this.workerId = workerId;
        this.leaseRefresher = leaseRefresher;
        this.currentStreamConfigMap = currentStreamConfigMap;
        this.shardSyncTaskManagerProvider = shardSyncTaskManagerProvider;
        this.streamToShardSyncTaskManagerMap = streamToShardSyncTaskManagerMap;
        this.shardSyncThreadPool = shardSyncThreadPool;
        this.isMultiStreamingMode = isMultiStreamingMode;
        this.metricsFactory = metricsFactory;
        this.leasesRecoveryAuditorExecutionFrequencyMillis = leasesRecoveryAuditorExecutionFrequencyMillis;
        this.leasesRecoveryAuditorInconsistencyConfidenceThreshold =
                leasesRecoveryAuditorInconsistencyConfidenceThreshold;
        this.leaderSynced = leaderSynced;
    }

    public synchronized TaskResult start(final LeaderDecider leaderDecider) {
        Validate.notNull(leaderDecider, "LeaderDecider is required to start PeriodicShardSyncManager.");
        this.leaderDecider = leaderDecider;
        if (!isRunning) {
            final Runnable periodicShardSyncer = () -> {
                try {
                    runShardSync();
                } catch (Throwable t) {
                    log.error("Error during runShardSync.", t);
                }
            };
            shardSyncThreadPool.scheduleWithFixedDelay(
                    periodicShardSyncer,
                    INITIAL_DELAY,
                    leasesRecoveryAuditorExecutionFrequencyMillis,
                    TimeUnit.MILLISECONDS);
            isRunning = true;
        }
        return new TaskResult(null);
    }

    /**
     * Runs shardSync once
     * Does not schedule periodic shardSync
     */
    public synchronized void syncShardsOnce() throws Exception {
        // TODO: Resume the shard sync from failed stream in the next attempt, to avoid syncing
        // TODO: for already synced streams
        for (StreamConfig streamConfig : currentStreamConfigMap.values()) {
            log.info("Syncing Kinesis shard info for {}", streamConfig);
            final ShardSyncTaskManager shardSyncTaskManager = shardSyncTaskManagerProvider.apply(streamConfig);
            final TaskResult taskResult = shardSyncTaskManager.callShardSyncTask();
            if (taskResult.getException() != null) {
                throw taskResult.getException();
            }
        }
    }

    public void stop() {
        if (isRunning) {
            log.info(String.format("Shutting down leader decider on worker %s", workerId));
            leaderDecider.shutdown();
            log.info(String.format("Shutting down periodic shard sync task scheduler on worker %s", workerId));
            shardSyncThreadPool.shutdown();
            isRunning = false;
        }
    }

    private void runShardSync() {
        if (leaderDecider.isLeader(workerId) && leaderSynced.get()) {
            log.info(String.format("WorkerId %s is leader, running the periodic shard sync task", workerId));

            final MetricsScope scope =
                    MetricsUtil.createMetricsWithOperation(metricsFactory, PERIODIC_SHARD_SYNC_MANAGER);
            int numStreamsWithPartialLeases = 0;
            int numStreamsToSync = 0;
            int numSkippedShardSyncTask = 0;
            boolean isRunSuccess = false;
            final long runStartMillis = System.currentTimeMillis();

            try {
                // Create a copy of the streams to be considered for this run to avoid data race with Scheduler.
                final Set<StreamIdentifier> streamConfigMap = new HashSet<>(currentStreamConfigMap.keySet());

                // Construct the stream to leases map to be used in the lease sync
                final Map<StreamIdentifier, List<Lease>> streamToLeasesMap = getStreamToLeasesMap(streamConfigMap);

                // For each of the stream, check if shard sync needs to be done based on the leases state.
                for (StreamIdentifier streamIdentifier : streamConfigMap) {
                    if (!currentStreamConfigMap.containsKey(streamIdentifier)) {
                        log.info("Skipping shard sync task for {} as stream is purged", streamIdentifier);
                        continue;
                    }
                    final ShardSyncResponse shardSyncResponse =
                            checkForShardSync(streamIdentifier, streamToLeasesMap.get(streamIdentifier));

                    numStreamsWithPartialLeases += shardSyncResponse.isHoleDetected() ? 1 : 0;
                    numStreamsToSync += shardSyncResponse.shouldDoShardSync ? 1 : 0;

                    if (shardSyncResponse.shouldDoShardSync()) {
                        log.info(
                                "Periodic shard syncer initiating shard sync for {} due to the reason - {} ",
                                streamIdentifier,
                                shardSyncResponse.reasonForDecision());
                        final StreamConfig streamConfig = currentStreamConfigMap.get(streamIdentifier);
                        if (streamConfig == null) {
                            log.info("Skipping shard sync task for {} as stream is purged", streamIdentifier);
                            continue;
                        }
                        final ShardSyncTaskManager shardSyncTaskManager;
                        if (streamToShardSyncTaskManagerMap.containsKey(streamConfig)) {
                            log.info(
                                    "shardSyncTaskManager for stream {} already exists", streamIdentifier.streamName());
                            shardSyncTaskManager = streamToShardSyncTaskManagerMap.get(streamConfig);
                        } else {
                            // If streamConfig of a stream has already been added to currentStreamConfigMap but
                            // Scheduler failed to create shardSyncTaskManager for it, then Scheduler will not try
                            // to create one later. So enable PeriodicShardSyncManager to do it for such cases
                            log.info(
                                    "Failed to get shardSyncTaskManager so creating one for stream {}.",
                                    streamIdentifier.streamName());
                            shardSyncTaskManager = streamToShardSyncTaskManagerMap.computeIfAbsent(
                                    streamConfig, s -> shardSyncTaskManagerProvider.apply(s));
                        }
                        if (!shardSyncTaskManager.submitShardSyncTask()) {
                            log.warn(
                                    "Failed to submit shard sync task for stream {}. This could be due to the previous pending shard sync task.",
                                    shardSyncTaskManager
                                            .shardDetector()
                                            .streamIdentifier()
                                            .streamName());
                            numSkippedShardSyncTask += 1;
                        } else {
                            log.info(
                                    "Submitted shard sync task for stream {} because of reason {}",
                                    shardSyncTaskManager
                                            .shardDetector()
                                            .streamIdentifier()
                                            .streamName(),
                                    shardSyncResponse.reasonForDecision());
                        }
                    } else {
                        log.info(
                                "Skipping shard sync for {} due to the reason - {}",
                                streamIdentifier,
                                shardSyncResponse.reasonForDecision());
                    }
                }
                isRunSuccess = true;
            } catch (Exception e) {
                log.error("Caught exception while running periodic shard syncer.", e);
            } finally {
                scope.addData(
                        "NumStreamsWithPartialLeases",
                        numStreamsWithPartialLeases,
                        StandardUnit.COUNT,
                        MetricsLevel.SUMMARY);
                scope.addData("NumStreamsToSync", numStreamsToSync, StandardUnit.COUNT, MetricsLevel.SUMMARY);
                scope.addData(
                        "NumSkippedShardSyncTask", numSkippedShardSyncTask, StandardUnit.COUNT, MetricsLevel.SUMMARY);
                MetricsUtil.addSuccessAndLatency(scope, isRunSuccess, runStartMillis, MetricsLevel.SUMMARY);
                scope.end();
            }
        } else {
            log.debug("WorkerId {} is not a leader, not running the shard sync task", workerId);
        }
    }

    /**
     * Retrieve all the streams, along with their associated leases
     * @param streamIdentifiersToFilter
     * @return
     * @throws DependencyException
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     */
    private Map<StreamIdentifier, List<Lease>> getStreamToLeasesMap(
            final Set<StreamIdentifier> streamIdentifiersToFilter)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        final List<Lease> leases = leaseRefresher.listLeases();
        if (!isMultiStreamingMode) {
            Validate.isTrue(streamIdentifiersToFilter.size() == 1);
            return Collections.singletonMap(streamIdentifiersToFilter.iterator().next(), leases);
        } else {
            final Map<StreamIdentifier, List<Lease>> streamToLeasesMap = new HashMap<>();
            for (Lease lease : leases) {
                StreamIdentifier streamIdentifier =
                        StreamIdentifier.multiStreamInstance(((MultiStreamLease) lease).streamIdentifier());
                if (streamIdentifiersToFilter.contains(streamIdentifier)) {
                    streamToLeasesMap
                            .computeIfAbsent(streamIdentifier, s -> new ArrayList<>())
                            .add(lease);
                }
            }
            return streamToLeasesMap;
        }
    }

    /**
     * Given a list of leases for a stream, determine if a shard sync is necessary.
     * @param streamIdentifier
     * @param leases
     * @return
     */
    @VisibleForTesting
    ShardSyncResponse checkForShardSync(StreamIdentifier streamIdentifier, List<Lease> leases) {
        if (CollectionUtils.isNullOrEmpty(leases)) {
            // If the leases is null or empty then we need to do shard sync
            log.info("No leases found for {}. Will be triggering shard sync", streamIdentifier);
            return new ShardSyncResponse(true, false, "No leases found for " + streamIdentifier);
        }
        // Check if there are any holes in the leases and return the first hole if present.
        Optional<HashRangeHole> hashRangeHoleOpt = hasHoleInLeases(streamIdentifier, leases);
        if (hashRangeHoleOpt.isPresent()) {
            // If hole is present, check if the hole is detected consecutively in previous occurrences.
            // If hole is determined with high confidence return true; return false otherwise
            // We are using the high confidence factor to avoid shard sync on any holes during resharding and
            // lease cleanups or any intermittent issues.
            final HashRangeHoleTracker hashRangeHoleTracker =
                    hashRangeHoleTrackerMap.computeIfAbsent(streamIdentifier, s -> new HashRangeHoleTracker());
            final boolean hasHoleWithHighConfidence =
                    hashRangeHoleTracker.hasHighConfidenceOfHoleWith(hashRangeHoleOpt.get());
            return new ShardSyncResponse(
                    hasHoleWithHighConfidence,
                    true,
                    "Detected same hole for " + hashRangeHoleTracker.getNumConsecutiveHoles()
                            + " times. Shard sync will be initiated when threshold reaches "
                            + leasesRecoveryAuditorInconsistencyConfidenceThreshold);
        } else {
            // If hole is not present, clear any previous tracking for this stream and return false;
            hashRangeHoleTrackerMap.remove(streamIdentifier);
            return new ShardSyncResponse(false, false, "Hash Ranges are complete for " + streamIdentifier);
        }
    }

    /**
     * Object containing metadata about the state of a shard sync
     */
    @Value
    @Accessors(fluent = true)
    @VisibleForTesting
    static class ShardSyncResponse {

        /**
         * Flag to determine if a shard sync is necessary or not
         */
        private final boolean shouldDoShardSync;

        private final boolean isHoleDetected;

        /**
         * Reason behind the state of 'shouldDoShardSync' flag
         */
        private final String reasonForDecision;
    }

    @VisibleForTesting
    Optional<HashRangeHole> hasHoleInLeases(StreamIdentifier streamIdentifier, List<Lease> leases) {
        // Filter the leases with any checkpoint other than shard end.
        List<Lease> activeLeases = leases.stream()
                .filter(lease ->
                        lease.checkpoint() != null && !lease.checkpoint().isShardEnd())
                .collect(Collectors.toList());
        List<Lease> activeLeasesWithHashRanges = fillWithHashRangesIfRequired(streamIdentifier, activeLeases);
        return checkForHoleInHashKeyRanges(streamIdentifier, activeLeasesWithHashRanges);
    }

    // If leases are missing hashranges information, update the leases in-memory as well as in the lease storage
    // by learning from kinesis shards.
    private List<Lease> fillWithHashRangesIfRequired(StreamIdentifier streamIdentifier, List<Lease> activeLeases) {
        List<Lease> activeLeasesWithNoHashRanges = activeLeases.stream()
                .filter(lease -> lease.hashKeyRangeForLease() == null)
                .collect(Collectors.toList());
        Optional<Lease> minLeaseOpt = activeLeasesWithNoHashRanges.stream().min(Comparator.comparing(Lease::leaseKey));
        if (minLeaseOpt.isPresent()) {
            // TODO : use minLease for new ListShards with startingShardId
            final Lease minLease = minLeaseOpt.get();
            final ShardDetector shardDetector = shardSyncTaskManagerProvider
                    .apply(currentStreamConfigMap.get(streamIdentifier))
                    .shardDetector();
            final Map<String, Shard> kinesisShards =
                    shardDetector.listShards().stream().collect(Collectors.toMap(Shard::shardId, shard -> shard));
            return activeLeases.stream()
                    .map(lease -> {
                        if (lease.hashKeyRangeForLease() == null) {
                            final String shardId = lease instanceof MultiStreamLease
                                    ? ((MultiStreamLease) lease).shardId()
                                    : lease.leaseKey();
                            final Shard shard = kinesisShards.get(shardId);
                            if (shard == null) {
                                return lease;
                            }
                            lease.hashKeyRange(fromHashKeyRange(shard.hashKeyRange()));
                            try {
                                leaseRefresher.updateLeaseWithMetaInfo(lease, UpdateField.HASH_KEY_RANGE);
                            } catch (Exception e) {
                                log.warn(
                                        "Unable to update hash range key information for lease {} of stream {}. "
                                                + "This may result in explicit lease sync.",
                                        lease.leaseKey(),
                                        streamIdentifier);
                            }
                        }
                        return lease;
                    })
                    .filter(lease -> lease.hashKeyRangeForLease() != null)
                    .collect(Collectors.toList());
        } else {
            return activeLeases;
        }
    }

    @VisibleForTesting
    static Optional<HashRangeHole> checkForHoleInHashKeyRanges(
            StreamIdentifier streamIdentifier, List<Lease> leasesWithHashKeyRanges) {
        // Sort the hash ranges by starting hash key.
        List<Lease> sortedLeasesWithHashKeyRanges = sortLeasesByHashRange(leasesWithHashKeyRanges);
        if (sortedLeasesWithHashKeyRanges.isEmpty()) {
            log.error("No leases with valid hashranges found for stream {}", streamIdentifier);
            return Optional.of(new HashRangeHole());
        }
        // Validate for hashranges bounds.
        if (!sortedLeasesWithHashKeyRanges
                        .get(0)
                        .hashKeyRangeForLease()
                        .startingHashKey()
                        .equals(MIN_HASH_KEY)
                || !sortedLeasesWithHashKeyRanges
                        .get(sortedLeasesWithHashKeyRanges.size() - 1)
                        .hashKeyRangeForLease()
                        .endingHashKey()
                        .equals(MAX_HASH_KEY)) {
            log.error(
                    "Incomplete hash range found for stream {} between {} and {}.",
                    streamIdentifier,
                    sortedLeasesWithHashKeyRanges.get(0),
                    sortedLeasesWithHashKeyRanges.get(sortedLeasesWithHashKeyRanges.size() - 1));
            return Optional.of(new HashRangeHole(
                    sortedLeasesWithHashKeyRanges.get(0).hashKeyRangeForLease(),
                    sortedLeasesWithHashKeyRanges
                            .get(sortedLeasesWithHashKeyRanges.size() - 1)
                            .hashKeyRangeForLease()));
        }
        // Check for any holes in the sorted hashrange intervals.
        if (sortedLeasesWithHashKeyRanges.size() > 1) {
            Lease leftMostLeaseToReportInCaseOfHole = sortedLeasesWithHashKeyRanges.get(0);
            HashKeyRangeForLease leftLeaseHashRange = leftMostLeaseToReportInCaseOfHole.hashKeyRangeForLease();
            for (int i = 1; i < sortedLeasesWithHashKeyRanges.size(); i++) {
                final HashKeyRangeForLease rightLeaseHashRange =
                        sortedLeasesWithHashKeyRanges.get(i).hashKeyRangeForLease();
                final BigInteger rangeDiff =
                        rightLeaseHashRange.startingHashKey().subtract(leftLeaseHashRange.endingHashKey());
                // Case of overlapping leases when the rangediff is 0 or negative.
                // signum() will be -1 for negative and 0 if value is 0.
                // Merge the range for further tracking.
                if (rangeDiff.signum() <= 0) {
                    leftLeaseHashRange = new HashKeyRangeForLease(
                            leftLeaseHashRange.startingHashKey(),
                            leftLeaseHashRange.endingHashKey().max(rightLeaseHashRange.endingHashKey()));
                } else {
                    // Case of non overlapping leases when rangediff is positive. signum() will be 1 for positive.
                    // If rangeDiff is 1, then it is a case of continuous hashrange. If not, it is a hole.
                    if (!rangeDiff.equals(BigInteger.ONE)) {
                        log.error(
                                "Incomplete hash range found for {} between {} and {}.",
                                streamIdentifier,
                                leftMostLeaseToReportInCaseOfHole,
                                sortedLeasesWithHashKeyRanges.get(i));
                        return Optional.of(new HashRangeHole(
                                leftMostLeaseToReportInCaseOfHole.hashKeyRangeForLease(),
                                sortedLeasesWithHashKeyRanges.get(i).hashKeyRangeForLease()));
                    }
                    leftMostLeaseToReportInCaseOfHole = sortedLeasesWithHashKeyRanges.get(i);
                    leftLeaseHashRange = rightLeaseHashRange;
                }
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    static List<Lease> sortLeasesByHashRange(List<Lease> leasesWithHashKeyRanges) {
        if (leasesWithHashKeyRanges.size() == 0 || leasesWithHashKeyRanges.size() == 1) {
            return leasesWithHashKeyRanges;
        }
        Collections.sort(leasesWithHashKeyRanges, new HashKeyRangeComparator());
        return leasesWithHashKeyRanges;
    }

    @Value
    private static class HashRangeHole {
        HashRangeHole() {
            hashRangeAtStartOfPossibleHole = hashRangeAtEndOfPossibleHole = null;
        }

        HashRangeHole(
                HashKeyRangeForLease hashRangeAtStartOfPossibleHole,
                HashKeyRangeForLease hashRangeAtEndOfPossibleHole) {
            this.hashRangeAtStartOfPossibleHole = hashRangeAtStartOfPossibleHole;
            this.hashRangeAtEndOfPossibleHole = hashRangeAtEndOfPossibleHole;
        }

        private final HashKeyRangeForLease hashRangeAtStartOfPossibleHole;
        private final HashKeyRangeForLease hashRangeAtEndOfPossibleHole;
    }

    private class HashRangeHoleTracker {
        private HashRangeHole hashRangeHole;

        @Getter
        private Integer numConsecutiveHoles;

        public boolean hasHighConfidenceOfHoleWith(@NonNull HashRangeHole hashRangeHole) {
            if (hashRangeHole.equals(this.hashRangeHole)) {
                ++this.numConsecutiveHoles;
            } else {
                this.hashRangeHole = hashRangeHole;
                this.numConsecutiveHoles = 1;
            }
            return numConsecutiveHoles >= leasesRecoveryAuditorInconsistencyConfidenceThreshold;
        }
    }

    /**
     * Helper class to compare leases based on their hash range.
     */
    private static class HashKeyRangeComparator implements Comparator<Lease>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Lease lease, Lease otherLease) {
            Validate.notNull(lease);
            Validate.notNull(otherLease);
            Validate.notNull(lease.hashKeyRangeForLease());
            Validate.notNull(otherLease.hashKeyRangeForLease());
            return ComparisonChain.start()
                    .compare(
                            lease.hashKeyRangeForLease().startingHashKey(),
                            otherLease.hashKeyRangeForLease().startingHashKey())
                    .compare(
                            lease.hashKeyRangeForLease().endingHashKey(),
                            otherLease.hashKeyRangeForLease().endingHashKey())
                    .result();
        }
    }
}
