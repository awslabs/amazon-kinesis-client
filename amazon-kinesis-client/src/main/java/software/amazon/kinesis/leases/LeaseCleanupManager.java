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

package software.amazon.kinesis.leases;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.LeasePendingDeletion;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.AWSExceptionManager;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Helper class to cleanup of any expired/closed shard leases. It will cleanup leases periodically as defined by
 * {@link LeaseManagementConfig#leaseCleanupConfig()} asynchronously.
 */
@Accessors(fluent=true)
@Slf4j
@RequiredArgsConstructor
@EqualsAndHashCode
public class LeaseCleanupManager {
    @NonNull
    private final LeaseCoordinator leaseCoordinator;
    @NonNull
    private final MetricsFactory metricsFactory;
    @NonNull
    private final ScheduledExecutorService deletionThreadPool;
    private final boolean cleanupLeasesUponShardCompletion;
    private final long leaseCleanupIntervalMillis;
    private final long completedLeaseCleanupIntervalMillis;
    private final long garbageLeaseCleanupIntervalMillis;
    private final Stopwatch completedLeaseStopwatch = Stopwatch.createUnstarted();
    private final Stopwatch garbageLeaseStopwatch = Stopwatch.createUnstarted();

    private final Queue<LeasePendingDeletion> deletionQueue = new ConcurrentLinkedQueue<>();

    private static final long INITIAL_DELAY = 0L;

    @Getter
    private volatile boolean isRunning = false;

    /**
     * Starts the lease cleanup thread, which is scheduled periodically as specified by
     * {@link LeaseCleanupManager#leaseCleanupIntervalMillis}
     */
    public void start() {
        if (!isRunning) {
            log.info("Starting lease cleanup thread.");
            completedLeaseStopwatch.reset().start();
            garbageLeaseStopwatch.reset().start();
            deletionThreadPool.scheduleAtFixedRate(new LeaseCleanupThread(), INITIAL_DELAY, leaseCleanupIntervalMillis,
                    TimeUnit.MILLISECONDS);
            isRunning = true;
        } else {
            log.info("Lease cleanup thread already running, no need to start.");
        }
    }

    /**
     * Stops the lease cleanup thread, which is scheduled periodically as specified by
     * {@link LeaseCleanupManager#leaseCleanupIntervalMillis}
     */
    public void shutdown() {
        if (isRunning) {
            log.info("Stopping the lease cleanup thread.");
            completedLeaseStopwatch.stop();
            garbageLeaseStopwatch.stop();
            deletionThreadPool.shutdown();
            isRunning = false;
        } else {
            log.info("Lease cleanup thread already stopped.");
        }
    }

    /**
     * Enqueues a lease for deletion without check for duplicate entry. Use {@link #isEnqueuedForDeletion}
     * for checking the duplicate entries.
     * @param leasePendingDeletion
     */
    public void enqueueForDeletion(LeasePendingDeletion leasePendingDeletion) {
        final Lease lease = leasePendingDeletion.lease();
        if (lease == null) {
            log.warn("Cannot enqueue {} for {} as instance doesn't hold the lease for that shard.",
                    leasePendingDeletion.shardInfo(), leasePendingDeletion.streamIdentifier());
        } else {
            log.debug("Enqueuing lease {} for deferred deletion.", lease.leaseKey());
            if (!deletionQueue.add(leasePendingDeletion)) {
                log.warn("Unable to enqueue lease {} for deletion.", lease.leaseKey());
            }
        }
    }

    /**
     * Check if lease was already enqueued for deletion.
     * //TODO: Optimize verifying duplicate entries https://sim.amazon.com/issues/KinesisLTR-597.
     * @param leasePendingDeletion
     * @return true if enqueued for deletion; false otherwise.
     */
    public boolean isEnqueuedForDeletion(LeasePendingDeletion leasePendingDeletion) {
        return deletionQueue.contains(leasePendingDeletion);
    }

    /**
     * Returns how many leases are currently waiting in the queue pending deletion.
     * @return number of leases pending deletion.
     */
    private int leasesPendingDeletion() {
        return deletionQueue.size();
    }

    /**
     *
     * @return true if the 'Completed Lease Stopwatch' has elapsed more time than the 'Completed Lease Cleanup Interval'
     */
    private boolean timeToCheckForCompletedShard() {
        return completedLeaseStopwatch.elapsed(TimeUnit.MILLISECONDS) >= completedLeaseCleanupIntervalMillis;
    }

    /**
     *
     * @return true if the 'Garbage Lease Stopwatch' has elapsed more time than the 'Garbage Lease Cleanup Interval'
     */
    private boolean timeToCheckForGarbageShard() {
        return garbageLeaseStopwatch.elapsed(TimeUnit.MILLISECONDS) >= garbageLeaseCleanupIntervalMillis;
    }

    public LeaseCleanupResult cleanupLease(LeasePendingDeletion leasePendingDeletion,
            boolean timeToCheckForCompletedShard, boolean timeToCheckForGarbageShard) throws TimeoutException,
            InterruptedException, DependencyException, ProvisionedThroughputException, InvalidStateException {
        final Lease lease = leasePendingDeletion.lease();
        final ShardInfo shardInfo = leasePendingDeletion.shardInfo();
        final StreamIdentifier streamIdentifier = leasePendingDeletion.streamIdentifier();

        final AWSExceptionManager exceptionManager = createExceptionManager();

        boolean cleanedUpCompletedLease = false;
        boolean cleanedUpGarbageLease = false;
        boolean alreadyCheckedForGarbageCollection = false;
        boolean wereChildShardsPresent = false;
        boolean wasResourceNotFound = false;

        try {
            if (cleanupLeasesUponShardCompletion && timeToCheckForCompletedShard) {
                final Lease leaseFromDDB = leaseCoordinator.leaseRefresher().getLease(lease.leaseKey());
                if (leaseFromDDB != null) {
                    Set<String> childShardKeys = leaseFromDDB.childShardIds();
                    if (CollectionUtils.isNullOrEmpty(childShardKeys)) {
                        try {
                            childShardKeys = leasePendingDeletion.getChildShardsFromService();

                            if (CollectionUtils.isNullOrEmpty(childShardKeys)) {
                                log.error(
                                        "No child shards returned from service for shard {} for {} while cleaning up lease.",
                                        shardInfo.shardId(), streamIdentifier.streamName());
                            } else {
                                wereChildShardsPresent = true;
                                updateLeaseWithChildShards(leasePendingDeletion, childShardKeys);
                            }
                        } catch (ExecutionException e) {
                            throw exceptionManager.apply(e.getCause());
                        } finally {
                            alreadyCheckedForGarbageCollection = true;
                        }
                    } else {
                        wereChildShardsPresent = true;
                    }
                    try {
                        cleanedUpCompletedLease = cleanupLeaseForCompletedShard(lease, shardInfo, childShardKeys);
                    } catch (Exception e) {
                        // Suppressing the exception here, so that we can attempt for garbage cleanup.
                        log.warn("Unable to cleanup lease for shard {} in {}", shardInfo.shardId(), streamIdentifier.streamName(), e);
                    }
                } else {
                    log.info("Lease not present in lease table while cleaning the shard {} of {}",
                            shardInfo.shardId(), streamIdentifier.streamName());
                    cleanedUpCompletedLease = true;
                }
            }

            if (!alreadyCheckedForGarbageCollection && timeToCheckForGarbageShard) {
                try {
                    wereChildShardsPresent = !CollectionUtils
                            .isNullOrEmpty(leasePendingDeletion.getChildShardsFromService());
                } catch (ExecutionException e) {
                    throw exceptionManager.apply(e.getCause());
                }
            }
        } catch (ResourceNotFoundException e) {
            wasResourceNotFound = true;
            cleanedUpGarbageLease = cleanupLeaseForGarbageShard(lease, e);
        }

        return new LeaseCleanupResult(cleanedUpCompletedLease, cleanedUpGarbageLease, wereChildShardsPresent,
                wasResourceNotFound);
    }

    // A lease that ended with SHARD_END from ResourceNotFoundException is safe to delete if it no longer exists in the
    // stream (known explicitly from ResourceNotFound being thrown when processing this shard),
    private boolean cleanupLeaseForGarbageShard(Lease lease, Throwable e)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        log.warn("Deleting lease {} as it is not present in the stream.", lease, e);
        leaseCoordinator.leaseRefresher().deleteLease(lease);
        return true;
    }

    /**
     * Check if the all of the parent shards for a given lease have an ongoing lease. If any one parent still has a
     * lease, return false. Otherwise return true
     *
     * @param lease
     * @param shardInfo
     * @return
     * @throws DependencyException
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     */
    private boolean allParentShardLeasesDeleted(Lease lease, ShardInfo shardInfo)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        for (String parentShard : lease.parentShardIds()) {
            final Lease parentLease = leaseCoordinator.leaseRefresher().getLease(ShardInfo.getLeaseKey(shardInfo, parentShard));

            if (parentLease != null) {
                log.warn("Lease {} has a parent lease {} which is still present in the lease table, skipping deletion " +
                        "for this lease.", lease, parentLease);
                return false;
            }
        }
        return true;
    }

    // We should only be deleting the current shard's lease if
    // 1. All of its children are currently being processed, i.e their checkpoint is not TRIM_HORIZON or AT_TIMESTAMP.
    // 2. Its parent shard lease(s) have already been deleted.
    private boolean cleanupLeaseForCompletedShard(Lease lease, ShardInfo shardInfo, Set<String> childShardKeys)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException, IllegalStateException {
        final Set<String> processedChildShardLeaseKeys = new HashSet<>();
        final Set<String> childShardLeaseKeys = childShardKeys.stream().map(ck -> ShardInfo.getLeaseKey(shardInfo, ck))
                .collect(Collectors.toSet());

        for (String childShardLeaseKey : childShardLeaseKeys) {
            final Lease childShardLease = Optional.ofNullable(
                    leaseCoordinator.leaseRefresher().getLease(childShardLeaseKey))
                    .orElseThrow(() -> new IllegalStateException(
                            "Child lease " + childShardLeaseKey + " for completed shard not found in "
                                    + "lease table - not cleaning up lease " + lease));

            if (!childShardLease.checkpoint().equals(ExtendedSequenceNumber.TRIM_HORIZON) && !childShardLease
                    .checkpoint().equals(ExtendedSequenceNumber.AT_TIMESTAMP)) {
                processedChildShardLeaseKeys.add(childShardLease.leaseKey());
            }
        }

        if (!allParentShardLeasesDeleted(lease, shardInfo) || !Objects.equals(childShardLeaseKeys, processedChildShardLeaseKeys)) {
            return false;
        }

        log.info("Deleting lease {} as it has been completely processed and processing of child shard(s) has begun.",
                lease);
        leaseCoordinator.leaseRefresher().deleteLease(lease);

        return true;
    }

    private void updateLeaseWithChildShards(LeasePendingDeletion leasePendingDeletion, Set<String> childShardKeys)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        final Lease updatedLease = leasePendingDeletion.lease();
        updatedLease.childShardIds(childShardKeys);

        leaseCoordinator.leaseRefresher().updateLeaseWithMetaInfo(updatedLease, UpdateField.CHILD_SHARDS);
    }

    private AWSExceptionManager createExceptionManager() {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);

        return exceptionManager;
    }

    @VisibleForTesting
    void cleanupLeases() {
        log.info("Number of pending leases to clean before the scan : {}", leasesPendingDeletion());
        if (deletionQueue.isEmpty()) {
            log.debug("No leases pending deletion.");
        } else if (timeToCheckForCompletedShard() | timeToCheckForGarbageShard()) {
            final Queue<LeasePendingDeletion> failedDeletions = new ConcurrentLinkedQueue<>();
            boolean completedLeaseCleanedUp = false;
            boolean garbageLeaseCleanedUp = false;

            log.debug("Attempting to clean up {} lease(s).", deletionQueue.size());

            while (!deletionQueue.isEmpty()) {
                final LeasePendingDeletion leasePendingDeletion = deletionQueue.poll();
                final String leaseKey = leasePendingDeletion.lease().leaseKey();
                final StreamIdentifier streamIdentifier = leasePendingDeletion.streamIdentifier();
                boolean deletionSucceeded = false;
                try {
                    final LeaseCleanupResult leaseCleanupResult = cleanupLease(leasePendingDeletion,
                            timeToCheckForCompletedShard(), timeToCheckForGarbageShard());
                    completedLeaseCleanedUp |= leaseCleanupResult.cleanedUpCompletedLease();
                    garbageLeaseCleanedUp |= leaseCleanupResult.cleanedUpGarbageLease();

                    if (leaseCleanupResult.leaseCleanedUp()) {
                        log.info("Successfully cleaned up lease {} for {} due to {}", leaseKey, streamIdentifier, leaseCleanupResult);
                        deletionSucceeded = true;
                    } else {
                        log.warn("Unable to clean up lease {} for {} due to {}", leaseKey, streamIdentifier, leaseCleanupResult);
                    }
                } catch (Exception e) {
                    log.error("Failed to cleanup lease {} for {}. Will re-enqueue for deletion and retry on next " +
                            "scheduled execution.", leaseKey, streamIdentifier, e);
                }
                if (!deletionSucceeded) {
                    log.debug("Did not cleanup lease {} for {}. Re-enqueueing for deletion.", leaseKey, streamIdentifier);
                    failedDeletions.add(leasePendingDeletion);
                }
            }
            if (completedLeaseCleanedUp) {
                log.debug("At least one completed lease was cleaned up - restarting interval");
                completedLeaseStopwatch.reset().start();
            }
            if (garbageLeaseCleanedUp) {
                log.debug("At least one garbage lease was cleaned up - restarting interval");
                garbageLeaseStopwatch.reset().start();
            }
            deletionQueue.addAll(failedDeletions);

            log.info("Number of pending leases to clean after the scan : {}", leasesPendingDeletion());
        }
    }

    private class LeaseCleanupThread implements Runnable {
        @Override
        public void run() {
            cleanupLeases();
        }
    }

    @Value
    public static class LeaseCleanupResult {
        boolean cleanedUpCompletedLease;
        boolean cleanedUpGarbageLease;
        boolean wereChildShardsPresent;
        boolean wasResourceNotFound;

        public boolean leaseCleanedUp() {
            return cleanedUpCompletedLease | cleanedUpGarbageLease;
        }
    }
}
