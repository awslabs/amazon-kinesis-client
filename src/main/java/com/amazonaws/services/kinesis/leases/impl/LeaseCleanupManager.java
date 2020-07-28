package com.amazonaws.services.kinesis.leases.impl;

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

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.LeasePendingDeletion;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.util.CollectionUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Helper class to cleanup of any expired/closed shard leases. It will cleanup leases periodically as defined by
 * {@link KinesisClientLibConfiguration#leaseCleanupIntervalMillis()} upon worker shutdown, following a re-shard event or
 * a shard expiring from the service.
 */
@RequiredArgsConstructor(access= AccessLevel.PACKAGE)
@EqualsAndHashCode
public class LeaseCleanupManager {
    @NonNull
    private IKinesisProxy kinesisProxy;
    @NonNull
    private final ILeaseManager<KinesisClientLease> leaseManager;
    @NonNull
    private final ScheduledExecutorService deletionThreadPool;
    @NonNull
    private final IMetricsFactory metricsFactory;
    private final boolean cleanupLeasesUponShardCompletion;
    private final long leaseCleanupIntervalMillis;
    private final long completedLeaseCleanupIntervalMillis;
    private final long garbageLeaseCleanupIntervalMillis;
    private final int maxRecords;

    private final Stopwatch completedLeaseStopwatch = Stopwatch.createUnstarted();
    private final Stopwatch garbageLeaseStopwatch = Stopwatch.createUnstarted();
    private final Queue<LeasePendingDeletion> deletionQueue = new ConcurrentLinkedQueue<>();

    private static final long INITIAL_DELAY = 0L;
    private static final Log LOG = LogFactory.getLog(LeaseCleanupManager.class);

    @Getter
    private volatile boolean isRunning = false;

    private static LeaseCleanupManager instance;

    /**
     * Factory method to return a singleton instance of {@link LeaseCleanupManager}.
     * @param kinesisProxy
     * @param leaseManager
     * @param deletionThreadPool
     * @param metricsFactory
     * @param cleanupLeasesUponShardCompletion
     * @param leaseCleanupIntervalMillis
     * @param completedLeaseCleanupIntervalMillis
     * @param garbageLeaseCleanupIntervalMillis
     * @param maxRecords
     * @return
     */
    public static LeaseCleanupManager createOrGetInstance(IKinesisProxy kinesisProxy, ILeaseManager leaseManager,
                                                          ScheduledExecutorService deletionThreadPool, IMetricsFactory metricsFactory,
                                                          boolean cleanupLeasesUponShardCompletion, long leaseCleanupIntervalMillis,
                                                          long completedLeaseCleanupIntervalMillis, long garbageLeaseCleanupIntervalMillis,
                                                          int maxRecords) {
        if (instance == null) {
            instance = new LeaseCleanupManager(kinesisProxy, leaseManager, deletionThreadPool, metricsFactory, cleanupLeasesUponShardCompletion,
                    leaseCleanupIntervalMillis, completedLeaseCleanupIntervalMillis, garbageLeaseCleanupIntervalMillis, maxRecords);
        }

        return instance;
    }

    /**
     * Starts the lease cleanup thread, which is scheduled periodically as specified by
     * {@link LeaseCleanupManager#leaseCleanupIntervalMillis}
     */
    public void start() {
        if (!isRunning) {
            LOG.info("Starting lease cleanup thread.");
            completedLeaseStopwatch.start();
            garbageLeaseStopwatch.start();
            deletionThreadPool.scheduleAtFixedRate(new LeaseCleanupThread(), INITIAL_DELAY, leaseCleanupIntervalMillis,
                                                   TimeUnit.MILLISECONDS);
            isRunning = true;
        } else {
            LOG.info("Lease cleanup thread already running, no need to start.");
        }
    }

    /**
     * Enqueues a lease for deletion without check for duplicate entry. Use {@link #isEnqueuedForDeletion}
     * for checking the duplicate entries.
     * @param leasePendingDeletion
     */
    public void enqueueForDeletion(LeasePendingDeletion leasePendingDeletion) {
        final KinesisClientLease lease = leasePendingDeletion.lease();
        if (lease == null) {
            LOG.warn("Cannot enqueue lease " + lease.getLeaseKey() + " for deferred deletion - instance doesn't hold " +
                    "the lease for that shard.");
        } else {
            LOG.debug("Enqueuing lease " + lease.getLeaseKey() + " for deferred deletion.");
            if (!deletionQueue.add(leasePendingDeletion)) {
                LOG.warn("Unable to enqueue lease " + lease.getLeaseKey() + " for deletion.");
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

    private boolean timeToCheckForCompletedShard() {
        return completedLeaseStopwatch.elapsed(TimeUnit.MILLISECONDS) >= completedLeaseCleanupIntervalMillis;
    }

    private boolean timeToCheckForGarbageShard() {
        return garbageLeaseStopwatch.elapsed(TimeUnit.MILLISECONDS) >= garbageLeaseCleanupIntervalMillis;
    }

    public LeaseCleanupResult cleanupLease(LeasePendingDeletion leasePendingDeletion,
                                           boolean timeToCheckForCompletedShard, boolean timeToCheckForGarbageShard)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        final KinesisClientLease lease = leasePendingDeletion.lease();
        final ShardInfo shardInfo = leasePendingDeletion.shardInfo();

        boolean cleanedUpCompletedLease = false;
        boolean cleanedUpGarbageLease = false;
        boolean alreadyCheckedForGarbageCollection = false;
        boolean wereChildShardsPresent = false;
        boolean wasResourceNotFound = false;

        try {
            if (cleanupLeasesUponShardCompletion && timeToCheckForCompletedShard) {
                final KinesisClientLease leaseFromDDB = leaseManager.getLease(shardInfo.getShardId());
                if(leaseFromDDB != null) {
                    Set<String> childShardKeys = leaseFromDDB.getChildShardIds();
                    if (CollectionUtils.isNullOrEmpty(childShardKeys)) {
                        try {
                            childShardKeys = getChildShardsFromService(shardInfo);

                            if (CollectionUtils.isNullOrEmpty(childShardKeys)) {
                                LOG.error("No child shards returned from service for shard " + shardInfo.getShardId());
                            } else {
                                wereChildShardsPresent = true;
                                updateLeaseWithChildShards(leasePendingDeletion, childShardKeys);
                            }
                        } catch (ResourceNotFoundException e) {
                            throw e;
                        } finally {
                            alreadyCheckedForGarbageCollection = true;
                        }
                    } else {
                        wereChildShardsPresent = true;
                    }
                    try {
                        cleanedUpCompletedLease = cleanupLeaseForCompletedShard(lease, childShardKeys);
                    } catch (Exception e) {
                        // Suppressing the exception here, so that we can attempt for garbage cleanup.
                        LOG.warn("Unable to cleanup lease for shard " + shardInfo.getShardId());
                    }
                } else {
                    LOG.info("Lease not present in lease table while cleaning the shard " + shardInfo.getShardId());
                    cleanedUpCompletedLease = true;
                }
            }

            if (!alreadyCheckedForGarbageCollection && timeToCheckForGarbageShard) {
                try {
                    wereChildShardsPresent = !CollectionUtils
                            .isNullOrEmpty(getChildShardsFromService(shardInfo));
                } catch (ResourceNotFoundException e) {
                    throw e;
                }
            }
        } catch (ResourceNotFoundException e) {
            wasResourceNotFound = true;
            cleanedUpGarbageLease = cleanupLeaseForGarbageShard(lease);
        }

        return new LeaseCleanupResult(cleanedUpCompletedLease, cleanedUpGarbageLease, wereChildShardsPresent,
                wasResourceNotFound);
    }

    private Set<String> getChildShardsFromService(ShardInfo shardInfo) {
        final String iterator = kinesisProxy.getIterator(shardInfo.getShardId(), ShardIteratorType.LATEST.toString());
        return kinesisProxy.get(iterator, maxRecords).getChildShards().stream().map(c -> c.getShardId()).collect(Collectors.toSet());
    }


    // A lease that ended with SHARD_END from ResourceNotFoundException is safe to delete if it no longer exists in the
    // stream (known explicitly from ResourceNotFound being thrown when processing this shard),
    private boolean cleanupLeaseForGarbageShard(KinesisClientLease lease) throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        LOG.info("Deleting lease " + lease.getLeaseKey() + " as it is not present in the stream.");
        leaseManager.deleteLease(lease);
        return true;
    }

    private boolean allParentShardLeasesDeleted(KinesisClientLease lease) throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        for (String parentShard : lease.getParentShardIds()) {
            final KinesisClientLease parentLease = leaseManager.getLease(parentShard);

            if (parentLease != null) {
                LOG.warn("Lease " + lease.getLeaseKey() + " has a parent lease " + parentLease.getLeaseKey() +
                        " which is still present in the lease table, skipping deletion for this lease.");
                return false;
            }
        }
        return true;
    }

    // We should only be deleting the current shard's lease if
    // 1. All of its children are currently being processed, i.e their checkpoint is not TRIM_HORIZON or AT_TIMESTAMP.
    // 2. Its parent shard lease(s) have already been deleted.
    private boolean cleanupLeaseForCompletedShard(KinesisClientLease lease, Set<String> childShardLeaseKeys)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException, IllegalStateException {
        final Set<String> processedChildShardLeaseKeys = new HashSet<>();

        for (String childShardLeaseKey : childShardLeaseKeys) {
            final KinesisClientLease childShardLease = Optional.ofNullable(
                    leaseManager.getLease(childShardLeaseKey))
                    .orElseThrow(() -> new IllegalStateException(
                            "Child lease " + childShardLeaseKey + " for completed shard not found in "
                                    + "lease table - not cleaning up lease " + lease));

            if (!childShardLease.getCheckpoint().equals(ExtendedSequenceNumber.TRIM_HORIZON) && !childShardLease
                    .getCheckpoint().equals(ExtendedSequenceNumber.AT_TIMESTAMP)) {
                processedChildShardLeaseKeys.add(childShardLease.getLeaseKey());
            }
        }

        if (!allParentShardLeasesDeleted(lease) || !Objects.equals(childShardLeaseKeys, processedChildShardLeaseKeys)) {
            return false;
        }

        LOG.info("Deleting lease " + lease.getLeaseKey() + " as it has been completely processed and processing of child shard(s) has begun.");
        leaseManager.deleteLease(lease);

        return true;
    }

    private void updateLeaseWithChildShards(LeasePendingDeletion leasePendingDeletion, Set<String> childShardKeys)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        final KinesisClientLease updatedLease = leasePendingDeletion.lease();
        updatedLease.setChildShardIds(childShardKeys);

        leaseManager.updateLease(updatedLease);
    }

    @VisibleForTesting
    void cleanupLeases() {
        LOG.info("Number of pending leases to clean before the scan : " + leasesPendingDeletion());
        if (deletionQueue.isEmpty()) {
            LOG.debug("No leases pending deletion.");
        } else if (timeToCheckForCompletedShard() | timeToCheckForGarbageShard()) {
            final Queue<LeasePendingDeletion> failedDeletions = new ConcurrentLinkedQueue<>();
            boolean completedLeaseCleanedUp = false;
            boolean garbageLeaseCleanedUp = false;

            LOG.debug("Attempting to clean up " + deletionQueue.size() + " lease(s).");

            while (!deletionQueue.isEmpty()) {
                final LeasePendingDeletion leasePendingDeletion = deletionQueue.poll();
                final String leaseKey = leasePendingDeletion.lease().getLeaseKey();
                boolean deletionSucceeded = false;
                try {
                    final LeaseCleanupResult leaseCleanupResult = cleanupLease(leasePendingDeletion,
                            timeToCheckForCompletedShard(), timeToCheckForGarbageShard());
                    completedLeaseCleanedUp |= leaseCleanupResult.cleanedUpCompletedLease();
                    garbageLeaseCleanedUp |= leaseCleanupResult.cleanedUpGarbageLease();

                    if (leaseCleanupResult.leaseCleanedUp()) {
                        LOG.debug("Successfully cleaned up lease " + leaseKey);
                        deletionSucceeded = true;
                    } else {
                        LOG.warn("Unable to clean up lease " + leaseKey + " due to " + leaseCleanupResult);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to cleanup lease " + leaseKey + ". Will re-enqueue for deletion and retry on next " +
                            "scheduled execution.", e);
                }
                if (!deletionSucceeded) {
                    LOG.debug("Did not cleanup lease " + leaseKey + ". Re-enqueueing for deletion.");
                    failedDeletions.add(leasePendingDeletion);
                }
            }
            if (completedLeaseCleanedUp) {
                LOG.debug("At least one completed lease was cleaned up - restarting interval");
                completedLeaseStopwatch.reset().start();
            }
            if (garbageLeaseCleanedUp) {
                LOG.debug("At least one garbage lease was cleaned up - restarting interval");
                garbageLeaseStopwatch.reset().start();
            }
            deletionQueue.addAll(failedDeletions);

            LOG.info("Number of pending leases to clean after the scan : " +  leasesPendingDeletion());
        }
    }

    private class LeaseCleanupThread implements Runnable {
        @Override
        public void run() {
            cleanupLeases();
        }
    }

    @Value
    @Accessors(fluent=true)
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
