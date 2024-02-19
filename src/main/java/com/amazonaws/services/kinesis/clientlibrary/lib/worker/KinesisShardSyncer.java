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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.kinesis.model.ChildShard;
import com.amazonaws.services.kinesis.model.ShardFilter;
import com.amazonaws.services.kinesis.model.ShardFilterType;
import com.amazonaws.util.CollectionUtils;
import lombok.NoArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang3.StringUtils;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.KinesisClientLibIOException;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.Shard;

import static com.amazonaws.services.kinesis.leases.impl.HashKeyRangeForLease.fromHashKeyRange;

/**
 * Helper class to sync leases with shards of the Kinesis stream.
 * It will create new leases/activities when it discovers new Kinesis shards (bootstrap/resharding).
 * It deletes leases for shards that have been trimmed from Kinesis, or if we've completed processing it
 * and begun processing it's child shards.
 */
class KinesisShardSyncer implements ShardSyncer {

    private static final Log LOG = LogFactory.getLog(KinesisShardSyncer.class);
    private final LeaseCleanupValidator leaseCleanupValidator;

    public KinesisShardSyncer(final LeaseCleanupValidator leaseCleanupValidator) {
        this.leaseCleanupValidator = leaseCleanupValidator;
    }

    synchronized void bootstrapShardLeases(IKinesisProxy kinesisProxy, ILeaseManager<KinesisClientLease> leaseManager,
                                           InitialPositionInStreamExtended initialPositionInStream,
                                           boolean ignoreUnexpectedChildShards)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException {
        syncShardLeases(kinesisProxy, leaseManager, initialPositionInStream,
                ignoreUnexpectedChildShards);
    }

    /**
     * Check and create leases for any new shards (e.g. following a reshard operation).
     *
     * @param kinesisProxy
     * @param leaseManager
     * @param initialPositionInStream
     * @param cleanupLeasesOfCompletedShards
     * @param ignoreUnexpectedChildShards
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    @Override
    public synchronized void checkAndCreateLeasesForNewShards(IKinesisProxy kinesisProxy, ILeaseManager<KinesisClientLease> leaseManager,
                                          InitialPositionInStreamExtended initialPositionInStream, boolean cleanupLeasesOfCompletedShards,
                                          boolean ignoreUnexpectedChildShards)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException,
            KinesisClientLibIOException {
        syncShardLeases(kinesisProxy, leaseManager, initialPositionInStream, ignoreUnexpectedChildShards);
    }

    /**
     * Check and create leases for any new shards (e.g. following a reshard operation).
     *
     * @param kinesisProxy
     * @param leaseManager
     * @param initialPositionInStream
     * @param cleanupLeasesOfCompletedShards
     * @param ignoreUnexpectedChildShards
     * @param latestShards latest snapshot of shards to reuse
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    @Override
    public synchronized void checkAndCreateLeasesForNewShards(IKinesisProxy kinesisProxy,
            ILeaseManager<KinesisClientLease> leaseManager, InitialPositionInStreamExtended initialPositionInStream,
            boolean cleanupLeasesOfCompletedShards, boolean ignoreUnexpectedChildShards, List<Shard> latestShards)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException,
            KinesisClientLibIOException {
        syncShardLeases(kinesisProxy, leaseManager, initialPositionInStream,
                ignoreUnexpectedChildShards, latestShards, leaseManager.isLeaseTableEmpty());
    }

    /**
     * Sync leases with Kinesis shards (e.g. at startup, or when we reach end of a shard).
     *
     * @param kinesisProxy
     * @param leaseManager
     * @param initialPosition
     * @param ignoreUnexpectedChildShards
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    private synchronized void syncShardLeases(IKinesisProxy kinesisProxy,
                                              ILeaseManager<KinesisClientLease> leaseManager,
                                              InitialPositionInStreamExtended initialPosition,
                                              boolean ignoreUnexpectedChildShards)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException {

        // In the case where the lease table is empty, we want to synchronize the minimal amount of shards possible
        // based on the given initial position.
        // TODO: Implement shard list filtering on non-empty lease table case
        final boolean isLeaseTableEmpty = leaseManager.isLeaseTableEmpty();
        final List<Shard> latestShards = isLeaseTableEmpty
                ? getShardListAtInitialPosition(kinesisProxy, initialPosition)
                : getCompleteShardList(kinesisProxy);

        syncShardLeases(kinesisProxy, leaseManager, initialPosition,
                ignoreUnexpectedChildShards, latestShards, isLeaseTableEmpty);
    }

    /**
     * Sync leases with Kinesis shards (e.g. at startup, or when we reach end of a shard).
     *
     * @param kinesisProxy
     * @param leaseManager
     * @param initialPosition
     * @param ignoreUnexpectedChildShards
     * @param latestShards latest snapshot of shards to reuse
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    private synchronized void syncShardLeases(IKinesisProxy kinesisProxy,
                                              ILeaseManager<KinesisClientLease> leaseManager,
                                              InitialPositionInStreamExtended initialPosition,
                                              boolean ignoreUnexpectedChildShards,
                                              List<Shard> latestShards,
                                              boolean isLeaseTableEmpty)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException,
            KinesisClientLibIOException {

        List<Shard> shards;
        if(CollectionUtils.isNullOrEmpty(latestShards)) {
            shards = isLeaseTableEmpty ? getShardListAtInitialPosition(kinesisProxy, initialPosition) : getCompleteShardList(kinesisProxy);
        } else {
            shards = latestShards;
        }
        LOG.debug("Num Shards: " + shards.size());

        Map<String, Shard> shardIdToShardMap = constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap = constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> inconsistentShardIds = findInconsistentShardIds(shardIdToChildShardIdsMap, shardIdToShardMap);
        if (!ignoreUnexpectedChildShards) {
            assertAllParentShardsAreClosed(inconsistentShardIds);
        }

        // Determine which lease sync strategy to use based on the state of the lease table
        final LeaseSynchronizer leaseSynchronizer = isLeaseTableEmpty
                ? new EmptyLeaseTableSynchronizer()
                : new NonEmptyLeaseTableSynchronizer(shardIdToShardMap, shardIdToChildShardIdsMap);

        final List<KinesisClientLease> currentLeases = leaseManager.listLeases();
        final List<KinesisClientLease> newLeasesToCreate = determineNewLeasesToCreate(leaseSynchronizer, shards,
                currentLeases, initialPosition, inconsistentShardIds);
        LOG.debug("Num new leases to create: " + newLeasesToCreate.size());

        for (KinesisClientLease lease : newLeasesToCreate) {
            long startTimeMillis = System.currentTimeMillis();
            boolean success = false;
            try {
                leaseManager.createLeaseIfNotExists(lease);
                success = true;
            } finally {
                MetricsHelper.addSuccessAndLatency("CreateLease", startTimeMillis, success, MetricsLevel.DETAILED);
            }
        }

        List<KinesisClientLease> trackedLeases = new ArrayList<>();
        if (currentLeases != null) {
            trackedLeases.addAll(currentLeases);
        }
        trackedLeases.addAll(newLeasesToCreate);
    }
    // CHECKSTYLE:ON CyclomaticComplexity

    /** Helper method to detect a race condition between fetching the shards via paginated DescribeStream calls
     * and a reshard operation.
     * @param inconsistentShardIds
     * @throws KinesisClientLibIOException
     */
    private void assertAllParentShardsAreClosed(Set<String> inconsistentShardIds) throws KinesisClientLibIOException {
        if (!inconsistentShardIds.isEmpty()) {
            String ids = StringUtils.join(inconsistentShardIds, ' ');
            throw new KinesisClientLibIOException(String.format("%d open child shards (%s) are inconsistent. "
                            + "This can happen due to a race condition between describeStream and a reshard operation.",
                    inconsistentShardIds.size(), ids));
        }
    }

    /**
     * Helper method to construct the list of inconsistent shards, which are open shards with non-closed ancestor
     * parent(s).
     * @param shardIdToChildShardIdsMap
     * @param shardIdToShardMap
     * @return Set of inconsistent open shard ids for shards having open parents.
     */
    private Set<String> findInconsistentShardIds(Map<String, Set<String>> shardIdToChildShardIdsMap,
            Map<String, Shard> shardIdToShardMap) {
        Set<String> result = new HashSet<String>();
        for (String parentShardId : shardIdToChildShardIdsMap.keySet()) {
            Shard parentShard = shardIdToShardMap.get(parentShardId);
            if ((parentShardId == null) || (parentShard.getSequenceNumberRange().getEndingSequenceNumber() == null)) {
                Set<String> childShardIdsMap = shardIdToChildShardIdsMap.get(parentShardId);
                result.addAll(childShardIdsMap);
            }
        }
        return result;
    }

    /**
     * Helper method to create a shardId->KinesisClientLease map.
     * Note: This has package level access for testing purposes only.
     * @param trackedLeaseList
     * @return
     */
    Map<String, KinesisClientLease> constructShardIdToKCLLeaseMap(List<KinesisClientLease> trackedLeaseList) {
        Map<String, KinesisClientLease> trackedLeasesMap = new HashMap<>();
        for (KinesisClientLease lease : trackedLeaseList) {
            trackedLeasesMap.put(lease.getLeaseKey(), lease);
        }
        return trackedLeasesMap;
    }

    /**
     * Note: this has package level access for testing purposes.
     * Useful for asserting that we don't have an incomplete shard list following a reshard operation.
     * We verify that if the shard is present in the shard list, it is closed and its hash key range
     * is covered by its child shards.
     */
    synchronized void assertClosedShardsAreCoveredOrAbsent(Map<String, Shard> shardIdToShardMap,
            Map<String, Set<String>> shardIdToChildShardIdsMap, Set<String> shardIdsOfClosedShards)
            throws KinesisClientLibIOException {
        String exceptionMessageSuffix = "This can happen if we constructed the list of shards "
                + " while a reshard operation was in progress.";

        for (String shardId : shardIdsOfClosedShards) {
            Shard shard = shardIdToShardMap.get(shardId);
            if (shard == null) {
                LOG.info("Shard " + shardId + " is not present in Kinesis anymore.");
                continue;
            }

            String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
            if (endingSequenceNumber == null) {
                throw new KinesisClientLibIOException("Shard " + shardIdsOfClosedShards + " is not closed. "
                        + exceptionMessageSuffix);
            }

            Set<String> childShardIds = shardIdToChildShardIdsMap.get(shardId);
            if (childShardIds == null) {
                throw new KinesisClientLibIOException("Incomplete shard list: Closed shard " + shardId
                        + " has no children." + exceptionMessageSuffix);
            }

            assertHashRangeOfClosedShardIsCovered(shard, shardIdToShardMap, childShardIds);
        }
    }

    private synchronized void assertHashRangeOfClosedShardIsCovered(Shard closedShard,
            Map<String, Shard> shardIdToShardMap, Set<String> childShardIds) throws KinesisClientLibIOException {

        BigInteger startingHashKeyOfClosedShard = new BigInteger(closedShard.getHashKeyRange().getStartingHashKey());
        BigInteger endingHashKeyOfClosedShard = new BigInteger(closedShard.getHashKeyRange().getEndingHashKey());
        BigInteger minStartingHashKeyOfChildren = null;
        BigInteger maxEndingHashKeyOfChildren = null;

        for (String childShardId : childShardIds) {
            Shard childShard = shardIdToShardMap.get(childShardId);
            BigInteger startingHashKey = new BigInteger(childShard.getHashKeyRange().getStartingHashKey());
            if ((minStartingHashKeyOfChildren == null) || (startingHashKey.compareTo(minStartingHashKeyOfChildren)
                    < 0)) {
                minStartingHashKeyOfChildren = startingHashKey;
            }
            BigInteger endingHashKey = new BigInteger(childShard.getHashKeyRange().getEndingHashKey());
            if ((maxEndingHashKeyOfChildren == null) || (endingHashKey.compareTo(maxEndingHashKeyOfChildren) > 0)) {
                maxEndingHashKeyOfChildren = endingHashKey;
            }
        }

        if ((minStartingHashKeyOfChildren == null) || (maxEndingHashKeyOfChildren == null) || (
                minStartingHashKeyOfChildren.compareTo(startingHashKeyOfClosedShard) > 0) || (
                maxEndingHashKeyOfChildren.compareTo(endingHashKeyOfClosedShard) < 0)) {
            throw new KinesisClientLibIOException("Incomplete shard list: hash key range of shard " + closedShard
                    .getShardId() + " is not covered by its child shards.");
        }

    }

    /**
     * Helper method to construct shardId->setOfChildShardIds map.
     * Note: This has package access for testing purposes only.
     * @param shardIdToShardMap
     * @return
     */
    static Map<String, Set<String>> constructShardIdToChildShardIdsMap(Map<String, Shard> shardIdToShardMap) {
        Map<String, Set<String>> shardIdToChildShardIdsMap = new HashMap<>();
        for (Map.Entry<String, Shard> entry : shardIdToShardMap.entrySet()) {
            String shardId = entry.getKey();
            Shard shard = entry.getValue();
            String parentShardId = shard.getParentShardId();
            if ((parentShardId != null) && (shardIdToShardMap.containsKey(parentShardId))) {
                Set<String> childShardIds = shardIdToChildShardIdsMap.get(parentShardId);
                if (childShardIds == null) {
                    childShardIds = new HashSet<String>();
                    shardIdToChildShardIdsMap.put(parentShardId, childShardIds);
                }
                childShardIds.add(shardId);
            }

            String adjacentParentShardId = shard.getAdjacentParentShardId();
            if ((adjacentParentShardId != null) && (shardIdToShardMap.containsKey(adjacentParentShardId))) {
                Set<String> childShardIds = shardIdToChildShardIdsMap.get(adjacentParentShardId);
                if (childShardIds == null) {
                    childShardIds = new HashSet<String>();
                    shardIdToChildShardIdsMap.put(adjacentParentShardId, childShardIds);
                }
                childShardIds.add(shardId);
            }
        }
        return shardIdToChildShardIdsMap;
    }

    private List<Shard> getCompleteShardList(IKinesisProxy kinesisProxy) throws KinesisClientLibIOException {
        List<Shard> shards = kinesisProxy.getShardList();
        if (shards == null) {
            throw new KinesisClientLibIOException(
                    "Stream is not in ACTIVE OR UPDATING state - will retry getting the shard list.");
        }
        return shards;
    }

    private List<Shard> getShardListAtInitialPosition(IKinesisProxy kinesisProxy,
                                                      InitialPositionInStreamExtended initialPosition)
            throws KinesisClientLibIOException {

        final ShardFilter shardFilter = getShardFilterAtInitialPosition(initialPosition);
        final List<Shard> shards = kinesisProxy.getShardListWithFilter(shardFilter);

        if (shards == null) {
            throw new KinesisClientLibIOException(
                    "Stream is not in ACTIVE OR UPDATING state - will retry getting the shard list.");
        }

        return shards;
    }

    private static ShardFilter getShardFilterAtInitialPosition(InitialPositionInStreamExtended initialPosition) {
        ShardFilter shardFilter = new ShardFilter();

        switch (initialPosition.getInitialPositionInStream()) {
            case LATEST:
                shardFilter = shardFilter.withType(ShardFilterType.AT_LATEST);
                break;
            case TRIM_HORIZON:
                shardFilter = shardFilter.withType(ShardFilterType.AT_TRIM_HORIZON);
                break;
            case AT_TIMESTAMP:
                shardFilter = shardFilter.withType(ShardFilterType.AT_TIMESTAMP)
                        .withTimestamp(initialPosition.getTimestamp());
                break;
            default:
                throw new IllegalArgumentException(initialPosition.getInitialPositionInStream()
                        + " is not a supported initial position in a Kinesis stream. Supported initial positions are"
                        + " AT_LATEST, AT_TRIM_HORIZON, and AT_TIMESTAMP.");
        }

        return shardFilter;
    }

    /**
     * Determine new leases to create and their initial checkpoint.
     * Note: Package level access only for testing purposes.
     *
     * @param leaseSynchronizer determines the strategy to use when updating leases based on the current state of
     *        the lease table (empty vs. non-empty)
     * @param shards List of all shards in Kinesis (we'll create new leases based on this set)
     * @param currentLeases List of current leases
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @param inconsistentShardIds Set of child shard ids having open parents.
     * @return List of new leases to create sorted by starting sequenceNumber of the corresponding shard
     */
    List<KinesisClientLease> determineNewLeasesToCreate(LeaseSynchronizer leaseSynchronizer,
                                                        List<Shard> shards,
                                                        List<KinesisClientLease> currentLeases,
                                                        InitialPositionInStreamExtended initialPosition,
                                                        Set<String> inconsistentShardIds) {

        return leaseSynchronizer.determineNewLeasesToCreate(shards, currentLeases, initialPosition,
                inconsistentShardIds);
    }

    /**
     * Determine new leases to create and their initial checkpoint.
     * Note: Package level access only for testing purposes.
     */
    List<KinesisClientLease> determineNewLeasesToCreate(LeaseSynchronizer leaseSynchronizer,
                                                        List<Shard> shards,
                                                        List<KinesisClientLease> currentLeases,
                                                        InitialPositionInStreamExtended initialPosition) {

        Set<String> inconsistentShardIds = new HashSet<String>();
        return determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, initialPosition, inconsistentShardIds);
    }

    /**
     * Note: Package level access for testing purposes only.
     * Check if this shard is a descendant of a shard that is (or will be) processed.
     * Create leases for the first ancestor of this shard that needs to be processed, as required.
     * See javadoc of determineNewLeasesToCreate() for rules and example.
     *
     * @param shardId The shardId to check.
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @param shardIdsOfCurrentLeases The shardIds for the current leases.
     * @param shardIdToShardMapOfAllKinesisShards ShardId->Shard map containing all shards obtained via DescribeStream.
     * @param shardIdToLeaseMapOfNewShards Add lease POJOs corresponding to ancestors to this map.
     * @param memoizationContext Memoization of shards that have been evaluated as part of the evaluation
     * @return true if the shard is a descendant of any current shard (lease already exists)
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    static boolean checkIfDescendantAndAddNewLeasesForAncestors(String shardId,
            InitialPositionInStreamExtended initialPosition, Set<String> shardIdsOfCurrentLeases,
            Map<String, Shard> shardIdToShardMapOfAllKinesisShards,
            Map<String, KinesisClientLease> shardIdToLeaseMapOfNewShards, MemoizationContext memoizationContext) {

        final Boolean previousValue = memoizationContext.isDescendant(shardId);

        if (previousValue != null) {
            return previousValue;
        }

        boolean isDescendant = false;
        Shard shard;
        Set<String> parentShardIds;
        Set<String> descendantParentShardIds = new HashSet<String>();

        if ((shardId != null) && (shardIdToShardMapOfAllKinesisShards.containsKey(shardId))) {
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                // This shard is a descendant of a current shard.
                isDescendant = true;
                // We don't need to add leases of its ancestors,
                // because we'd have done it when creating a lease for this shard.
            } else {
                shard = shardIdToShardMapOfAllKinesisShards.get(shardId);
                parentShardIds = getParentShardIds(shard, shardIdToShardMapOfAllKinesisShards);
                for (String parentShardId : parentShardIds) {
                    // Check if the parent is a descendant, and include its ancestors. Or, if the parent is NOT a
                    // descendant but we should create a lease for it anyway (e.g. to include in processing from
                    // TRIM_HORIZON or AT_TIMESTAMP). If either is true, then we mark the current shard as a descendant.
                    final boolean isParentDescendant = checkIfDescendantAndAddNewLeasesForAncestors(parentShardId,
                            initialPosition, shardIdsOfCurrentLeases, shardIdToShardMapOfAllKinesisShards,
                            shardIdToLeaseMapOfNewShards, memoizationContext);
                    if (isParentDescendant || memoizationContext.shouldCreateLease(parentShardId)) {
                        isDescendant = true;
                        descendantParentShardIds.add(parentShardId);
                        LOG.debug("Parent shard " + parentShardId + " is a descendant.");
                    } else {
                        LOG.debug("Parent shard " + parentShardId + " is NOT a descendant.");
                    }
                }

                // If this is a descendant, create leases for its parent shards (if they don't exist)
                if (isDescendant) {
                    for (String parentShardId : parentShardIds) {
                        if (!shardIdsOfCurrentLeases.contains(parentShardId)) {
                            KinesisClientLease lease = shardIdToLeaseMapOfNewShards.get(parentShardId);

                            // If the lease for the parent shard does not already exist, there are two cases in which we
                            // would want to create it:
                            // - If we have already marked the parentShardId for lease creation in a prior recursive
                            //   call. This could happen if we are trying to process from TRIM_HORIZON or AT_TIMESTAMP.
                            // - If the parent shard is not a descendant but the current shard is a descendant, then
                            //   the parent shard is the oldest shard in the shard hierarchy that does not have an
                            //   ancestor in the lease table (the adjacent parent is necessarily a descendant, and
                            //   therefore covered in the lease table). So we should create a lease for the parent.

                            if (lease == null) {
                                if (memoizationContext.shouldCreateLease(parentShardId) ||
                                        !descendantParentShardIds.contains(parentShardId)) {
                                    LOG.debug("Need to create a lease for shardId " + parentShardId);
                                    lease = newKCLLease(shardIdToShardMapOfAllKinesisShards.get(parentShardId));
                                    shardIdToLeaseMapOfNewShards.put(parentShardId, lease);
                                }
                            }

                            /**
                             * If the shard is a descendant and the specified initial position is AT_TIMESTAMP, then the
                             * checkpoint should be set to AT_TIMESTAMP, else to TRIM_HORIZON. For AT_TIMESTAMP, we will
                             * add a lease just like we do for TRIM_HORIZON. However we will only return back records
                             * with server-side timestamp at or after the specified initial position timestamp.
                             *
                             * Shard structure (each level depicts a stream segment):
                             * 0 1 2 3 4   5   - shards till epoch 102
                             * \ / \ / |   |
                             *  6   7  4   5   - shards from epoch 103 - 205
                             *   \ /   |  /\
                             *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
                             *
                             * Current leases: (4, 5, 7)
                             *
                             * For the above example, suppose the initial position in stream is set to AT_TIMESTAMP with
                             * timestamp value 206. We will then create new leases for all the shards 0 and 1 (with
                             * checkpoint set AT_TIMESTAMP), even though these ancestor shards have an epoch less than
                             * 206. However as we begin processing the ancestor shards, their checkpoints would be
                             * updated to SHARD_END and their leases would then be deleted since they won't have records
                             * with server-side timestamp at/after 206. And after that we will begin processing the
                             * descendant shards with epoch at/after 206 and we will return the records that meet the
                             * timestamp requirement for these shards.
                             */
                            if (lease != null) {
                                if (descendantParentShardIds.contains(parentShardId) && !initialPosition
                                        .getInitialPositionInStream().equals(InitialPositionInStream.AT_TIMESTAMP)) {
                                    lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                                } else {
                                    lease.setCheckpoint(convertToCheckpoint(initialPosition));
                                }
                            }
                        }
                    }
                } else {
                    // This shard is not a descendant, but should still be included if the customer wants to process all
                    // records in the stream or if the initial position is AT_TIMESTAMP. For AT_TIMESTAMP, we will add a
                    // lease just like we do for TRIM_HORIZON. However we will only return back records with server-side
                    // timestamp at or after the specified initial position timestamp.
                    if (initialPosition.getInitialPositionInStream().equals(InitialPositionInStream.TRIM_HORIZON)
                            || initialPosition.getInitialPositionInStream()
                            .equals(InitialPositionInStream.AT_TIMESTAMP)) {
                        memoizationContext.setShouldCreateLease(shardId, true);
                    }
                }

            }
        }

        memoizationContext.setIsDescendant(shardId, isDescendant);
        return isDescendant;
    }
    // CHECKSTYLE:ON CyclomaticComplexity

    /**
     * Helper method to get parent shardIds of the current shard - includes the parent shardIds if:
     * a/ they are not null
     * b/ if they exist in the current shard map (i.e. haven't expired)
     *
     * @param shard Will return parents of this shard
     * @param shardIdToShardMapOfAllKinesisShards ShardId->Shard map containing all shards obtained via DescribeStream.
     * @return Set of parentShardIds
     */
    static Set<String> getParentShardIds(Shard shard, Map<String, Shard> shardIdToShardMapOfAllKinesisShards) {
        Set<String> parentShardIds = new HashSet<String>(2);
        String parentShardId = shard.getParentShardId();
        if ((parentShardId != null) && shardIdToShardMapOfAllKinesisShards.containsKey(parentShardId)) {
            parentShardIds.add(parentShardId);
        }
        String adjacentParentShardId = shard.getAdjacentParentShardId();
        if ((adjacentParentShardId != null) && shardIdToShardMapOfAllKinesisShards.containsKey(adjacentParentShardId)) {
            parentShardIds.add(adjacentParentShardId);
        }
        return parentShardIds;
    }

    /**
     * Helper method to create a new KinesisClientLease POJO for a shard.
     * Note: Package level access only for testing purposes
     *
     * @param shard
     * @return
     */
    static KinesisClientLease newKCLLease(Shard shard) {
        KinesisClientLease newLease = new KinesisClientLease();
        newLease.setLeaseKey(shard.getShardId());
        List<String> parentShardIds = new ArrayList<String>(2);
        if (shard.getParentShardId() != null) {
            parentShardIds.add(shard.getParentShardId());
        }
        if (shard.getAdjacentParentShardId() != null) {
            parentShardIds.add(shard.getAdjacentParentShardId());
        }
        newLease.setParentShardIds(parentShardIds);
        newLease.setOwnerSwitchesSinceCheckpoint(0L);
        newLease.setHashKeyRange(fromHashKeyRange(shard.getHashKeyRange()));
        return newLease;
    }

    /**
     * Helper method to create a new KinesisClientLease POJO for a ChildShard.
     * Note: Package level access only for testing purposes
     *
     * @param childShard
     * @return
     */
    static KinesisClientLease newKCLLeaseForChildShard(ChildShard childShard) throws InvalidStateException {
        final KinesisClientLease newLease = new KinesisClientLease();
        newLease.setLeaseKey(childShard.getShardId());
        final List<String> parentShardIds = new ArrayList<>();
        if (!CollectionUtils.isNullOrEmpty(childShard.getParentShards())) {
            parentShardIds.addAll(childShard.getParentShards());
        } else {
            throw new InvalidStateException("Unable to populate new lease for child shard " + childShard.getShardId()
            + " because parent shards cannot be found.");
        }
        newLease.setParentShardIds(parentShardIds);
        newLease.setOwnerSwitchesSinceCheckpoint(0L);
        newLease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        newLease.setHashKeyRange(fromHashKeyRange(childShard.getHashKeyRange()));
        return newLease;
    }

    /**
     * Helper method to construct a shardId->Shard map for the specified list of shards.
     *
     * @param shards List of shards
     * @return ShardId->Shard map
     */
    static Map<String, Shard> constructShardIdToShardMap(List<Shard> shards) {
        Map<String, Shard> shardIdToShardMap = new HashMap<String, Shard>();
        for (Shard shard : shards) {
            shardIdToShardMap.put(shard.getShardId(), shard);
        }
        return shardIdToShardMap;
    }

    /**
     * Helper method to return all the open shards for a stream.
     * Note: Package level access only for testing purposes.
     *
     * @param allShards All shards returved via DescribeStream. We assume this to represent a consistent shard list.
     * @return List of open shards (shards at the tip of the stream) - may include shards that are not yet active.
     */
    static List<Shard> getOpenShards(List<Shard> allShards) {
        List<Shard> openShards = new ArrayList<Shard>();
        for (Shard shard : allShards) {
            String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
            if (endingSequenceNumber == null) {
                openShards.add(shard);
                LOG.debug("Found open shard: " + shard.getShardId());
            }
        }
        return openShards;
    }

    static ExtendedSequenceNumber convertToCheckpoint(InitialPositionInStreamExtended position) {
        ExtendedSequenceNumber checkpoint = null;

        if (position.getInitialPositionInStream().equals(InitialPositionInStream.TRIM_HORIZON)) {
            checkpoint = ExtendedSequenceNumber.TRIM_HORIZON;
        } else if (position.getInitialPositionInStream().equals(InitialPositionInStream.LATEST)) {
            checkpoint = ExtendedSequenceNumber.LATEST;
        } else if (position.getInitialPositionInStream().equals(InitialPositionInStream.AT_TIMESTAMP)) {
            checkpoint = ExtendedSequenceNumber.AT_TIMESTAMP;
        }

        return checkpoint;
    }

    /** Helper class to compare leases based on starting sequence number of the corresponding shards.
     *
     */
    static class StartingSequenceNumberAndShardIdBasedComparator implements Comparator<KinesisClientLease>,
            Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<String, Shard> shardIdToShardMap;

        /**
         * @param shardIdToShardMapOfAllKinesisShards
         */
        public StartingSequenceNumberAndShardIdBasedComparator(Map<String, Shard> shardIdToShardMapOfAllKinesisShards) {
            shardIdToShardMap = shardIdToShardMapOfAllKinesisShards;
        }

        /**
         * Compares two leases based on the starting sequence number of corresponding shards.
         * If shards are not found in the shardId->shard map supplied, we do a string comparison on the shardIds.
         * We assume that lease1 and lease2 are:
         *     a/ not null,
         *     b/ shards (if found) have non-null starting sequence numbers
         *
         * {@inheritDoc}
         */
        @Override
        public int compare(KinesisClientLease lease1, KinesisClientLease lease2) {
            int result = 0;
            String shardId1 = lease1.getLeaseKey();
            String shardId2 = lease2.getLeaseKey();
            Shard shard1 = shardIdToShardMap.get(shardId1);
            Shard shard2 = shardIdToShardMap.get(shardId2);

            // If we found shards for the two leases, use comparison of the starting sequence numbers
            if ((shard1 != null) && (shard2 != null)) {
                BigInteger sequenceNumber1 = new BigInteger(
                        shard1.getSequenceNumberRange().getStartingSequenceNumber());
                BigInteger sequenceNumber2 = new BigInteger(
                        shard2.getSequenceNumberRange().getStartingSequenceNumber());
                result = sequenceNumber1.compareTo(sequenceNumber2);
            }

            if (result == 0) {
                result = shardId1.compareTo(shardId2);
            }

            return result;
        }

    }

    /**
     * Helper class to pass around state between recursive traversals of shard hierarchy.
     */
    @NoArgsConstructor
    static class MemoizationContext {
        private Map<String, Boolean> isDescendantMap = new HashMap<>();
        private Map<String, Boolean> shouldCreateLeaseMap = new HashMap<>();

        Boolean isDescendant(String shardId) {
            return isDescendantMap.get(shardId);
        }

        void setIsDescendant(String shardId, Boolean isDescendant) {
            isDescendantMap.put(shardId, isDescendant);
        }

        Boolean shouldCreateLease(String shardId) {
            return shouldCreateLeaseMap.computeIfAbsent(shardId, x -> Boolean.FALSE);
        }

        void setShouldCreateLease(String shardId, Boolean shouldCreateLease) {
            shouldCreateLeaseMap.put(shardId, shouldCreateLease);
        }
    }
}
