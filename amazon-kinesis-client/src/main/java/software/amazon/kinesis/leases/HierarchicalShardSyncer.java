/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.leases;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Helper class to sync leases with shards of the Kinesis stream.
 * It will create new leases/activities when it discovers new Kinesis shards (bootstrap/resharding).
 * It deletes leases for shards that have been trimmed from Kinesis, or if we've completed processing it
 * and begun processing it's child shards.
 */
@Slf4j
@KinesisClientInternalApi
public class HierarchicalShardSyncer {

    /**
     * Check and create leases for any new shards (e.g. following a reshard operation). Sync leases with Kinesis shards
     * (e.g. at startup, or when we reach end of a shard).
     *
     * @param shardDetector
     * @param leaseRefresher
     * @param initialPosition
     * @param cleanupLeasesOfCompletedShards
     * @param ignoreUnexpectedChildShards
     * @param scope
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    public synchronized void checkAndCreateLeaseForNewShards(@NonNull final ShardDetector shardDetector,
            final LeaseRefresher leaseRefresher, final InitialPositionInStreamExtended initialPosition,
            final boolean cleanupLeasesOfCompletedShards, final boolean ignoreUnexpectedChildShards,
            final MetricsScope scope) throws DependencyException, InvalidStateException,
            ProvisionedThroughputException, KinesisClientLibIOException {
        final List<Shard> shards = getShardList(shardDetector);
        log.debug("Num shards: {}", shards.size());

        final Map<String, Shard> shardIdToShardMap = constructShardIdToShardMap(shards);
        final Map<String, Set<String>> shardIdToChildShardIdsMap = constructShardIdToChildShardIdsMap(
                shardIdToShardMap);
        final Set<String> inconsistentShardIds = findInconsistentShardIds(shardIdToChildShardIdsMap, shardIdToShardMap);
        if (!ignoreUnexpectedChildShards) {
            assertAllParentShardsAreClosed(inconsistentShardIds);
        }

        final List<Lease> currentLeases = leaseRefresher.listLeases();

        final List<Lease> newLeasesToCreate = determineNewLeasesToCreate(shards, currentLeases, initialPosition,
                inconsistentShardIds);
        log.debug("Num new leases to create: {}", newLeasesToCreate.size());
        for (Lease lease : newLeasesToCreate) {
            long startTime = System.currentTimeMillis();
            boolean success = false;
            try {
                leaseRefresher.createLeaseIfNotExists(lease);
                success = true;
            } finally {
                MetricsUtil.addSuccessAndLatency(scope, "CreateLease", success, startTime, MetricsLevel.DETAILED);
            }
        }
        
        final List<Lease> trackedLeases = new ArrayList<>(currentLeases);
        trackedLeases.addAll(newLeasesToCreate);
        cleanupGarbageLeases(shardDetector, shards, trackedLeases, leaseRefresher);
        if (cleanupLeasesOfCompletedShards) {
            cleanupLeasesOfFinishedShards(currentLeases, shardIdToShardMap, shardIdToChildShardIdsMap, trackedLeases,
                    leaseRefresher);
        }
    }
    // CHECKSTYLE:ON CyclomaticComplexity

    /** Helper method to detect a race condition between fetching the shards via paginated DescribeStream calls
     * and a reshard operation.
     * @param inconsistentShardIds
     * @throws KinesisClientLibIOException
     */
    private static void assertAllParentShardsAreClosed(final Set<String> inconsistentShardIds)
        throws KinesisClientLibIOException {
        if (!CollectionUtils.isNullOrEmpty(inconsistentShardIds)) {
            final String ids = StringUtils.join(inconsistentShardIds, ' ');
            throw new KinesisClientLibIOException(String.format(
                    "%d open child shards (%s) are inconsistent. This can happen due to a race condition between describeStream and a reshard operation.",
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
    private static Set<String> findInconsistentShardIds(final Map<String, Set<String>> shardIdToChildShardIdsMap,
            final Map<String, Shard> shardIdToShardMap) {
        return shardIdToChildShardIdsMap.entrySet().stream()
                .filter(entry -> entry.getKey() == null
                        || shardIdToShardMap.get(entry.getKey()).sequenceNumberRange().endingSequenceNumber() == null)
                .flatMap(entry -> shardIdToChildShardIdsMap.get(entry.getKey()).stream()).collect(Collectors.toSet());
    }

    /**
     * Note: this has package level access for testing purposes. 
     * Useful for asserting that we don't have an incomplete shard list following a reshard operation.
     * We verify that if the shard is present in the shard list, it is closed and its hash key range
     *     is covered by its child shards.
     * @param shardIdsOfClosedShards Id of the shard which is expected to be closed
     * @return ShardIds of child shards (children of the expectedClosedShard)
     * @throws KinesisClientLibIOException
     */
    synchronized void assertClosedShardsAreCoveredOrAbsent(final Map<String, Shard> shardIdToShardMap,
            final Map<String, Set<String>> shardIdToChildShardIdsMap, final Set<String> shardIdsOfClosedShards)
            throws KinesisClientLibIOException {
        final String exceptionMessageSuffix = "This can happen if we constructed the list of shards "
                        + " while a reshard operation was in progress.";
        
        for (String shardId : shardIdsOfClosedShards) {
            final Shard shard = shardIdToShardMap.get(shardId);
            if (shard == null) {
                log.info("Shard {} is not present in Kinesis anymore.", shardId);
                continue;
            }
            
            final String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
            if (endingSequenceNumber == null) {
                throw new KinesisClientLibIOException("Shard " + shardIdsOfClosedShards
                        + " is not closed. " + exceptionMessageSuffix);
            }

            final Set<String> childShardIds = shardIdToChildShardIdsMap.get(shardId);
            if (childShardIds == null) {
                throw new KinesisClientLibIOException("Incomplete shard list: Closed shard " + shardId
                        + " has no children." + exceptionMessageSuffix);
            }

            assertHashRangeOfClosedShardIsCovered(shard, shardIdToShardMap, childShardIds);
        }
    }

    private synchronized void assertHashRangeOfClosedShardIsCovered(final Shard closedShard,
            final Map<String, Shard> shardIdToShardMap, final Set<String> childShardIds)
            throws KinesisClientLibIOException {
        BigInteger minStartingHashKeyOfChildren = null;
        BigInteger maxEndingHashKeyOfChildren = null;

        final BigInteger startingHashKeyOfClosedShard = new BigInteger(closedShard.hashKeyRange().startingHashKey());
        final BigInteger endingHashKeyOfClosedShard = new BigInteger(closedShard.hashKeyRange().endingHashKey());

        for (String childShardId : childShardIds) {
            final Shard childShard = shardIdToShardMap.get(childShardId);
            final BigInteger startingHashKey = new BigInteger(childShard.hashKeyRange().startingHashKey());
            if (minStartingHashKeyOfChildren == null || startingHashKey.compareTo(minStartingHashKeyOfChildren) < 0) {
                minStartingHashKeyOfChildren = startingHashKey;
            }

            final BigInteger endingHashKey = new BigInteger(childShard.hashKeyRange().endingHashKey());
            if (maxEndingHashKeyOfChildren == null || endingHashKey.compareTo(maxEndingHashKeyOfChildren) > 0) {
                maxEndingHashKeyOfChildren = endingHashKey;
            }
        }
        
        if (minStartingHashKeyOfChildren == null || maxEndingHashKeyOfChildren == null
                || minStartingHashKeyOfChildren.compareTo(startingHashKeyOfClosedShard) > 0
                || maxEndingHashKeyOfChildren.compareTo(endingHashKeyOfClosedShard) < 0) {
            throw new KinesisClientLibIOException(String.format(
                    "Incomplete shard list: hash key range of shard %s is not covered by its child shards.",
                    closedShard.shardId()));
        }
        
    }
    
    /**
     * Helper method to construct shardId->setOfChildShardIds map.
     * Note: This has package access for testing purposes only.
     * @param shardIdToShardMap
     * @return
     */
    static Map<String, Set<String>> constructShardIdToChildShardIdsMap(final Map<String, Shard> shardIdToShardMap) {
        final Map<String, Set<String>> shardIdToChildShardIdsMap = new HashMap<>();

        for (final Map.Entry<String, Shard> entry : shardIdToShardMap.entrySet()) {
            final String shardId = entry.getKey();
            final Shard shard = entry.getValue();
            final String parentShardId = shard.parentShardId();
            if (parentShardId != null && shardIdToShardMap.containsKey(parentShardId)) {
                final Set<String> childShardIds = shardIdToChildShardIdsMap.computeIfAbsent(parentShardId,
                        key -> new HashSet<>());
                childShardIds.add(shardId);
            }

            final String adjacentParentShardId = shard.adjacentParentShardId();
            if (adjacentParentShardId != null && shardIdToShardMap.containsKey(adjacentParentShardId)) {
                final Set<String> childShardIds = shardIdToChildShardIdsMap.computeIfAbsent(adjacentParentShardId,
                        key -> new HashSet<>());
                childShardIds.add(shardId);
            }
        }
        return shardIdToChildShardIdsMap;
    }

    private static List<Shard> getShardList(@NonNull final ShardDetector shardDetector) throws KinesisClientLibIOException {
        final List<Shard> shards = shardDetector.listShards();
        if (shards == null) {
            throw new KinesisClientLibIOException(
                    "Stream is not in ACTIVE OR UPDATING state - will retry getting the shard list.");
        }
        return shards;
    }

    /**
     * Determine new leases to create and their initial checkpoint.
     * Note: Package level access only for testing purposes.
     *
     * For each open (no ending sequence number) shard without open parents that doesn't already have a lease,
     * determine if it is a descendent of any shard which is or will be processed (e.g. for which a lease exists):
     * If so, set checkpoint of the shard to TrimHorizon and also create leases for ancestors if needed.
     * If not, set checkpoint of the shard to the initial position specified by the client.
     * To check if we need to create leases for ancestors, we use the following rules:
     *   * If we began (or will begin) processing data for a shard, then we must reach end of that shard before
     *         we begin processing data from any of its descendants.
     *   * A shard does not start processing data until data from all its parents has been processed.
     * Note, if the initial position is LATEST and a shard has two parents and only one is a descendant - we'll create
     * leases corresponding to both the parents - the parent shard which is not a descendant will have  
     * its checkpoint set to Latest.
     * 
     * We assume that if there is an existing lease for a shard, then either:
     *   * we have previously created a lease for its parent (if it was needed), or
     *   * the parent shard has expired.
     * 
     * For example:
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5   - shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5   - shards from epoch 103 - 205
     *   \ /   |  / \
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     * New leases to create: (2, 6, 7, 8, 9, 10)
     * 
     * The leases returned are sorted by the starting sequence number - following the same order
     * when persisting the leases in DynamoDB will ensure that we recover gracefully if we fail
     * before creating all the leases.
     *
     * If a shard has no existing lease, is open, and is a descendant of a parent which is still open, we ignore it
     * here; this happens when the list of shards is inconsistent, which could be due to pagination delay for very
     * high shard count streams (i.e., dynamodb streams for tables with thousands of partitions).  This can only
     * currently happen here if ignoreUnexpectedChildShards was true in syncShardleases.
     *
     * 
     * @param shards List of all shards in Kinesis (we'll create new leases based on this set)
     * @param currentLeases List of current leases
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @param inconsistentShardIds Set of child shard ids having open parents.
     * @return List of new leases to create sorted by starting sequenceNumber of the corresponding shard
     */
    static List<Lease> determineNewLeasesToCreate(final List<Shard> shards, final List<Lease> currentLeases,
            final InitialPositionInStreamExtended initialPosition, final Set<String> inconsistentShardIds) {
        final Map<String, Lease> shardIdToNewLeaseMap = new HashMap<>();
        final Map<String, Shard> shardIdToShardMapOfAllKinesisShards = constructShardIdToShardMap(shards);

        final Set<String> shardIdsOfCurrentLeases = currentLeases.stream()
                .peek(lease -> log.debug("Existing lease: {}", lease)).map(Lease::leaseKey).collect(Collectors.toSet());

        final List<Shard> openShards = getOpenShards(shards);
        final Map<String, Boolean> memoizationContext = new HashMap<>();

        // Iterate over the open shards and find those that don't have any lease entries.
        for (Shard shard : openShards) {
            final String shardId = shard.shardId();
            log.debug("Evaluating leases for open shard {} and its ancestors.", shardId);
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                log.debug("Lease for shardId {} already exists. Not creating a lease", shardId);
            } else if (inconsistentShardIds.contains(shardId)) {
                log.info("shardId {} is an inconsistent child.  Not creating a lease", shardId);
            } else {
                log.debug("Need to create a lease for shardId {}", shardId);
                final Lease newLease = newKCLLease(shard);
                final boolean isDescendant = checkIfDescendantAndAddNewLeasesForAncestors(shardId, initialPosition,
                        shardIdsOfCurrentLeases, shardIdToShardMapOfAllKinesisShards, shardIdToNewLeaseMap,
                        memoizationContext);

                /**
                 * If the shard is a descendant and the specified initial position is AT_TIMESTAMP, then the
                 * checkpoint should be set to AT_TIMESTAMP, else to TRIM_HORIZON. For AT_TIMESTAMP, we will add a
                 * lease just like we do for TRIM_HORIZON. However we will only return back records with server-side
                 * timestamp at or after the specified initial position timestamp.
                 *
                 * Shard structure (each level depicts a stream segment):
                 * 0 1 2 3 4   5   - shards till epoch 102
                 * \ / \ / |   |
                 *  6   7  4   5   - shards from epoch 103 - 205
                 *   \ /   |  /\
                 *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
                 *
                 * Current leases: empty set
                 *
                 * For the above example, suppose the initial position in stream is set to AT_TIMESTAMP with
                 * timestamp value 206. We will then create new leases for all the shards (with checkpoint set to
                 * AT_TIMESTAMP), including the ancestor shards with epoch less than 206. However as we begin
                 * processing the ancestor shards, their checkpoints would be updated to SHARD_END and their leases
                 * would then be deleted since they won't have records with server-side timestamp at/after 206. And
                 * after that we will begin processing the descendant shards with epoch at/after 206 and we will
                 * return the records that meet the timestamp requirement for these shards.
                 */
                if (isDescendant
                        && !initialPosition.getInitialPositionInStream().equals(InitialPositionInStream.AT_TIMESTAMP)) {
                    newLease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                } else {
                    newLease.checkpoint(convertToCheckpoint(initialPosition));
                }
                log.debug("Set checkpoint of {} to {}", newLease.leaseKey(), newLease.checkpoint());
                shardIdToNewLeaseMap.put(shardId, newLease);
            }
        }

        final List<Lease> newLeasesToCreate = new ArrayList<>(shardIdToNewLeaseMap.values());
        final Comparator<Lease> startingSequenceNumberComparator = new StartingSequenceNumberAndShardIdBasedComparator(
                shardIdToShardMapOfAllKinesisShards);
        newLeasesToCreate.sort(startingSequenceNumberComparator);
        return newLeasesToCreate;
    }

    /**
     * Determine new leases to create and their initial checkpoint.
     * Note: Package level access only for testing purposes.
     */
    static List<Lease> determineNewLeasesToCreate(final List<Shard> shards, final List<Lease> currentLeases,
            final InitialPositionInStreamExtended initialPosition) {
        final Set<String> inconsistentShardIds = new HashSet<>();
        return determineNewLeasesToCreate(shards, currentLeases, initialPosition, inconsistentShardIds);
    }

    /**
     * Note: Package level access for testing purposes only.
     * Check if this shard is a descendant of a shard that is (or will be) processed.
     * Create leases for the ancestors of this shard as required.
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
    static boolean checkIfDescendantAndAddNewLeasesForAncestors(final String shardId,
            final InitialPositionInStreamExtended initialPosition, final Set<String> shardIdsOfCurrentLeases,
            final Map<String, Shard> shardIdToShardMapOfAllKinesisShards,
            final Map<String, Lease> shardIdToLeaseMapOfNewShards, final Map<String, Boolean> memoizationContext) {
        
        final Boolean previousValue = memoizationContext.get(shardId);
        if (previousValue != null) {
            return previousValue;
        }

        boolean isDescendant = false;
        final Set<String> descendantParentShardIds = new HashSet<>();

        if (shardId != null && shardIdToShardMapOfAllKinesisShards.containsKey(shardId)) {
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                // This shard is a descendant of a current shard.
                isDescendant = true;
                // We don't need to add leases of its ancestors,
                // because we'd have done it when creating a lease for this shard.
            } else {
                final Shard shard = shardIdToShardMapOfAllKinesisShards.get(shardId);
                final Set<String> parentShardIds = getParentShardIds(shard, shardIdToShardMapOfAllKinesisShards);
                for (String parentShardId : parentShardIds) {
                    // Check if the parent is a descendant, and include its ancestors.
                    if (checkIfDescendantAndAddNewLeasesForAncestors(parentShardId, initialPosition,
                            shardIdsOfCurrentLeases, shardIdToShardMapOfAllKinesisShards, shardIdToLeaseMapOfNewShards,
                            memoizationContext)) {
                        isDescendant = true;
                        descendantParentShardIds.add(parentShardId);
                        log.debug("Parent shard {} is a descendant.", parentShardId);
                    } else {
                        log.debug("Parent shard {} is NOT a descendant.", parentShardId);
                    }
                }

                // If this is a descendant, create leases for its parent shards (if they don't exist)
                if (isDescendant) {
                    for (String parentShardId : parentShardIds) {
                        if (!shardIdsOfCurrentLeases.contains(parentShardId)) {
                            log.debug("Need to create a lease for shardId {}", parentShardId);
                            Lease lease = shardIdToLeaseMapOfNewShards.get(parentShardId);
                            if (lease == null) {
                                lease = newKCLLease(shardIdToShardMapOfAllKinesisShards.get(parentShardId));
                                shardIdToLeaseMapOfNewShards.put(parentShardId, lease);
                            }

                            if (descendantParentShardIds.contains(parentShardId)
                                    && !initialPosition.getInitialPositionInStream()
                                        .equals(InitialPositionInStream.AT_TIMESTAMP)) {
                                lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                            } else {
                                lease.checkpoint(convertToCheckpoint(initialPosition));
                            }
                        }
                    }
                } else {
                    // This shard should be included, if the customer wants to process all records in the stream or
                    // if the initial position is AT_TIMESTAMP. For AT_TIMESTAMP, we will add a lease just like we do
                    // for TRIM_HORIZON. However we will only return back records with server-side timestamp at or
                    // after the specified initial position timestamp.
                    if (initialPosition.getInitialPositionInStream().equals(InitialPositionInStream.TRIM_HORIZON)
                            || initialPosition.getInitialPositionInStream()
                                .equals(InitialPositionInStream.AT_TIMESTAMP)) {
                        isDescendant = true;
                    }
                }

            }
        }

        memoizationContext.put(shardId, isDescendant);
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
    static Set<String> getParentShardIds(final Shard shard,
            final Map<String, Shard> shardIdToShardMapOfAllKinesisShards) {
        final Set<String> parentShardIds = new HashSet<>(2);
        final String parentShardId = shard.parentShardId();
        if (parentShardId != null && shardIdToShardMapOfAllKinesisShards.containsKey(parentShardId)) {
            parentShardIds.add(parentShardId);
        }
        final String adjacentParentShardId = shard.adjacentParentShardId();
        if (adjacentParentShardId != null && shardIdToShardMapOfAllKinesisShards.containsKey(adjacentParentShardId)) {
            parentShardIds.add(adjacentParentShardId);
        }
        return parentShardIds;
    }

    /**
     * Delete leases corresponding to shards that no longer exist in the stream. Current scheme: Delete a lease if:
     * <ul>
     * <li>The corresponding shard is not present in the list of Kinesis shards</li>
     * <li>The parentShardIds listed in the lease are also not present in the list of Kinesis shards.</li>
     * </ul>
     *
     * @param shards
     *            List of all Kinesis shards (assumed to be a consistent snapshot - when stream is in Active state).
     * @param trackedLeases
     *            List of
     * @param leaseRefresher
     * @throws KinesisClientLibIOException
     *             Thrown if we couldn't get a fresh shard list from Kinesis.
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    private static void cleanupGarbageLeases(@NonNull final ShardDetector shardDetector, final List<Shard> shards,
            final List<Lease> trackedLeases, final LeaseRefresher leaseRefresher) throws KinesisClientLibIOException,
            DependencyException, InvalidStateException, ProvisionedThroughputException {
        final Set<String> kinesisShards = shards.stream().map(Shard::shardId).collect(Collectors.toSet());

        // Check if there are leases for non-existent shards
        final List<Lease> garbageLeases = trackedLeases.stream()
                .filter(lease -> isCandidateForCleanup(lease, kinesisShards)).collect(Collectors.toList());

        if (!CollectionUtils.isNullOrEmpty(garbageLeases)) {
            log.info("Found {} candidate leases for cleanup. Refreshing list of" 
                    + " Kinesis shards to pick up recent/latest shards", garbageLeases.size());
            final Set<String> currentKinesisShardIds = getShardList(shardDetector).stream().map(Shard::shardId)
                    .collect(Collectors.toSet());

            for (Lease lease : garbageLeases) {
                if (isCandidateForCleanup(lease, currentKinesisShardIds)) {
                    log.info("Deleting lease for shard {} as it is not present in Kinesis stream.", lease.leaseKey());
                    leaseRefresher.deleteLease(lease);
                }
            }
        }
    }

    /**
     * Note: This method has package level access, solely for testing purposes.
     * 
     * @param lease Candidate shard we are considering for deletion.
     * @param currentKinesisShardIds
     * @return true if neither the shard (corresponding to the lease), nor its parents are present in
     *         currentKinesisShardIds
     * @throws KinesisClientLibIOException Thrown if currentKinesisShardIds contains a parent shard but not the child
     *         shard (we are evaluating for deletion).
     */
    static boolean isCandidateForCleanup(final Lease lease, final Set<String> currentKinesisShardIds)
            throws KinesisClientLibIOException {
        boolean isCandidateForCleanup = true;

        if (currentKinesisShardIds.contains(lease.leaseKey())) {
            isCandidateForCleanup = false;
        } else {
            log.info("Found lease for non-existent shard: {}. Checking its parent shards", lease.leaseKey());
            final Set<String> parentShardIds = lease.parentShardIds();
            for (String parentShardId : parentShardIds) {
                
                // Throw an exception if the parent shard exists (but the child does not).
                // This may be a (rare) race condition between fetching the shard list and Kinesis expiring shards. 
                if (currentKinesisShardIds.contains(parentShardId)) {
                    final String message = String.format("Parent shard %s exists but not the child shard %s",
                            parentShardId, lease.leaseKey());
                    log.info(message);
                    throw new KinesisClientLibIOException(message);
                }
            }
        }

        return isCandidateForCleanup;
    }
    
    /**
     * Private helper method.
     * Clean up leases for shards that meet the following criteria:
     * a/ the shard has been fully processed (checkpoint is set to SHARD_END)
     * b/ we've begun processing all the child shards: we have leases for all child shards and their checkpoint is not
     *      TRIM_HORIZON.
     * 
     * @param currentLeases List of leases we evaluate for clean up
     * @param shardIdToShardMap Map of shardId->Shard (assumed to include all Kinesis shards)
     * @param shardIdToChildShardIdsMap Map of shardId->childShardIds (assumed to include all Kinesis shards)
     * @param trackedLeases List of all leases we are tracking.
     * @param leaseRefresher Lease refresher (will be used to delete leases)
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    private synchronized void cleanupLeasesOfFinishedShards(final Collection<Lease> currentLeases,
            final Map<String, Shard> shardIdToShardMap, final Map<String, Set<String>> shardIdToChildShardIdsMap,
            final List<Lease> trackedLeases, final LeaseRefresher leaseRefresher) throws DependencyException,
            InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException {
        final List<Lease> leasesOfClosedShards = currentLeases.stream()
                .filter(lease -> lease.checkpoint().equals(ExtendedSequenceNumber.SHARD_END))
                .collect(Collectors.toList());
        final Set<String> shardIdsOfClosedShards = leasesOfClosedShards.stream().map(Lease::leaseKey)
                .collect(Collectors.toSet());

        if (!CollectionUtils.isNullOrEmpty(leasesOfClosedShards)) {
            assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, shardIdsOfClosedShards);
            Comparator<? super Lease> startingSequenceNumberComparator = new StartingSequenceNumberAndShardIdBasedComparator(
                    shardIdToShardMap);
            leasesOfClosedShards.sort(startingSequenceNumberComparator);
            final Map<String, Lease> trackedLeaseMap = trackedLeases.stream()
                    .collect(Collectors.toMap(Lease::leaseKey, Function.identity()));

            for (Lease leaseOfClosedShard : leasesOfClosedShards) {
                final String closedShardId = leaseOfClosedShard.leaseKey();
                final Set<String> childShardIds = shardIdToChildShardIdsMap.get(closedShardId);
                if (closedShardId != null && !CollectionUtils.isNullOrEmpty(childShardIds)) {
                    cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, leaseRefresher);
                }
            }
        }        
    }

    /** 
     * Delete lease for the closed shard. Rules for deletion are:
     * a/ the checkpoint for the closed shard is SHARD_END,
     * b/ there are leases for all the childShardIds and their checkpoint is NOT TRIM_HORIZON
     * Note: This method has package level access solely for testing purposes.
     * 
     * @param closedShardId Identifies the closed shard
     * @param childShardIds ShardIds of children of the closed shard
     * @param trackedLeases shardId->Lease map with all leases we are tracking (should not be null)
     * @param leaseRefresher
     * @throws ProvisionedThroughputException 
     * @throws InvalidStateException 
     * @throws DependencyException 
     */
    synchronized void cleanupLeaseForClosedShard(final String closedShardId, final Set<String> childShardIds,
            final Map<String, Lease> trackedLeases, final LeaseRefresher leaseRefresher)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final Lease leaseForClosedShard = trackedLeases.get(closedShardId);
        final List<Lease> childShardLeases = childShardIds.stream().map(trackedLeases::get).filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (leaseForClosedShard != null && leaseForClosedShard.checkpoint().equals(ExtendedSequenceNumber.SHARD_END)
                && childShardLeases.size() == childShardIds.size()) {
            boolean okayToDelete = true;
            for (Lease lease : childShardLeases) {
                if (lease.checkpoint().equals(ExtendedSequenceNumber.TRIM_HORIZON)) {
                    okayToDelete = false;
                    break;
                }
            }
            
            if (okayToDelete) {
                log.info("Deleting lease for shard {} as it has been completely processed and processing of child "
                        + "shards has begun.", leaseForClosedShard.leaseKey());
                leaseRefresher.deleteLease(leaseForClosedShard);
            }
        }
    }

    /**
     * Helper method to create a new Lease POJO for a shard.
     * Note: Package level access only for testing purposes
     * 
     * @param shard
     * @return
     */
    private static Lease newKCLLease(final Shard shard) {
        Lease newLease = new Lease();
        newLease.leaseKey(shard.shardId());
        List<String> parentShardIds = new ArrayList<>(2);
        if (shard.parentShardId() != null) {
            parentShardIds.add(shard.parentShardId());
        }
        if (shard.adjacentParentShardId() != null) {
            parentShardIds.add(shard.adjacentParentShardId());
        }
        newLease.parentShardIds(parentShardIds);
        newLease.ownerSwitchesSinceCheckpoint(0L);

        return newLease;
    }

    /**
     * Helper method to construct a shardId->Shard map for the specified list of shards.
     * 
     * @param shards List of shards
     * @return ShardId->Shard map
     */
    static Map<String, Shard> constructShardIdToShardMap(final List<Shard> shards) {
        return shards.stream().collect(Collectors.toMap(Shard::shardId, Function.identity()));
    }

    /**
     * Helper method to return all the open shards for a stream.
     * Note: Package level access only for testing purposes.
     * 
     * @param allShards All shards returved via DescribeStream. We assume this to represent a consistent shard list.
     * @return List of open shards (shards at the tip of the stream) - may include shards that are not yet active.
     */
    static List<Shard> getOpenShards(final List<Shard> allShards) {
        return allShards.stream().filter(shard -> shard.sequenceNumberRange().endingSequenceNumber() == null)
                .peek(shard -> log.debug("Found open shard: {}", shard.shardId())).collect(Collectors.toList());
    }

    private static ExtendedSequenceNumber convertToCheckpoint(final InitialPositionInStreamExtended position) {
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
    @RequiredArgsConstructor
    private static class StartingSequenceNumberAndShardIdBasedComparator implements Comparator<Lease>, Serializable {
        private static final long serialVersionUID = 1L;

        private final Map<String, Shard> shardIdToShardMap;

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
        public int compare(final Lease lease1, final Lease lease2) {
            int result = 0;
            final String shardId1 = lease1.leaseKey();
            final String shardId2 = lease2.leaseKey();
            final Shard shard1 = shardIdToShardMap.get(shardId1);
            final Shard shard2 = shardIdToShardMap.get(shardId2);
            
            // If we found shards for the two leases, use comparison of the starting sequence numbers
            if (shard1 != null && shard2 != null) {
                BigInteger sequenceNumber1 = new BigInteger(shard1.sequenceNumberRange().startingSequenceNumber());
                BigInteger sequenceNumber2 = new BigInteger(shard2.sequenceNumberRange().startingSequenceNumber());
                result = sequenceNumber1.compareTo(sequenceNumber2);                
            }
            
            if (result == 0) {
                result = shardId1.compareTo(shardId2);
            }
            
            return result;
        }
                
    }

}
