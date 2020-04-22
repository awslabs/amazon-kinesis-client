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
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
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

    private final boolean isMultiStreamMode;

    private String streamIdentifier = "";

    private static final String MIN_HASH_KEY = BigInteger.ZERO.toString();
    private static final String MAX_HASH_KEY = new BigInteger("2").pow(128).subtract(BigInteger.ONE).toString();
    private static final int retriesForCompleteHashRange = 3;

    private static final long DELAY_BETWEEN_LIST_SHARDS_MILLIS = 1000;

    public HierarchicalShardSyncer() {
        isMultiStreamMode = false;
    }

    public HierarchicalShardSyncer(final boolean isMultiStreamMode) {
        this.isMultiStreamMode = isMultiStreamMode;
    }

    private static final BiFunction<Lease, MultiStreamArgs, String> shardIdFromLeaseDeducer =
            (lease, multiStreamArgs) ->
            multiStreamArgs.isMultiStreamMode() ?
                    ((MultiStreamLease) lease).shardId() :
                    lease.leaseKey();

    /**
     * Check and create leases for any new shards (e.g. following a reshard operation). Sync leases with Kinesis shards
     * (e.g. at startup, or when we reach end of a shard).
     *
     * @param shardDetector
     * @param leaseRefresher
     * @param initialPosition
     * @param scope
     * @param cleanupLeasesOfCompletedShards
     * @param ignoreUnexpectedChildShards
     * @param garbageCollectLeases
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    public synchronized void checkAndCreateLeaseForNewShards(@NonNull final ShardDetector shardDetector,
            final LeaseRefresher leaseRefresher, final InitialPositionInStreamExtended initialPosition,
            final MetricsScope scope, final boolean cleanupLeasesOfCompletedShards, final boolean ignoreUnexpectedChildShards,
            final boolean garbageCollectLeases, final boolean isLeaseTableEmpty)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException, InterruptedException {
        this.streamIdentifier = shardDetector.streamIdentifier().serialize();
        final List<Shard> latestShards = isLeaseTableEmpty ?
                getShardListAtInitialPosition(shardDetector, initialPosition) : getShardList(shardDetector);
        checkAndCreateLeaseForNewShards(shardDetector, leaseRefresher, initialPosition, latestShards, cleanupLeasesOfCompletedShards, ignoreUnexpectedChildShards, scope, garbageCollectLeases,
                isLeaseTableEmpty);
    }

    //Provide a pre-collcted list of shards to avoid calling ListShards API
    public synchronized void checkAndCreateLeaseForNewShards(@NonNull final ShardDetector shardDetector,
            final LeaseRefresher leaseRefresher, final InitialPositionInStreamExtended initialPosition,
            List<Shard> latestShards, final boolean cleanupLeasesOfCompletedShards, final boolean ignoreUnexpectedChildShards,
            final MetricsScope scope, final boolean garbageCollectLeases, final boolean isLeaseTableEmpty)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException {

        this.streamIdentifier = shardDetector.streamIdentifier().serialize();
        //TODO: Need to add multistream support for this https://sim.amazon.com/issues/KinesisLTR-191

        if (!CollectionUtils.isNullOrEmpty(latestShards)) {
            log.debug("{} - Num shards: {}", streamIdentifier, latestShards.size());
        }

        final Map<String, Shard> shardIdToShardMap = constructShardIdToShardMap(latestShards);
        final Map<String, Set<String>> shardIdToChildShardIdsMap = constructShardIdToChildShardIdsMap(
                shardIdToShardMap);
        final Set<String> inconsistentShardIds = findInconsistentShardIds(shardIdToChildShardIdsMap, shardIdToShardMap);
        if (!ignoreUnexpectedChildShards) {
            assertAllParentShardsAreClosed(inconsistentShardIds);
        }
        final List<Lease> currentLeases = isMultiStreamMode ?
                leaseRefresher.listLeasesForStream(shardDetector.streamIdentifier()) : leaseRefresher.listLeases();
        final MultiStreamArgs multiStreamArgs = new MultiStreamArgs(isMultiStreamMode, shardDetector.streamIdentifier());
        final LeaseSynchronizer leaseSynchronizer = isLeaseTableEmpty ? new EmptyLeaseTableSynchronizer() :
                new NonEmptyLeaseTableSynchronizer(shardDetector, shardIdToShardMap, shardIdToChildShardIdsMap);
        final List<Lease> newLeasesToCreate = determineNewLeasesToCreate(leaseSynchronizer, latestShards, currentLeases,
                initialPosition, inconsistentShardIds, multiStreamArgs);
        log.debug("{} - Num new leases to create: {}", streamIdentifier, newLeasesToCreate.size());
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
        if (!isLeaseTableEmpty && garbageCollectLeases) {
            cleanupGarbageLeases(shardDetector, latestShards, trackedLeases, leaseRefresher, multiStreamArgs);
        }
        if (!isLeaseTableEmpty && cleanupLeasesOfCompletedShards) {
            cleanupLeasesOfFinishedShards(currentLeases, shardIdToShardMap, shardIdToChildShardIdsMap, trackedLeases,
                    leaseRefresher, multiStreamArgs);
        }
    }

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
                log.info("{} : Shard {} is not present in Kinesis anymore.", streamIdentifier, shardId);
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

    /**
     * Helper method to resolve the correct shard filter to use when listing shards from a position in a stream.
     * @param initialPositionInStreamExtended
     * @return ShardFilter shard filter for the corresponding position in the stream.
     */
    private static ShardFilter getShardFilterFromInitialPosition(InitialPositionInStreamExtended initialPositionInStreamExtended) {

        ShardFilter.Builder builder = ShardFilter.builder();

        switch (initialPositionInStreamExtended.getInitialPositionInStream()) {
            case LATEST:
                builder = builder.type(ShardFilterType.AT_LATEST);
                break;
            case TRIM_HORIZON:
                builder = builder.type(ShardFilterType.AT_TRIM_HORIZON);
                break;
            case AT_TIMESTAMP:
                builder = builder.type(ShardFilterType.AT_TIMESTAMP).timestamp(initialPositionInStreamExtended.getTimestamp().toInstant());
                break;
        }
        return builder.build();
    }

    private static List<Shard> getShardListAtInitialPosition(@NonNull final ShardDetector shardDetector,
        InitialPositionInStreamExtended initialPositionInStreamExtended) throws KinesisClientLibIOException, InterruptedException {

        final ShardFilter shardFilter = getShardFilterFromInitialPosition(initialPositionInStreamExtended);
        final String streamName = shardDetector.streamIdentifier().streamName();

        List<Shard> shards;

        for (int i = 0; i < retriesForCompleteHashRange; i++) {
            shards = shardDetector.listShardsWithFilter(shardFilter);

            if (shards == null) {
                throw new KinesisClientLibIOException(
                        "Stream " + streamName + " is not in ACTIVE OR UPDATING state - will retry getting the shard list.");
            }

            if (hashRangeOfShardsIsComplete(shards)) {
                return shards;
            }

            Thread.sleep(DELAY_BETWEEN_LIST_SHARDS_MILLIS);
        }

        throw new KinesisClientLibIOException("Hash range of shards returned for " + streamName + " was incomplete after "
                + retriesForCompleteHashRange + " retries.");
    }

    private static List<Shard> getShardList(@NonNull final ShardDetector shardDetector) throws KinesisClientLibIOException {
        final Optional<List<Shard>> shards = Optional.of(shardDetector.listShards());

        return shards.orElseThrow(() -> new KinesisClientLibIOException("Stream " + shardDetector.streamIdentifier().streamName() +
                " is not in ACTIVE OR UPDATING state - will retry getting the shard list."));
    }

    private static boolean hashRangeOfShardsIsComplete(@NonNull List<Shard> shards) {

        if (shards.isEmpty()) {
            throw new IllegalStateException("No shards found when attempting to validate complete hash range.");
        }

        final Comparator<Shard> shardStartingHashKeyBasedComparator = new ShardStartingHashKeyBasedComparator();
        shards.sort(shardStartingHashKeyBasedComparator);

        if (!shards.get(0).hashKeyRange().startingHashKey().equals(MIN_HASH_KEY) ||
                !shards.get(shards.size() - 1).hashKeyRange().endingHashKey().equals(MAX_HASH_KEY)) {
            return false;
        }

        if (shards.size() > 1) {
            for (int i = 1; i < shards.size(); i++) {
                final Shard shardAtStartOfPossibleHole = shards.get(i - 1);
                final Shard shardAtEndOfPossibleHole = shards.get(i);
                final BigInteger startOfPossibleHole = new BigInteger(shardAtStartOfPossibleHole.hashKeyRange().endingHashKey());
                final BigInteger endOfPossibleHole = new BigInteger(shardAtEndOfPossibleHole.hashKeyRange().startingHashKey());

                if (!endOfPossibleHole.subtract(startOfPossibleHole).equals(BigInteger.ONE)) {
                    log.error("Incomplete hash range found between {} and {}.", shardAtStartOfPossibleHole, shardAtEndOfPossibleHole);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Determine new leases to create and their initial checkpoint.
     * Note: Package level access only for testing purposes.
     *
     * @param leaseSynchronizer determines the strategy we'll be using to update any new leases.
     * @param shards List of all shards in Kinesis (we'll create new leases based on this set)
     * @param currentLeases List of current leases
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @param inconsistentShardIds Set of child shard ids having open parents.
     * @param multiStreamArgs determines if we are using multistream mode.
     * @return List of new leases to create sorted by starting sequenceNumber of the corresponding shard
     */
    static List<Lease> determineNewLeasesToCreate(final LeaseSynchronizer leaseSynchronizer, final List<Shard> shards,
            final List<Lease> currentLeases, final InitialPositionInStreamExtended initialPosition,
            final Set<String> inconsistentShardIds, final MultiStreamArgs multiStreamArgs) {
        return leaseSynchronizer.determineNewLeasesToCreate(shards, currentLeases, initialPosition, inconsistentShardIds, multiStreamArgs);
    }

    /**
     * Determine new leases to create and their initial checkpoint.
     * Note: Package level access only for testing purposes.
     *
     * @param leaseSynchronizer determines the strategy we'll be using to update any new leases.
     * @param shards List of all shards in Kinesis (we'll create new leases based on this set)
     * @param currentLeases List of current leases
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @param inconsistentShardIds Set of child shard ids having open parents.
     * @return List of new leases to create sorted by starting sequenceNumber of the corresponding shard
     */
    static List<Lease> determineNewLeasesToCreate(final LeaseSynchronizer leaseSynchronizer, final List<Shard> shards,
            final List<Lease> currentLeases, final InitialPositionInStreamExtended initialPosition,final Set<String> inconsistentShardIds) {
        return determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, initialPosition, inconsistentShardIds,
                new MultiStreamArgs(false, null));
    }

    /**
     * Determine new leases to create and their initial checkpoint.
     * Note: Package level access only for testing purposes.
     *
     * @param leaseSynchronizer determines the strategy we'll be using to update any new leases.
     * @param shards List of all shards in Kinesis (we'll create new leases based on this set)
     * @param currentLeases List of current leases
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @return List of new leases to create sorted by starting sequenceNumber of the corresponding shard
     */
    static List<Lease> determineNewLeasesToCreate(final LeaseSynchronizer leaseSynchronizer, final List<Shard> shards,
            final List<Lease> currentLeases, final InitialPositionInStreamExtended initialPosition) {
        final Set<String> inconsistentShardIds = new HashSet<>();
        return determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, initialPosition, inconsistentShardIds);
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
            final Map<String, Lease> shardIdToLeaseMapOfNewShards, final Map<String, Boolean> memoizationContext,
            final MultiStreamArgs multiStreamArgs) {
        final String streamIdentifier = getStreamIdentifier(multiStreamArgs);
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
                            memoizationContext, multiStreamArgs)) {
                        isDescendant = true;
                        descendantParentShardIds.add(parentShardId);
                        log.debug("{} : Parent shard {} is a descendant.", streamIdentifier, parentShardId);
                    } else {
                        log.debug("{} : Parent shard {} is NOT a descendant.", streamIdentifier, parentShardId);
                    }
                }

                // If this is a descendant, create leases for its parent shards (if they don't exist)
                if (isDescendant) {
                    for (String parentShardId : parentShardIds) {
                        if (!shardIdsOfCurrentLeases.contains(parentShardId)) {
                            log.debug("{} : Need to create a lease for shardId {}", streamIdentifier, parentShardId);
                            Lease lease = shardIdToLeaseMapOfNewShards.get(parentShardId);
                            if (lease == null) {
                                lease = multiStreamArgs.isMultiStreamMode() ?
                                        newKCLMultiStreamLease(shardIdToShardMapOfAllKinesisShards.get(parentShardId),
                                                multiStreamArgs.streamIdentifier()) :
                                        newKCLLease(shardIdToShardMapOfAllKinesisShards.get(parentShardId));
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

    static boolean checkIfDescendantAndAddNewLeasesForAncestors(final String shardId,
            final InitialPositionInStreamExtended initialPosition, final Set<String> shardIdsOfCurrentLeases,
            final Map<String, Shard> shardIdToShardMapOfAllKinesisShards,
            final Map<String, Lease> shardIdToLeaseMapOfNewShards, final Map<String, Boolean> memoizationContext) {
        return checkIfDescendantAndAddNewLeasesForAncestors(shardId, initialPosition, shardIdsOfCurrentLeases,
                shardIdToShardMapOfAllKinesisShards, shardIdToLeaseMapOfNewShards, memoizationContext,
                new MultiStreamArgs(false, null));
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
            final List<Lease> trackedLeases, final LeaseRefresher leaseRefresher,
            final MultiStreamArgs multiStreamArgs) throws KinesisClientLibIOException,
            DependencyException, InvalidStateException, ProvisionedThroughputException {
        final String streamIdentifier = getStreamIdentifier(multiStreamArgs);
        final Set<String> kinesisShards = shards.stream().map(Shard::shardId).collect(Collectors.toSet());

        // Check if there are leases for non-existent shards
        final List<Lease> garbageLeases = trackedLeases.stream()
                .filter(lease -> isCandidateForCleanup(lease, kinesisShards, multiStreamArgs)).collect(Collectors.toList());

        if (!CollectionUtils.isNullOrEmpty(garbageLeases)) {
            log.info("{} : Found {} candidate leases for cleanup. Refreshing list of"
                    + " Kinesis shards to pick up recent/latest shards", streamIdentifier, garbageLeases.size());
            final Set<String> currentKinesisShardIds = getShardList(shardDetector).stream().map(Shard::shardId)
                    .collect(Collectors.toSet());

            for (Lease lease : garbageLeases) {
                if (isCandidateForCleanup(lease, currentKinesisShardIds, multiStreamArgs)) {
                    log.info("{} : Deleting lease for shard {} as it is not present in Kinesis stream.",
                            streamIdentifier, lease.leaseKey());
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
    static boolean isCandidateForCleanup(final Lease lease, final Set<String> currentKinesisShardIds,
            final MultiStreamArgs multiStreamArgs)
            throws KinesisClientLibIOException {

        final String streamIdentifier = getStreamIdentifier(multiStreamArgs);

        boolean isCandidateForCleanup = true;
        final String shardId = shardIdFromLeaseDeducer.apply(lease, multiStreamArgs);

        if (currentKinesisShardIds.contains(shardId)) {
            isCandidateForCleanup = false;
        } else {
            log.info("{} : Found lease for non-existent shard: {}. Checking its parent shards", streamIdentifier, shardId);
            final Set<String> parentShardIds = lease.parentShardIds();
            for (String parentShardId : parentShardIds) {
                
                // Throw an exception if the parent shard exists (but the child does not).
                // This may be a (rare) race condition between fetching the shard list and Kinesis expiring shards. 
                if (currentKinesisShardIds.contains(parentShardId)) {
                    final String message = String.format("Parent shard %s exists but not the child shard %s",
                            parentShardId, shardId);
                    log.info("{} : {}", streamIdentifier, message);
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
            final List<Lease> trackedLeases, final LeaseRefresher leaseRefresher,
            final MultiStreamArgs multiStreamArgs) throws DependencyException,
            InvalidStateException, ProvisionedThroughputException, KinesisClientLibIOException {
        final List<Lease> leasesOfClosedShards = currentLeases.stream()
                .filter(lease -> lease.checkpoint().equals(ExtendedSequenceNumber.SHARD_END))
                .collect(Collectors.toList());
        final Set<String> shardIdsOfClosedShards = leasesOfClosedShards.stream()
                .map(lease -> shardIdFromLeaseDeducer.apply(lease, multiStreamArgs)).collect(Collectors.toSet());

        if (!CollectionUtils.isNullOrEmpty(leasesOfClosedShards)) {
            assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, shardIdsOfClosedShards);
            //TODO: Verify before LTR launch that ending sequence number is still returned from the service.
            Comparator<? super Lease> startingSequenceNumberComparator = new StartingSequenceNumberAndShardIdBasedComparator(
                    shardIdToShardMap, multiStreamArgs);
            leasesOfClosedShards.sort(startingSequenceNumberComparator);
            final Map<String, Lease> trackedLeaseMap = trackedLeases.stream()
                    .collect(Collectors.toMap(lease -> shardIdFromLeaseDeducer.apply(lease, multiStreamArgs), Function.identity()));

            for (Lease leaseOfClosedShard : leasesOfClosedShards) {
                final String closedShardId = shardIdFromLeaseDeducer.apply(leaseOfClosedShard, multiStreamArgs);
                final Set<String> childShardIds = shardIdToChildShardIdsMap.get(closedShardId);
                if (closedShardId != null && !CollectionUtils.isNullOrEmpty(childShardIds)) {
                    cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, leaseRefresher, multiStreamArgs);
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
            final Map<String, Lease> trackedLeases, final LeaseRefresher leaseRefresher, final MultiStreamArgs multiStreamArgs)
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
                log.info("{} : Deleting lease for shard {} as it has been completely processed and processing of child "
                        + "shards has begun.", streamIdentifier, shardIdFromLeaseDeducer.apply(leaseForClosedShard, multiStreamArgs));
                leaseRefresher.deleteLease(leaseForClosedShard);
            }
        }
    }

    public synchronized Lease createLeaseForChildShard(final ChildShard childShard, final StreamIdentifier streamIdentifier) throws InvalidStateException {
        final MultiStreamArgs multiStreamArgs = new MultiStreamArgs(isMultiStreamMode, streamIdentifier);

        return multiStreamArgs.isMultiStreamMode() ? newKCLMultiStreamLeaseForChildShard(childShard, streamIdentifier)
                                                   : newKCLLeaseForChildShard(childShard);
    }

    private static Lease newKCLLeaseForChildShard(final ChildShard childShard) throws InvalidStateException {
        Lease newLease = new Lease();
        newLease.leaseKey(childShard.shardId());
        if (!CollectionUtils.isNullOrEmpty(childShard.parentShards())) {
            newLease.parentShardIds(childShard.parentShards());
        } else {
            throw new InvalidStateException("Unable to populate new lease for child shard " + childShard.shardId() + "because parent shards cannot be found.");
        }
        newLease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        newLease.ownerSwitchesSinceCheckpoint(0L);
        return newLease;
    }

    private static Lease newKCLMultiStreamLeaseForChildShard(final ChildShard childShard, final StreamIdentifier streamIdentifier) throws InvalidStateException {
        MultiStreamLease newLease = new MultiStreamLease();
        newLease.leaseKey(MultiStreamLease.getLeaseKey(streamIdentifier.serialize(), childShard.shardId()));
        if (!CollectionUtils.isNullOrEmpty(childShard.parentShards())) {
            newLease.parentShardIds(childShard.parentShards());
        } else {
            throw new InvalidStateException("Unable to populate new lease for child shard " + childShard.shardId() + "because parent shards cannot be found.");
        }
        newLease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        newLease.ownerSwitchesSinceCheckpoint(0L);
        newLease.streamIdentifier(streamIdentifier.serialize());
        newLease.shardId(childShard.shardId());
        return newLease;
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

    private static Lease newKCLMultiStreamLease(final Shard shard, final StreamIdentifier streamIdentifier) {
        MultiStreamLease newLease = new MultiStreamLease();
        newLease.leaseKey(MultiStreamLease.getLeaseKey(streamIdentifier.serialize(), shard.shardId()));
        List<String> parentShardIds = new ArrayList<>(2);
        if (shard.parentShardId() != null) {
            parentShardIds.add(shard.parentShardId());
        }
        if (shard.adjacentParentShardId() != null) {
            parentShardIds.add(shard.adjacentParentShardId());
        }
        newLease.parentShardIds(parentShardIds);
        newLease.ownerSwitchesSinceCheckpoint(0L);
        newLease.streamIdentifier(streamIdentifier.serialize());
        newLease.shardId(shard.shardId());
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
    static List<Shard> getOpenShards(final List<Shard> allShards, final String streamIdentifier) {
        return allShards.stream().filter(shard -> shard.sequenceNumberRange().endingSequenceNumber() == null)
                .peek(shard -> log.debug("{} : Found open shard: {}", streamIdentifier, shard.shardId())).collect(Collectors.toList());
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

    private static String getStreamIdentifier(MultiStreamArgs multiStreamArgs) {
        return Optional.ofNullable(multiStreamArgs.streamIdentifier())
                .map(streamId -> streamId.serialize()).orElse("single_stream_mode");
    }

    /**
     * Helper class to compare shards based on their hash range.
     */
    @RequiredArgsConstructor
    private static class ShardStartingHashKeyBasedComparator implements Comparator<Shard>, Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * Compares two shards based on their starting hash keys.
         * We assume that the shards provided are non-null.
         *
         * {@inheritDoc}
         */
        @Override
        public int compare(Shard shard1, Shard shard2) {
            BigInteger hashKey1 = new BigInteger(shard1.hashKeyRange().startingHashKey());
            BigInteger hashKey2 = new BigInteger(shard2.hashKeyRange().startingHashKey());

            return hashKey1.compareTo(hashKey2);
        }
    }

    /** Helper class to compare leases based on starting sequence number of the corresponding shards.
     *
     */
    @RequiredArgsConstructor
    private static class StartingSequenceNumberAndShardIdBasedComparator implements Comparator<Lease>, Serializable {
        private static final long serialVersionUID = 1L;

        private final Map<String, Shard> shardIdToShardMap;
        private final MultiStreamArgs multiStreamArgs;

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
            final String shardId1 = shardIdFromLeaseDeducer.apply(lease1, multiStreamArgs);
            final String shardId2 = shardIdFromLeaseDeducer.apply(lease2, multiStreamArgs);
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

    @Data
    @Accessors(fluent = true)
    @VisibleForTesting
    static class MultiStreamArgs {
        private final Boolean isMultiStreamMode;
        private final StreamIdentifier streamIdentifier;
    }

    /**
     * Interface to determine how to create new leases.
     */
    @VisibleForTesting
    interface LeaseSynchronizer {
        /**
         * Determines how to create leases.
         * @param shards
         * @param currentLeases
         * @param initialPosition
         * @param inconsistentShardIds
         * @param multiStreamArgs
         * @return
         */
        List<Lease> determineNewLeasesToCreate(List<Shard> shards, List<Lease> currentLeases,
                                               InitialPositionInStreamExtended initialPosition, Set<String> inconsistentShardIds,
                                               MultiStreamArgs multiStreamArgs);
    }

    /**
     * Class to help create leases when the table is initially empty.
     */
    @Slf4j
    @AllArgsConstructor
    static class EmptyLeaseTableSynchronizer implements LeaseSynchronizer {

        /**
         * Determines how to create leases when the lease table is initially empty. For this, we read all shards where
         * the KCL is reading from. For any shards which are closed, we will discover their child shards through GetRecords
         * child shard information.
         *
         * @param shards
         * @param currentLeases
         * @param initialPosition
         * @param inconsistentShardIds
         * @param multiStreamArgs
         * @return
         */
        @Override
        public List<Lease> determineNewLeasesToCreate(List<Shard> shards, List<Lease> currentLeases,
            InitialPositionInStreamExtended initialPosition, Set<String> inconsistentShardIds, MultiStreamArgs multiStreamArgs) {
            final String streamIdentifier = Optional.ofNullable(multiStreamArgs.streamIdentifier())
                    .map(streamId -> streamId.serialize()).orElse("");
            final Map<String, Shard> shardIdToShardMapOfAllKinesisShards = constructShardIdToShardMap(shards);

            currentLeases.stream().peek(lease -> log.debug("{} : Existing lease: {}", streamIdentifier, lease))
                    .map(lease -> shardIdFromLeaseDeducer.apply(lease, multiStreamArgs))
                    .collect(Collectors.toSet());

            final List<Lease> newLeasesToCreate = getLeasesToCreateForOpenAndClosedShards(initialPosition, shards, multiStreamArgs, streamIdentifier);

            //TODO: Verify before LTR launch that ending sequence number is still returned from the service.
            final Comparator<Lease> startingSequenceNumberComparator =
                    new StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMapOfAllKinesisShards, multiStreamArgs);
            newLeasesToCreate.sort(startingSequenceNumberComparator);
            return newLeasesToCreate;
        }

        /**
         * Helper method to create leases. For an empty lease table, we will be creating leases for all shards
         * regardless of if they are open or closed. Closed shards will be unblocked via child shard information upon
         * reaching SHARD_END.
         */
        private List<Lease> getLeasesToCreateForOpenAndClosedShards(InitialPositionInStreamExtended initialPosition,
            List<Shard> shards, MultiStreamArgs multiStreamArgs, String streamId)  {
            final Map<String, Lease> shardIdToNewLeaseMap = new HashMap<>();

            for (Shard shard : shards) {
                final String shardId = shard.shardId();
                final Lease lease = multiStreamArgs.isMultiStreamMode() ?
                        newKCLMultiStreamLease(shard, multiStreamArgs.streamIdentifier) : newKCLLease(shard);
                lease.checkpoint(convertToCheckpoint(initialPosition));

                log.debug("{} : Need to create a lease for shard with shardId {}", streamId, shardId);

                shardIdToNewLeaseMap.put(shardId, lease);
            }

            return new ArrayList(shardIdToNewLeaseMap.values());
        }
    }


    /**
     * Class to help create leases when the lease table is not initially empty.
     */
    @Slf4j
    @AllArgsConstructor
    static class NonEmptyLeaseTableSynchronizer implements LeaseSynchronizer {

        private final ShardDetector shardDetector;
        private final Map<String, Shard> shardIdToShardMap;
        private final Map<String, Set<String>> shardIdToChildShardIdsMap;

        /**
         * Determine new leases to create and their initial checkpoint.
         * Note: Package level access only for testing purposes.
         * <p>
         * For each open (no ending sequence number) shard without open parents that doesn't already have a lease,
         * determine if it is a descendent of any shard which is or will be processed (e.g. for which a lease exists):
         * If so, set checkpoint of the shard to TrimHorizon and also create leases for ancestors if needed.
         * If not, set checkpoint of the shard to the initial position specified by the client.
         * To check if we need to create leases for ancestors, we use the following rules:
         * * If we began (or will begin) processing data for a shard, then we must reach end of that shard before
         * we begin processing data from any of its descendants.
         * * A shard does not start processing data until data from all its parents has been processed.
         * Note, if the initial position is LATEST and a shard has two parents and only one is a descendant - we'll create
         * leases corresponding to both the parents - the parent shard which is not a descendant will have
         * its checkpoint set to Latest.
         * <p>
         * We assume that if there is an existing lease for a shard, then either:
         * * we have previously created a lease for its parent (if it was needed), or
         * * the parent shard has expired.
         * <p>
         * For example:
         * Shard structure (each level depicts a stream segment):
         * 0 1 2 3 4   5   - shards till epoch 102
         * \ / \ / |   |
         * 6   7  4   5   - shards from epoch 103 - 205
         * \ /   |  / \
         * 8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
         * Current leases: (3, 4, 5)
         * New leases to create: (2, 6, 7, 8, 9, 10)
         * <p>
         * The leases returned are sorted by the starting sequence number - following the same order
         * when persisting the leases in DynamoDB will ensure that we recover gracefully if we fail
         * before creating all the leases.
         * <p>
         * If a shard has no existing lease, is open, and is a descendant of a parent which is still open, we ignore it
         * here; this happens when the list of shards is inconsistent, which could be due to pagination delay for very
         * high shard count streams (i.e., dynamodb streams for tables with thousands of partitions).  This can only
         * currently happen here if ignoreUnexpectedChildShards was true in syncShardleases.
         *
         * @return List of new leases to create sorted by starting sequenceNumber of the corresponding shard
         */
        @Override
        public synchronized List<Lease> determineNewLeasesToCreate(final List<Shard> shards, final List<Lease> currentLeases,
                final InitialPositionInStreamExtended initialPosition, final Set<String> inconsistentShardIds,
                final MultiStreamArgs multiStreamArgs) {
            final Map<String, Lease> shardIdToNewLeaseMap = new HashMap<>();
            final Map<String, Shard> shardIdToShardMapOfAllKinesisShards = constructShardIdToShardMap(shards);
            final String streamIdentifier = Optional.ofNullable(multiStreamArgs.streamIdentifier())
                    .map(streamId -> streamId.serialize()).orElse("");
            final Set<String> shardIdsOfCurrentLeases = currentLeases.stream()
                    .peek(lease -> log.debug("{} : Existing lease: {}", streamIdentifier, lease))
                    .map(lease -> shardIdFromLeaseDeducer.apply(lease, multiStreamArgs))
                    .collect(Collectors.toSet());

            final List<Shard> openShards = getOpenShards(shards, streamIdentifier);
            final Map<String, Boolean> memoizationContext = new HashMap<>();

            // Iterate over the open shards and find those that don't have any lease entries.
            for (Shard shard : openShards) {
                final String shardId = shard.shardId();
                log.debug("{} : Evaluating leases for open shard {} and its ancestors.", streamIdentifier, shardId);
                if (shardIdsOfCurrentLeases.contains(shardId)) {
                    log.debug("{} : Lease for shardId {} already exists. Not creating a lease", streamIdentifier, shardId);
                } else if (inconsistentShardIds.contains(shardId)) {
                    log.info("{} : shardId {} is an inconsistent child.  Not creating a lease", streamIdentifier, shardId);
                } else {
                    log.debug("{} : Need to create a lease for shardId {}", streamIdentifier, shardId);
                    final Lease newLease = multiStreamArgs.isMultiStreamMode() ?
                            newKCLMultiStreamLease(shard, multiStreamArgs.streamIdentifier()) :
                            newKCLLease(shard);
                    final boolean isDescendant = checkIfDescendantAndAddNewLeasesForAncestors(shardId, initialPosition,
                            shardIdsOfCurrentLeases, shardIdToShardMapOfAllKinesisShards, shardIdToNewLeaseMap,
                            memoizationContext, multiStreamArgs);

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
                    log.debug("{} : Set checkpoint of {} to {}", streamIdentifier, newLease.leaseKey(), newLease.checkpoint());
                    shardIdToNewLeaseMap.put(shardId, newLease);
                }
            }

            final List<Lease> newLeasesToCreate = new ArrayList<>(shardIdToNewLeaseMap.values());
            final Comparator<Lease> startingSequenceNumberComparator = new StartingSequenceNumberAndShardIdBasedComparator(
                    shardIdToShardMapOfAllKinesisShards, multiStreamArgs);
            newLeasesToCreate.sort(startingSequenceNumberComparator);
            return newLeasesToCreate;
        }
    }
}
