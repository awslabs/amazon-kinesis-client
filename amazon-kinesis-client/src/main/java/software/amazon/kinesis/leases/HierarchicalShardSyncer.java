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
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
        final List<Shard> latestShards = leaseRefresher.isLeaseTableEmpty() ?
                getShardListAtInitialPosition(shardDetector, initialPosition) : getFullShardList(shardDetector);
        checkAndCreateLeaseForNewShards(shardDetector, leaseRefresher, initialPosition, cleanupLeasesOfCompletedShards,
                                        ignoreUnexpectedChildShards, scope, latestShards);
    }

    //Provide a pre-collcted list of shards to avoid calling ListShards API
    public synchronized void checkAndCreateLeaseForNewShards(@NonNull final ShardDetector shardDetector,
            final LeaseRefresher leaseRefresher, final InitialPositionInStreamExtended initialPosition, final boolean cleanupLeasesOfCompletedShards,
            final boolean ignoreUnexpectedChildShards, final MetricsScope scope, List<Shard> latestShards)
            throws DependencyException, InvalidStateException,
            ProvisionedThroughputException, KinesisClientLibIOException {
        if (!CollectionUtils.isNullOrEmpty(latestShards)) {
            log.debug("Num shards: {}", latestShards.size());
        }

        final Map<String, Shard> shardIdToShardMap = constructShardIdToShardMap(latestShards);
        final Map<String, Set<String>> shardIdToChildShardIdsMap = constructShardIdToChildShardIdsMap(
                shardIdToShardMap);
        final Set<String> inconsistentShardIds = findInconsistentShardIds(shardIdToChildShardIdsMap, shardIdToShardMap);
        if (!ignoreUnexpectedChildShards) {
            assertAllParentShardsAreClosed(inconsistentShardIds);
        }
        final List<Lease> currentLeases = isMultiStreamMode ?
                getLeasesForStream(shardDetector.streamIdentifier(), leaseRefresher) :
                leaseRefresher.listLeases();
        final MultiStreamArgs multiStreamArgs = new MultiStreamArgs(isMultiStreamMode, shardDetector.streamIdentifier());
        final LeaseSynchronizer leaseSynchronizer = leaseRefresher.isLeaseTableEmpty() ?
                new EmptyLeaseTableSynchronizer() :
                new NonEmptyLeaseTableSynchronizer(shardDetector, shardIdToShardMap, shardIdToChildShardIdsMap);
        final List<Lease> newLeasesToCreate = determineNewLeasesToCreate(leaseSynchronizer, latestShards, currentLeases,
                initialPosition, inconsistentShardIds, multiStreamArgs);
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
        leaseSynchronizer.cleanupGarbageLeases(latestShards, leaseRefresher, trackedLeases, multiStreamArgs);
        if (cleanupLeasesOfCompletedShards) {
            leaseSynchronizer.cleanupLeasesOfFinishedShards(leaseRefresher, currentLeases, trackedLeases, multiStreamArgs);
        }
    }

    static List<Lease> determineNewLeasesToCreate(LeaseSynchronizer leaseSynchronizer, final List<Shard> shards,
                                                  final List<Lease> currentLeases,
                                                  final InitialPositionInStreamExtended initialPosition) {
        final Set<String> inconsistentShardIds = new HashSet<>();
        return determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, initialPosition, inconsistentShardIds);
    }

    static List<Lease> determineNewLeasesToCreate(LeaseSynchronizer leaseSynchronizer, final List<Shard> shards,
                                                  final List<Lease> currentLeases, final InitialPositionInStreamExtended initialPosition,
                                                  final Set<String> inconsistentShardIds) {
        return determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, initialPosition, inconsistentShardIds,
                new MultiStreamArgs(false, null));
    }

    static List<Lease> determineNewLeasesToCreate(LeaseSynchronizer leaseSynchronizer, final List<Shard> shards,
                                                  final List<Lease> currentLeases, final InitialPositionInStreamExtended initialPosition,
                                                  final Set<String> inconsistentShardIds, final MultiStreamArgs multiStreamArgs) {
        return leaseSynchronizer.determineNewLeasesToCreate(shards, currentLeases, initialPosition,inconsistentShardIds,
                multiStreamArgs);
    }

    // CHECKSTYLE:ON CyclomaticComplexity

    /** Note: This method has package level access solely for testing purposes.
     *
     * @param streamIdentifier We'll use this stream identifier to filter leases
     * @param leaseRefresher Used to fetch leases
     * @return Return list of leases (corresponding to shards) of the specified stream.
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     */
    static List<Lease> getLeasesForStream(StreamIdentifier streamIdentifier,
            LeaseRefresher leaseRefresher)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        List<Lease> streamLeases = new ArrayList<>();
        for (Lease lease : leaseRefresher.listLeases()) {
            if (streamIdentifier.serialize().equals(((MultiStreamLease)lease).streamIdentifier())) {
                streamLeases.add(lease);
            }
        }
        return streamLeases;
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

    static List<Shard> getShardListAtInitialPosition(@NonNull final ShardDetector shardDetector,
            InitialPositionInStreamExtended initialPositionInStreamExtended) throws KinesisClientLibIOException {
        final ShardFilter shardFilter = getShardFilterFromInitialPosition(initialPositionInStreamExtended);
        final Optional<List<Shard>> shards = Optional.of(shardDetector.listShardsWithFilter(shardFilter));

        log.debug("Retrieved {} shards with ShardFilter - {}.", shards.map(s -> s.size()).orElse(0), shardFilter);

        return shards.orElseThrow(() -> new KinesisClientLibIOException("Stream is not in ACTIVE OR UPDATING state - " +
                "will retry getting the shard list."));
    }

    static List<Shard> getFullShardList(@NonNull final ShardDetector shardDetector) throws KinesisClientLibIOException {
        final Optional<List<Shard>> shards = Optional.of(shardDetector.listShards());

        log.debug("Retrieved {} shards.", shards.map(s -> s.size()).orElse(0));

        return shards.orElseThrow(() -> new KinesisClientLibIOException("Stream is not in ACTIVE OR UPDATING state - " +
                "will retry getting the shard list."));
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
        boolean isCandidateForCleanup = true;

        final String shardId = shardIdFromLeaseDeducer.apply(lease, multiStreamArgs);

        if (currentKinesisShardIds.contains(shardId)) {
            isCandidateForCleanup = false;
        } else {
            log.info("Found lease for non-existent shard: {}. Checking its parent shards", shardId);
            final Set<String> parentShardIds = lease.parentShardIds();
            for (String parentShardId : parentShardIds) {
                
                // Throw an exception if the parent shard exists (but the child does not).
                // This may be a (rare) race condition between fetching the shard list and Kinesis expiring shards. 
                if (currentKinesisShardIds.contains(parentShardId)) {
                    final String message = String.format("Parent shard %s exists but not the child shard %s",
                            parentShardId, shardId);
                    log.info(message);
                    throw new KinesisClientLibIOException(message);
                }
            }
        }

        return isCandidateForCleanup;
    }
    
    /**
     * Helper method to create a new Lease POJO for a shard.
     * Note: Package level access only for testing purposes
     * 
     * @param shard
     * @return
     */
    static Lease newKCLLease(final Shard shard) {
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

    static Lease newKCLMultiStreamLease(final Shard shard, final StreamIdentifier streamIdentifier) {
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
    static List<Shard> getOpenShards(final List<Shard> allShards) {
        return allShards.stream().filter(shard -> shard.sequenceNumberRange().endingSequenceNumber() == null)
                .peek(shard -> log.debug("Found open shard: {}", shard.shardId())).collect(Collectors.toList());
    }

    static ExtendedSequenceNumber convertToCheckpoint(final InitialPositionInStreamExtended position) {
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
    static class StartingSequenceNumberAndShardIdBasedComparator implements Comparator<Lease>, Serializable {
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

}
