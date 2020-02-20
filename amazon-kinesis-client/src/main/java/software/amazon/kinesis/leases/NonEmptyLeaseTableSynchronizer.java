package software.amazon.kinesis.leases;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static software.amazon.kinesis.leases.HierarchicalShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors;
import static software.amazon.kinesis.leases.HierarchicalShardSyncer.constructShardIdToShardMap;

@Slf4j
@AllArgsConstructor
public class NonEmptyLeaseTableSynchronizer implements LeaseSynchronizer {

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
    public synchronized List<Lease> determineNewLeasesToCreate(List<Shard> shards, List<Lease> currentLeases,
                                                               InitialPositionInStreamExtended initialPosition, Set<String> inconsistentShardIds,
                                                               HierarchicalShardSyncer.MultiStreamArgs multiStreamArgs) {
        final Map<String, Lease> shardIdToNewLeaseMap = new HashMap<>();
        final Map<String, Shard> shardIdToShardMapOfAllKinesisShards = constructShardIdToShardMap(shards);

        final Set<String> shardIdsOfCurrentLeases = currentLeases.stream()
                .peek(lease -> log.debug("Existing lease: {}", lease))
                .map(lease -> shardIdFromLeaseDeducer.apply(lease, multiStreamArgs))
                .collect(Collectors.toSet());

        final List<Shard> openShards = HierarchicalShardSyncer.getOpenShards(shards);
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
                final Lease newLease = multiStreamArgs.isMultiStreamMode() ?
                        HierarchicalShardSyncer.newKCLMultiStreamLease(shard, multiStreamArgs.streamIdentifier()) :
                        HierarchicalShardSyncer.newKCLLease(shard);
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
                    newLease.checkpoint(HierarchicalShardSyncer.convertToCheckpoint(initialPosition));
                }
                log.debug("Set checkpoint of {} to {}", newLease.leaseKey(), newLease.checkpoint());
                shardIdToNewLeaseMap.put(shardId, newLease);
            }
        }

        final List<Lease> newLeasesToCreate = new ArrayList<>(shardIdToNewLeaseMap.values());
        final Comparator<Lease> startingSequenceNumberComparator = new HierarchicalShardSyncer.StartingSequenceNumberAndShardIdBasedComparator(
                shardIdToShardMapOfAllKinesisShards, multiStreamArgs);
        newLeasesToCreate.sort(startingSequenceNumberComparator);
        return newLeasesToCreate;
    }

    @Override
    public synchronized void cleanupGarbageLeases(List<Shard> shards, LeaseRefresher leaseRefresher,
                                                  List<Lease> trackedLeases, HierarchicalShardSyncer.MultiStreamArgs multiStreamArgs)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        final Set<String> kinesisShards = shards.stream().map(Shard::shardId).collect(Collectors.toSet());

        // Check if there are leases for non-existent shards
        final List<Lease> garbageLeases = trackedLeases.stream()
                .filter(lease -> HierarchicalShardSyncer.isCandidateForCleanup(lease, kinesisShards, multiStreamArgs)).collect(Collectors.toList());

        if (!CollectionUtils.isNullOrEmpty(garbageLeases)) {
            log.info("Found {} candidate leases for cleanup. Refreshing list of"
                    + " Kinesis shards to pick up recent/latest shards", garbageLeases.size());
            final Set<String> currentKinesisShardIds = HierarchicalShardSyncer.getFullShardList(shardDetector).stream().map(Shard::shardId)
                    .collect(Collectors.toSet());

            for (Lease lease : garbageLeases) {
                if (HierarchicalShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds, multiStreamArgs)) {
                    log.info("Deleting lease for shard {} as it is not present in Kinesis stream.", lease.leaseKey());
                    leaseRefresher.deleteLease(lease);
                }
            }
        }
    }

    @Override
    public synchronized void cleanupLeasesOfFinishedShards(LeaseRefresher leaseRefresher, List<Lease> currentLeases,
                                                           List<Lease> trackedLeases, HierarchicalShardSyncer.MultiStreamArgs multiStreamArgs)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        final List<Lease> leasesOfClosedShards = currentLeases.stream()
                .filter(lease -> lease.checkpoint().equals(ExtendedSequenceNumber.SHARD_END))
                .collect(Collectors.toList());
        final Set<String> shardIdsOfClosedShards = leasesOfClosedShards.stream()
                .map(lease -> shardIdFromLeaseDeducer.apply(lease, multiStreamArgs)).collect(Collectors.toSet());

        if (!CollectionUtils.isNullOrEmpty(leasesOfClosedShards)) {
            assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, shardIdsOfClosedShards);
            Comparator<? super Lease> startingSequenceNumberComparator =
                    new HierarchicalShardSyncer.StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMap, multiStreamArgs);
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
     * Note: this has package level access for testing purposes.
     * Useful for asserting that we don't have an incomplete shard list following a reshard operation.
     * We verify that if the shard is present in the shard list, it is closed and its hash key range
     *     is covered by its child shards.
     * @param shardIdsOfClosedShards Id of the shard which is expected to be closed
     * @return ShardIds of child shards (children of the expectedClosedShard)
     * @throws KinesisClientLibIOException
     */
    synchronized void assertClosedShardsAreCoveredOrAbsent(final Map<String, Shard> shardIdToShardMap,
                                                           final Map<String, Set<String>> shardIdToChildShardIdsMap,
                                                           final Set<String> shardIdsOfClosedShards)
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
                                                 final Map<String, Lease> trackedLeases, final LeaseRefresher leaseRefresher, final HierarchicalShardSyncer.MultiStreamArgs multiStreamArgs)
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
                        + "shards has begun.", shardIdFromLeaseDeducer.apply(leaseForClosedShard, multiStreamArgs));
                leaseRefresher.deleteLease(leaseForClosedShard);
            }
        }
    }

}
