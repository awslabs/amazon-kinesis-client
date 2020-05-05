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

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.Lease;
import com.amazonaws.services.kinesis.model.Shard;
import lombok.AllArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TODO - non-empty lease table sync story
 */
@AllArgsConstructor
class NonEmptyLeaseTableSynchronizer implements LeaseSynchronizer {

    private static final Log LOG = LogFactory.getLog(NonEmptyLeaseTableSynchronizer.class);

    private final Map<String, Shard> shardIdToShardMap;
    private final Map<String, Set<String>> shardIdToChildShardIdsMap;

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
     * @param shards List of all shards in Kinesis (we'll create new leases based on this set)
     * @param currentLeases List of current leases
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @param inconsistentShardIds Set of child shard ids having open parents.
     * @return List of new leases to create sorted by starting sequenceNumber of the corresponding shard
     */
    @Override
    public List<KinesisClientLease> determineNewLeasesToCreate(List<Shard> shards,
                                                               List<KinesisClientLease> currentLeases,
                                                               InitialPositionInStreamExtended initialPosition,
                                                               Set<String> inconsistentShardIds) {

        Map<String, KinesisClientLease> shardIdToNewLeaseMap = new HashMap<>();
        Map<String, Shard> shardIdToShardMapOfAllKinesisShards = KinesisShardSyncer.constructShardIdToShardMap(shards);

        Set<String> shardIdsOfCurrentLeases = new HashSet<String>();
        for (Lease lease : currentLeases) {
            shardIdsOfCurrentLeases.add(lease.getLeaseKey());
            LOG.debug("Existing lease: " + lease);
        }

        List<Shard> openShards = KinesisShardSyncer.getOpenShards(shards);
        Map<String, Boolean> memoizationContext = new HashMap<>();

        // Iterate over the open shards and find those that don't have any lease entries.
        for (Shard shard : openShards) {
            String shardId = shard.getShardId();
            LOG.debug("Evaluating leases for open shard " + shardId + " and its ancestors.");
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                LOG.debug("Lease for shardId " + shardId + " already exists. Not creating a lease");
            } else if (inconsistentShardIds.contains(shardId)) {
                LOG.info("shardId " + shardId + " is an inconsistent child.  Not creating a lease");
            } else {
                LOG.debug("Need to create a lease for shardId " + shardId);
                KinesisClientLease newLease = KinesisShardSyncer.newKCLLease(shard);
                boolean isDescendant = KinesisShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId,
                        initialPosition, shardIdsOfCurrentLeases, shardIdToShardMapOfAllKinesisShards,
                        shardIdToNewLeaseMap, memoizationContext);

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
                if (isDescendant && !initialPosition.getInitialPositionInStream()
                        .equals(InitialPositionInStream.AT_TIMESTAMP)) {
                    newLease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                } else {
                    newLease.setCheckpoint(KinesisShardSyncer.convertToCheckpoint(initialPosition));
                }
                LOG.debug("Set checkpoint of " + newLease.getLeaseKey() + " to " + newLease.getCheckpoint());
                shardIdToNewLeaseMap.put(shardId, newLease);
            }
        }

        List<KinesisClientLease> newLeasesToCreate = new ArrayList<>();
        newLeasesToCreate.addAll(shardIdToNewLeaseMap.values());
        Comparator<? super KinesisClientLease> startingSequenceNumberComparator = new KinesisShardSyncer.StartingSequenceNumberAndShardIdBasedComparator(
                shardIdToShardMapOfAllKinesisShards);
        Collections.sort(newLeasesToCreate, startingSequenceNumberComparator);
        return newLeasesToCreate;
    }
}
