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
     * determine if it is a descendant of any shard which is or will be processed (e.g. for which a lease exists):
     * If so, create a lease for the first ancestor that needs to be processed (if needed). We will create leases
     * for no more than one level in the ancestry tree. Once we find the first ancestor that needs to be processed,
     * we will avoid creating leases for further descendants of that ancestor.
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
     *  \  /   |  / \
     *   8     4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     *
     * Current leases: (4, 5, 7)
     *
     * If initial position is LATEST:
     *   - New leases to create: (6)
     * If initial position is TRIM_HORIZON:
     *   - New leases to create: (0, 1)
     * If initial position is AT_TIMESTAMP(epoch=200):
     *   - New leases to create: (0, 1)
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
        final KinesisShardSyncer.MemoizationContext memoizationContext = new KinesisShardSyncer.MemoizationContext();


        // Iterate over the open shards and find those that don't have any lease entries.
        for (Shard shard : openShards) {
            String shardId = shard.getShardId();
            LOG.debug("Evaluating leases for open shard " + shardId + " and its ancestors.");
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                LOG.debug("Lease for shardId " + shardId + " already exists. Not creating a lease");
            } else if (inconsistentShardIds.contains(shardId)) {
                LOG.info("shardId " + shardId + " is an inconsistent child.  Not creating a lease");
            } else {
                LOG.debug("Beginning traversal of ancestry tree for shardId " + shardId);

                // A shard is a descendant if at least one if its ancestors exists in the lease table.
                // We will create leases for only one level in the ancestry tree. Once we find the first ancestor
                // that needs to be processed in order to complete the hash range, we will not create leases for
                // further descendants of that ancestor.
                boolean isDescendant = KinesisShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId,
                        initialPosition, shardIdsOfCurrentLeases, shardIdToShardMapOfAllKinesisShards,
                        shardIdToNewLeaseMap, memoizationContext);

                // If shard is a descendant, the leases for its ancestors were already created above. Open shards
                // that are NOT descendants will not have leases yet, so we create them here. We will not create
                // leases for open shards that ARE descendants yet - leases for these shards will be created upon
                // SHARD_END of their parents.
                if (!isDescendant) {
                    LOG.debug("ShardId " + shardId + " has no ancestors. Creating a lease.");
                    final KinesisClientLease newLease = KinesisShardSyncer.newKCLLease(shard);
                    newLease.setCheckpoint(KinesisShardSyncer.convertToCheckpoint(initialPosition));
                    LOG.debug("Set checkpoint of " + newLease.getLeaseKey() + " to " + newLease.getCheckpoint());
                    shardIdToNewLeaseMap.put(shardId, newLease);
                } else {
                    LOG.debug("ShardId " + shardId + " is a descendant whose ancestors should already have leases. " +
                            "Not creating a lease.");
                }
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
