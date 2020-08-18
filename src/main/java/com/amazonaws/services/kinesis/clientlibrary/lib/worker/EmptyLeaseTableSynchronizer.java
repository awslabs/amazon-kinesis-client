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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisShardSyncer.StartingSequenceNumberAndShardIdBasedComparator;

/**
 * Class to help create leases when the table is initially empty.
 */
@AllArgsConstructor
class EmptyLeaseTableSynchronizer implements LeaseSynchronizer {

    private static final Log LOG = LogFactory.getLog(EmptyLeaseTableSynchronizer.class);

    /**
     * Determines how to create leases when the lease table is initially empty. For this, we read all shards where
     * the KCL is reading from. For any shards which are closed, we will discover their child shards through GetRecords
     * child shard information.
     *
     * @param shards
     * @param currentLeases
     * @param initialPosition
     * @param inconsistentShardIds
     * @return
     */
    @Override
    public List<KinesisClientLease> determineNewLeasesToCreate(List<Shard> shards,
                                                               List<KinesisClientLease> currentLeases,
                                                               InitialPositionInStreamExtended initialPosition,
                                                               Set<String> inconsistentShardIds) {

        final Map<String, Shard> shardIdToShardMapOfAllKinesisShards =
                KinesisShardSyncer.constructShardIdToShardMap(shards);

        currentLeases.forEach(lease -> LOG.debug("Existing lease: " + lease.getLeaseKey()));

        final List<KinesisClientLease> newLeasesToCreate =
                getLeasesToCreateForOpenAndClosedShards(initialPosition, shards);

        final Comparator<KinesisClientLease> startingSequenceNumberComparator =
                new StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMapOfAllKinesisShards);

        newLeasesToCreate.sort(startingSequenceNumberComparator);
        return newLeasesToCreate;
    }

    /**
     * Helper method to create leases. For an empty lease table, we will be creating leases for all shards
     * regardless of if they are open or closed. Closed shards will be unblocked via child shard information upon
     * reaching SHARD_END.
     */
    private List<KinesisClientLease> getLeasesToCreateForOpenAndClosedShards(
            InitialPositionInStreamExtended initialPosition,
            List<Shard> shards) {

        final Map<String, Lease> shardIdToNewLeaseMap = new HashMap<>();

        for (Shard shard : shards) {
            final String shardId = shard.getShardId();
            final KinesisClientLease lease = KinesisShardSyncer.newKCLLease(shard);

            final ExtendedSequenceNumber checkpoint = KinesisShardSyncer.convertToCheckpoint(initialPosition);
            lease.setCheckpoint(checkpoint);

            LOG.debug("Need to create a lease for shard with shardId " + shardId);
            shardIdToNewLeaseMap.put(shardId, lease);
        }

        return new ArrayList(shardIdToNewLeaseMap.values());
    }
}