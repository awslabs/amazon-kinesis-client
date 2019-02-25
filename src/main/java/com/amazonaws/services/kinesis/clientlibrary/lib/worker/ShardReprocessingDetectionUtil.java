/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class removes shards from newLeasesToCreate that have:
 * a) Already begun processing, or
 * b) One of their descendants have begun processing.
 * If a lease for the shard itself, or any one of its descendants is found
 * in currentLeases, and the lease is not checkpointed at TRIM_HORIZON, LATEST,
 * or AT_TIMESTAMP, it is considered to have begun processing.
 * This class expects newLeasesToCreate to be sorted in increasing order of
 * sequence number range, i.e., from oldest ancestor to youngest descendants.
 */
public class ShardReprocessingDetectionUtil {
    private final List<KinesisClientLease> newLeasesToCreate;
    private final Map<String, Set<String>> shardToChildShard;

    private final Map<String, KinesisClientLease> currentShardToLease;
    private final Map<String, Boolean> memoizeIsBeingReprocessed;

    public ShardReprocessingDetectionUtil(List<KinesisClientLease> newLeasesToCreate,
            List<KinesisClientLease> currentLeases,
            Map<String, Set<String>> shardToChildShard) {
        this.newLeasesToCreate = newLeasesToCreate;
        this.shardToChildShard = shardToChildShard;
        this.currentShardToLease = new HashMap<>();
        this.memoizeIsBeingReprocessed = new HashMap<>();

        currentLeases.stream().forEach(currentLease ->
            currentShardToLease.put(currentLease.getLeaseKey(), currentLease)
        );
    }

    /**
     * This method iterates through every lease in the newLeasesToCreate list and determines if the shard is being
     * reprocessed by using a helper method hasShardBeenProcessed, in which case, it removes that shard from the
     * newLeasesToCreate.
     */
    public List<KinesisClientLease> removeShardsWhoseDescendantsExist() {
        return newLeasesToCreate.stream()
                .filter(lease -> !hasShardBeenProcessed(lease.getLeaseKey()))
                .collect(Collectors.toList());
    }

    /**
     * This helper method traverses through graph of descendant shards for the given shardId, to determine if
     * the given shard has been processed before. It does this by finding any descendant of the given shard
     * in the currentLeases.
     */
    private boolean hasShardBeenProcessed(final String shardId) {
        // If there's memoized result about this shard, return that.
        if (memoizeIsBeingReprocessed.containsKey(shardId)) {
            return memoizeIsBeingReprocessed.get(shardId);
        }

        // If a lease exists and it has begun processing, mark this shard as being processed.
        if (currentShardToLease.containsKey(shardId)
                && hasShardBegunProcessing(currentShardToLease.get(shardId))) {
            memoizeIsBeingReprocessed.put(shardId, true);
            return true;
        }

        if (shardToChildShard.containsKey(shardId)) {
            // For every child shard ...
            for (String childShard : shardToChildShard.get(shardId)) {
                // If child shard is being reprocessed, this shard is also being reprocessed.
                // Memoize the result and return true.
                if (hasShardBeenProcessed(childShard)) {
                    memoizeIsBeingReprocessed.put(shardId, true);
                    return true;
                }
            }
        }

        memoizeIsBeingReprocessed.put(shardId, false);
        return false;
    }


    /**
     * This helper function tells if the given shard has begun processing by checking the
     * checkpoint for its lease. If checkpoint is TRIM_HORIZON, LATEST or AT_TIMESTAMP,
     * it is safe to assume the lease hasn't begun processing yet.
     */
    private boolean hasShardBegunProcessing(KinesisClientLease lease) {
        return !lease.getCheckpoint().equals(ExtendedSequenceNumber.TRIM_HORIZON)
                && !lease.getCheckpoint().equals(ExtendedSequenceNumber.LATEST)
                && !lease.getCheckpoint().equals(ExtendedSequenceNumber.AT_TIMESTAMP);
    }
}
