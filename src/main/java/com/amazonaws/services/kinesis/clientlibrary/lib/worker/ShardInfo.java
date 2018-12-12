/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

/**
 * Used to pass shard related info among different classes and as a key to the map of shard consumers.
 */
public class ShardInfo {

    private final String shardId;
    private final String concurrencyToken;
    // Sorted list of parent shardIds.
    private final List<String> parentShardIds;
    private final ExtendedSequenceNumber checkpoint;

    /**
     * Creates a new ShardInfo object. The checkpoint is not part of the equality, but is used for debugging output.
     * 
     * @param shardId
     *            Kinesis shardId that this will be about
     * @param concurrencyToken
     *            Used to differentiate between lost and reclaimed leases
     * @param parentShardIds
     *            Parent shards of the shard identified by Kinesis shardId
     * @param checkpoint
     *            the latest checkpoint from lease
     */
    public ShardInfo(String shardId,
            String concurrencyToken,
            Collection<String> parentShardIds,
            ExtendedSequenceNumber checkpoint) {
        this.shardId = shardId;
        this.concurrencyToken = concurrencyToken;
        this.parentShardIds = new LinkedList<String>();
        if (parentShardIds != null) {
            this.parentShardIds.addAll(parentShardIds);
        }
        // ShardInfo stores parent shard Ids in canonical order in the parentShardIds list.
        // This makes it easy to check for equality in ShardInfo.equals method.
        Collections.sort(this.parentShardIds);
        this.checkpoint = checkpoint;
    }

    /**
     * The shardId that this ShardInfo contains data about
     * 
     * @return the shardId
     */
    public String getShardId() {
        return shardId;
    }

    /**
     * Concurrency token for the lease that this shard is part of
     *
     * @return the concurrencyToken
     */
    public String getConcurrencyToken() {
        return concurrencyToken;
    }

    /**
     * A list of shards that are parents of this shard. This may be empty if the shard has no parents.
     * 
     * @return a list of shardId's that are parents of this shard, or empty if the shard has no parents.
     */
    protected List<String> getParentShardIds() {
        return new LinkedList<String>(parentShardIds);
    }

    /**
     * Whether the shard has been completely processed or not.
     *
     * @return completion status of the shard
     */
    protected boolean isCompleted() {
        return ExtendedSequenceNumber.SHARD_END.equals(checkpoint);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(concurrencyToken).append(parentShardIds).append(shardId).toHashCode();
    }

    /**
     * This method assumes parentShardIds is ordered. The Worker.cleanupShardConsumers() method relies on this method
     * returning true for ShardInfo objects which may have been instantiated with parentShardIds in a different order
     * (and rest of the fields being the equal). For example shardInfo1.equals(shardInfo2) should return true with
     * shardInfo1 and shardInfo2 defined as follows.
     * ShardInfo shardInfo1 = new ShardInfo(shardId1, concurrencyToken1, Arrays.asList("parent1", "parent2"));
     * ShardInfo shardInfo2 = new ShardInfo(shardId1, concurrencyToken1, Arrays.asList("parent2", "parent1"));
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ShardInfo other = (ShardInfo) obj;
        return new EqualsBuilder().append(concurrencyToken, other.concurrencyToken)
                .append(parentShardIds, other.parentShardIds).append(shardId, other.shardId).isEquals();

    }


    @Override
    public String toString() {
        return "ShardInfo [shardId=" + shardId + ", concurrencyToken=" + concurrencyToken + ", parentShardIds="
                + parentShardIds + ", checkpoint=" + checkpoint + "]";
    }



}
