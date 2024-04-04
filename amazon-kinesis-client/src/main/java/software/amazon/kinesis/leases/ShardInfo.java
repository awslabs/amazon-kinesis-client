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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import lombok.AccessLevel;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Used to pass shard related info among different classes and as a key to the map of shard consumers.
 */
@Getter
@Accessors(fluent = true)
@ToString(exclude = {"isMultiStreamMode", "streamIdentifierStr"})
public class ShardInfo {

    private final String shardId;
    private final String concurrencyToken;
    // Sorted list of parent shardIds.
    private final List<String> parentShardIds;
    private final ExtendedSequenceNumber checkpoint;
    private final StreamConfig streamConfig;

    @Getter(AccessLevel.NONE)
    private final boolean isMultiStreamMode;
    @Getter(AccessLevel.NONE)
    private final String streamIdentifierStr;

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
     * @param streamConfig
     *            The {@link StreamConfig} instance for the stream that the shard belongs to
     */
    public ShardInfo(@NonNull final String shardId,
            final String concurrencyToken,
            final Collection<String> parentShardIds,
            final ExtendedSequenceNumber checkpoint,
            @NonNull final StreamConfig streamConfig) {
        this.shardId = shardId;
        this.concurrencyToken = concurrencyToken;
        this.parentShardIds = new LinkedList<>();
        if (parentShardIds != null) {
            this.parentShardIds.addAll(parentShardIds);
        }
        // ShardInfo stores parent shard Ids in canonical order in the parentShardIds list.
        // This makes it easy to check for equality in ShardInfo.equals method.
        Collections.sort(this.parentShardIds);
        this.checkpoint = checkpoint;
        this.streamConfig = streamConfig;
        this.isMultiStreamMode = streamConfig.streamIdentifier().isMultiStreamInstance();
        this.streamIdentifierStr = streamConfig.streamIdentifier().serialize();
    }

    /**
     * A list of shards that are parents of this shard. This may be empty if the shard has no parents.
     * 
     * @return a list of shardId's that are parents of this shard, or empty if the shard has no parents.
     */
    public List<String> parentShardIds() {
        return new LinkedList<>(parentShardIds);
    }

    /**
     * Whether the shard has been completely processed or not.
     *
     * @return completion status of the shard
     */
    public boolean isCompleted() {
        return ExtendedSequenceNumber.SHARD_END.equals(checkpoint);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(concurrencyToken).append(parentShardIds).append(shardId).append(streamIdentifierStr)
                .toHashCode();
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
                .append(parentShardIds, other.parentShardIds).append(shardId, other.shardId)
                .append(streamIdentifierStr, other.streamIdentifierStr).isEquals();

    }

    /**
     * Utility method to derive lease key from ShardInfo.
     * @param shardInfo
     * @return lease key
     */
    public static String getLeaseKey(ShardInfo shardInfo) {
        return getLeaseKey(shardInfo, shardInfo.shardId());
    }

    /**
     * Utility method to derive lease key from ShardInfo and shardId to override.
     * @param shardInfo
     * @param shardIdOverride
     * @return lease key
     */
    public static String getLeaseKey(ShardInfo shardInfo, String shardIdOverride) {
        return shardInfo.isMultiStreamMode ?
               MultiStreamLease.getLeaseKey(shardInfo.streamIdentifierStr, shardIdOverride) :
               shardIdOverride;
    }

}
