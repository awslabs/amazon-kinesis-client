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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;


import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;

/**
 * Helper class to create Shard, SequenceRange and related objects.
 */
public class ShardObjectHelper {

    private static final int EXPONENT = 128;
    
    /**
     * Max value of a sequence number (2^128 -1). Useful for defining sequence number range for a shard.
     */
    static final String MAX_SEQUENCE_NUMBER = new BigInteger("2").pow(EXPONENT).subtract(BigInteger.ONE).toString();
    
    /**
     * Min value of a sequence number (0). Useful for defining sequence number range for a shard.
     */
    static final String MIN_SEQUENCE_NUMBER = BigInteger.ZERO.toString();

    /**
     * Max value of a hash key (2^128 -1). Useful for defining hash key range for a shard.
     */
    public static final String MAX_HASH_KEY = new BigInteger("2").pow(EXPONENT).subtract(BigInteger.ONE).toString();
    
    /**
     * Min value of a hash key (0). Useful for defining sequence number range for a shard.
     */
    public static final String MIN_HASH_KEY = BigInteger.ZERO.toString();

    /**
     * 
     */
    private ShardObjectHelper() {
    }

    /** Helper method to create a new shard object.
     * @param shardId
     * @param parentShardId
     * @param adjacentParentShardId
     * @param sequenceNumberRange
     * @return
     */
    static Shard newShard(String shardId,
            String parentShardId,
            String adjacentParentShardId,
            SequenceNumberRange sequenceNumberRange) {
        return newShard(shardId, parentShardId, adjacentParentShardId, sequenceNumberRange,
                HashKeyRange.builder().startingHashKey("1").endingHashKey("100").build());
    }

    /** Helper method to create a new shard object.
     * @param shardId
     * @param parentShardId
     * @param adjacentParentShardId
     * @param sequenceNumberRange
     * @param hashKeyRange
     * @return
     */
    public static Shard newShard(String shardId,
                                 String parentShardId,
                                 String adjacentParentShardId,
                                 SequenceNumberRange sequenceNumberRange,
                                 HashKeyRange hashKeyRange) {
        return Shard.builder().shardId(shardId).parentShardId(parentShardId)
                .adjacentParentShardId(adjacentParentShardId).sequenceNumberRange(sequenceNumberRange)
                .hashKeyRange(hashKeyRange).build();
    }

    /** Helper method.
     * @param startingSequenceNumber
     * @param endingSequenceNumber
     * @return
     */
    public static SequenceNumberRange newSequenceNumberRange(String startingSequenceNumber, String endingSequenceNumber) {
        return SequenceNumberRange.builder().startingSequenceNumber(startingSequenceNumber).endingSequenceNumber(endingSequenceNumber).build();
    }

    /** Helper method.
     * @param startingHashKey
     * @param endingHashKey
     * @return
     */
    public static HashKeyRange newHashKeyRange(String startingHashKey, String endingHashKey) {
        return HashKeyRange.builder().startingHashKey(startingHashKey).endingHashKey(endingHashKey).build();
    }
    
    static List<String> getParentShardIds(Shard shard) {
        List<String> parentShardIds = new ArrayList<>(2);
        if (shard.adjacentParentShardId() != null) {
            parentShardIds.add(shard.adjacentParentShardId());
        }
        if (shard.parentShardId() != null) {
            parentShardIds.add(shard.parentShardId());
        }
        return parentShardIds;
    }

}
