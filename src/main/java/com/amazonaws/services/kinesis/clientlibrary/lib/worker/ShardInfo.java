/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Used to pass shard related info among different classes and as a key to the map of shard consumers.
 */
class ShardInfo {
    
    private final String shardId;
    private final String concurrencyToken;
    // Sorted list of parent shardIds.
    private final List<String> parentShardIds;

    /**
     * @param shardId Kinesis shardId
     * @param concurrencyToken Used to differentiate between lost and reclaimed leases
     * @param parentShardIds Parent shards of the shard identified by Kinesis shardId
     */
    public ShardInfo(String shardId, String concurrencyToken, Collection<String> parentShardIds) {
        this.shardId = shardId;
        this.concurrencyToken = concurrencyToken;
        this.parentShardIds = new LinkedList<String>();
        if (parentShardIds != null) {
            this.parentShardIds.addAll(parentShardIds);
        }
        Collections.sort(this.parentShardIds);
    }

    /**
     * @return the shardId
     */
    protected String getShardId() {
        return shardId;
    }

    /**
     * @return the concurrencyToken
     */
    protected String getConcurrencyToken() {
        return concurrencyToken;
    }

    /**
     * @return the parentShardIds
     */
    protected List<String> getParentShardIds() {
        return new LinkedList<String>(parentShardIds);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((concurrencyToken == null) ? 0 : concurrencyToken.hashCode());
        result = prime * result + ((parentShardIds == null) ? 0 : parentShardIds.hashCode());
        result = prime * result + ((shardId == null) ? 0 : shardId.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    // CHECKSTYLE:OFF NPathComplexity
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
        if (concurrencyToken == null) {
            if (other.concurrencyToken != null) {
                return false;
            }
        } else if (!concurrencyToken.equals(other.concurrencyToken)) {
            return false;
        }
        if (parentShardIds == null) {
            if (other.parentShardIds != null) {
                return false;
            }
        } else if (!parentShardIds.equals(other.parentShardIds)) {
            return false;
        }
        if (shardId == null) {
            if (other.shardId != null) {
                return false;
            }
        } else if (!shardId.equals(other.shardId)) {
            return false;
        }
        return true;
    }
    // CHECKSTYLE:ON CyclomaticComplexity
    // CHECKSTYLE:ON NPathComplexity
        

}
