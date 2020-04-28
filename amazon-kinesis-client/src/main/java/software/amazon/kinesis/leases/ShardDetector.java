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

import java.util.List;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.kinesis.common.StreamIdentifier;

/**
 *
 */
public interface ShardDetector {

    /**
     * Gets shard based on shardId.
     *
     * @param shardId
     * @return Shard
     */
    Shard shard(String shardId);

    /**
     * List shards.
     *
     * @return Shards
     */
    List<Shard> listShards();

    /**
     * List shards with shard filter.
     *
     * @param  ShardFilter
     * @return Shards
     */
    List<Shard> listShardsWithFilter(ShardFilter shardFilter);

    /**
     * Gets stream identifier.
     *
     * @return StreamIdentifier
     */
    default StreamIdentifier streamIdentifier() {
        throw new UnsupportedOperationException("StreamName not available");
    }

    /**
     * Gets a list shards response based on the request.
     *
     * @param request list shards request
     * @return ListShardsResponse which contains list shards response
     */
    ListShardsResponse getListShardsResponse(ListShardsRequest request) throws Exception;
}
