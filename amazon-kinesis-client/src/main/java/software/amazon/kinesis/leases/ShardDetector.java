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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import software.amazon.awssdk.services.kinesis.model.ChildShard;
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
     * This method behaves exactly similar to listShards except the fact that this does not consume and throw
     * ResourceNotFoundException instead of returning empty list.
     *
     * @return Shards
     */
    default List<Shard> listShardsWithoutConsumingResourceNotFoundException() {
        throw new UnsupportedOperationException("listShardsWithoutConsumingResourceNotFoundException not implemented");
    }

    /**
     * List shards with shard filter.
     *
     * @param shardFilter
     * @return Shards
     */
    default List<Shard> listShardsWithFilter(ShardFilter shardFilter) {
        throw new UnsupportedOperationException("listShardsWithFilter not available.");
    }

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
    default ListShardsResponse getListShardsResponse(ListShardsRequest request) throws Exception {
        throw new UnsupportedOperationException("getListShardsResponse not available.");
    }

    /**
     * Gets the children shards of a shard.
     * @param shardId
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    default List<ChildShard> getChildShards(String shardId) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException("getChildShards not available.");
    }
}
