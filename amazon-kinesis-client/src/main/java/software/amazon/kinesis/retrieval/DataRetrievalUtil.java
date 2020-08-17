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

package software.amazon.kinesis.retrieval;

import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.utils.CollectionUtils;

import java.util.List;

public class DataRetrievalUtil {

    public static boolean isValidResult(String shardEndIndicator, List<ChildShard> childShards) {
        // shardEndIndicator is nextShardIterator for GetRecordsResponse, and is continuationSequenceNumber for SubscribeToShardEvent
        // There are two valid scenarios for the shardEndIndicator and childShards combination.
        // 1. ShardEnd scenario: shardEndIndicator should be null and childShards should be a non-empty list.
        // 2. Non-ShardEnd scenario: shardEndIndicator should be non-null and childShards should be null or an empty list.
        // Otherwise, the retrieval result is invalid.
        if (shardEndIndicator == null && CollectionUtils.isNullOrEmpty(childShards) ||
                shardEndIndicator != null && !CollectionUtils.isNullOrEmpty(childShards)) {
            return false;
        }

        // For ShardEnd scenario, for each childShard we should validate if parentShards are available.
        // Missing parentShards can cause issues with creating leases for childShards during ShardConsumer shutdown.
        if (!CollectionUtils.isNullOrEmpty(childShards)) {
            for (ChildShard childShard : childShards) {
                if (CollectionUtils.isNullOrEmpty(childShard.parentShards())) {
                    return false;
                }
            }
        }
        return true;
    }
}
