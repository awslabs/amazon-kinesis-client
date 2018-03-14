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
package software.amazon.kinesis.leases;

import java.util.List;

/**
 * Shard Prioritization that returns the same original list of shards without any modifications.
 */
public class NoOpShardPrioritization implements
        ShardPrioritization {

    /**
     * Empty constructor for NoOp Shard Prioritization.
     */
    public NoOpShardPrioritization() {
    }

    @Override
    public List<ShardInfo> prioritize(List<ShardInfo> original) {
        return original;
    }
}
