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
 * Provides logic to prioritize or filter shards before their execution.
 */
public interface ShardPrioritization {

    /**
     * Returns new list of shards ordered based on their priority.
     * Resulted list may have fewer shards compared to original list
     * 
     * @param original
     *            list of shards needed to be prioritized
     * @return new list that contains only shards that should be processed
     */
    List<ShardInfo> prioritize(List<ShardInfo> original);
}
