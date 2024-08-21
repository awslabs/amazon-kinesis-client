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
