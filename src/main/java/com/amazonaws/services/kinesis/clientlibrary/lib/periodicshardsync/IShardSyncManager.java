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
package com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync;

import com.amazonaws.services.kinesis.leases.impl.Lease;

/**
 * ShardSyncManager interface which exposes methods to start and stop the Sync
 * Manager and also get the associated LeaderPoller
 * 
 * @param <T>
 */
public interface IShardSyncManager<T extends Lease> {
    /**
     * Starts the ShardSyncManager
     */
    public void start();

    /**
     * Stops the ShardSyncManager
     */
    public void stop();

    /**
     * @return Returns the associated LeaderPoller
     */
    LeaderPoller<T> getLeaderPoller();
}
