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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.model.Shard;

import java.util.List;
import java.util.Set;

/**
 * Interface used by {@link KinesisShardSyncer} to determine how to create new leases based on the current state
 * of the lease table (i.e. whether the lease table is empty or non-empty).
 */
interface LeaseSynchronizer {

    /**
     * Determines how to create leases.
     * @param shards
     * @param currentLeases
     * @param initialPosition
     * @param inconsistentShardIds
     * @return
     */
    List<KinesisClientLease> determineNewLeasesToCreate(List<Shard> shards,
                                                        List<KinesisClientLease> currentLeases,
                                                        InitialPositionInStreamExtended initialPosition,
                                                        Set<String> inconsistentShardIds);
}