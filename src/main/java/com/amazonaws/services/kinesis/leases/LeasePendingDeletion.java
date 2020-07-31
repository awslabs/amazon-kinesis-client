/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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
package com.amazonaws.services.kinesis.leases;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * Helper class for cleaning up leases.
 */
@Accessors(fluent=true)
@Value
public class LeasePendingDeletion {
    private final KinesisClientLease lease;
    private final ShardInfo shardInfo;
}

