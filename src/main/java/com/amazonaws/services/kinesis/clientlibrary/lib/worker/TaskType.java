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

/**
 * Enumerates types of tasks executed as part of processing a shard.
 */
public enum TaskType {
    /**
     * Polls and waits until parent shard(s) have been fully processed.
     */
    BLOCK_ON_PARENT_SHARDS,
    /**
     * Initialization of RecordProcessor (and Amazon Kinesis Client Library internal state for a shard).
     */
    INITIALIZE,
    /**
     * Fetching and processing of records.
     */
    PROCESS,
    /**
     * Shutdown of RecordProcessor.
     */
    SHUTDOWN,
    /**
     * Graceful shutdown has been requested, and notification of the record processor will occur.
     */
    SHUTDOWN_NOTIFICATION,
    /**
     * Occurs once the shutdown has been completed
     */
    SHUTDOWN_COMPLETE,
    /**
     * Sync leases/activities corresponding to Kinesis shards.
     */
    SHARDSYNC
}
