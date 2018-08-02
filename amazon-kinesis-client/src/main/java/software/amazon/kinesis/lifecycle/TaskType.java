/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.lifecycle;

/**
 * Enumerates types of tasks executed as part of processing a shard.
 */
public enum TaskType {
    /**
     * Polls and waits until parent shard(s) have been fully processed.
     */
    BLOCK_ON_PARENT_SHARDS,
    /**
     * Initialization of ShardRecordProcessor (and Amazon Kinesis Client Library internal state for a shard).
     */
    INITIALIZE,
    /**
     * Fetching and processing of records.
     */
    PROCESS,
    /**
     * Shutdown of ShardRecordProcessor.
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
