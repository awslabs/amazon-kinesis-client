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
package software.amazon.kinesis.checkpoint;

/**
 * Enumeration of the sentinel values of checkpoints.
 * Used during initialization of ShardConsumers to determine the starting point
 * in the shard and to flag that a shard has been completely processed.
 */
public enum SentinelCheckpoint {
    /**
     * Start from the first available record in the shard.
     */
    TRIM_HORIZON,
    /**
     * Start from the latest record in the shard.
     */
    LATEST,
    /**
     * We've completely processed all records in this shard.
     */
    SHARD_END,
    /**
     * Start from the record at or after the specified server-side timestamp.
     */
    AT_TIMESTAMP
}
