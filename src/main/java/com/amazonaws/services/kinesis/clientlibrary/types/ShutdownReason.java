/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.types;

/**
 * Reason the RecordProcessor is being shutdown.
 * Used to distinguish between a fail-over vs. a termination (shard is closed and all records have been delivered).
 * In case of a fail over, applications should NOT checkpoint as part of shutdown,
 * since another record processor may have already started processing records for that shard.
 * In case of termination (resharding use case), applications SHOULD checkpoint their progress to indicate
 * that they have successfully processed all the records (processing of child shards can then begin).
 */
public enum ShutdownReason {
    /**
     * Processing will be moved to a different record processor (fail over, load balancing use cases).
     * Applications SHOULD NOT checkpoint their progress (as another record processor may have already started
     * processing data).
     */
    ZOMBIE,

    /**
     * Terminate processing for this RecordProcessor (resharding use case).
     * Indicates that the shard is closed and all records from the shard have been delivered to the application.
     * Applications SHOULD checkpoint their progress to indicate that they have successfully processed all records
     * from this shard and processing of child shards can be started.
     */
    TERMINATE
}
