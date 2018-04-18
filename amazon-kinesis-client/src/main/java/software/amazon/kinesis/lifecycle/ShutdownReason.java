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
package software.amazon.kinesis.lifecycle;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;
import software.amazon.kinesis.processor.RecordProcessor;

import static software.amazon.kinesis.lifecycle.ConsumerStates.ConsumerState;
import static software.amazon.kinesis.lifecycle.ConsumerStates.ShardConsumerState;


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
    ZOMBIE(3, ShardConsumerState.SHUTTING_DOWN.getConsumerState()),

    /**
     * Terminate processing for this RecordProcessor (resharding use case).
     * Indicates that the shard is closed and all records from the shard have been delivered to the application.
     * Applications SHOULD checkpoint their progress to indicate that they have successfully processed all records
     * from this shard and processing of child shards can be started.
     */
    TERMINATE(2, ShardConsumerState.SHUTTING_DOWN.getConsumerState()),

    /**
     * Indicates that the entire application is being shutdown, and if desired the record processor will be given a
     * final chance to checkpoint. This state will not trigger a direct call to
     * {@link RecordProcessor#shutdown(ShutdownInput)}, but
     * instead depend on a different interface for backward compatibility.
     */
    REQUESTED(1, ShardConsumerState.SHUTDOWN_REQUESTED.getConsumerState());

    private final int rank;
    @Getter(AccessLevel.PACKAGE)
    @Accessors(fluent = true)
    private final ConsumerState shutdownState;

    ShutdownReason(int rank, ConsumerState shutdownState) {
        this.rank = rank;
        this.shutdownState = shutdownState;
    }

    /**
     * Indicates whether the given reason can override the current reason.
     * 
     * @param reason the reason to transition to
     * @return true if the transition is allowed, false if it's not.
     */
    public boolean canTransitionTo(ShutdownReason reason) {
        if (reason == null) {
            return false;
        }
        return reason.rank > this.rank;
    }
}
