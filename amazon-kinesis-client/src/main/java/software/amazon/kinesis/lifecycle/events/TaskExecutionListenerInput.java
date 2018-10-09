/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.lifecycle.events;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.TaskOutcome;
import software.amazon.kinesis.lifecycle.TaskType;
import software.amazon.kinesis.lifecycle.TaskExecutionListener;

/**
 * Container for the parameters to the TaskExecutionListener's
 * {@link TaskExecutionListener#beforeTaskExecution(TaskExecutionListenerInput)} method.
 * {@link TaskExecutionListener#afterTaskExecution(TaskExecutionListenerInput)} method.
 */
@Data
@Builder(toBuilder = true)
@Accessors(fluent = true)
public class TaskExecutionListenerInput {
    /**
     * Detailed information about the shard whose progress is monitored by TaskExecutionListener.
     */
    private final ShardInfo shardInfo;
    /**
     * The type of task being executed for the shard.
     *
     * This corresponds to the state the shard is in.
     */
    private final TaskType taskType;
    /**
     * Outcome of the task execution for the shard.
     */
    private final TaskOutcome taskOutcome;
}
