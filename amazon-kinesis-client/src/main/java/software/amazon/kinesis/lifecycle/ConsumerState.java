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

package software.amazon.kinesis.lifecycle;

import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;

/**
 * Represents a the current state of the consumer. This handles the creation of tasks for the consumer, and what to
 * do when a transition occurs.
 *
 */
interface ConsumerState {
    /**
     * Creates a new task for this state using the passed in consumer to build the task. If there is no task
     * required for this state it may return a null value. {@link ConsumerState}'s are allowed to modify the
     * consumer during the execution of this method.
     *
     * @param consumerArgument
     *            configuration specific to the task being created
     * @param consumer
     *            the consumer to use build the task, or execute state.
     * @param input
     *            the process input received, this may be null if it's a control message
     * @return a valid task for this state or null if there is no task required.
     */
    ConsumerTask createTask(ShardConsumerArgument consumerArgument, ShardConsumer consumer, ProcessRecordsInput input);

    /**
     * Provides the next state of the consumer upon success of the task return by
     * {@link ConsumerState#createTask(ShardConsumerArgument, ShardConsumer, ProcessRecordsInput)}.
     *
     * @return the next state that the consumer should transition to, this may be the same object as the current
     *         state.
     */
    ConsumerState successTransition();

    /**
     * Provides the next state of the consumer if the task failed. This defaults to no state change.
     * 
     * @return the state to change to upon a task failure
     */
    default ConsumerState failureTransition() {
        return this;
    }

    /**
     * Provides the next state of the consumer when a shutdown has been requested. The returned state is dependent
     * on the current state, and the shutdown reason.
     *
     * @param shutdownReason
     *            the reason that a shutdown was requested
     * @return the next state that the consumer should transition to, this may be the same object as the current
     *         state.
     */
    ConsumerState shutdownTransition(ShutdownReason shutdownReason);

    /**
     * The type of task that {@link ConsumerState#createTask(ShardConsumerArgument, ShardConsumer, ProcessRecordsInput)}
     * would return. This is always a valid state
     * even if createTask would return a null value.
     *
     * @return the type of task that this state represents.
     */
    TaskType taskType();

    /**
     * An enumeration represent the type of this state. Different consumer states may return the same
     * {@link ConsumerStates.ShardConsumerState}.
     *
     * @return the type of consumer state this represents.
     */
    ConsumerStates.ShardConsumerState state();

    boolean isTerminal();

    /**
     * Whether this state requires data to be available before the task can be created
     *
     * @return true if the task requires data to be available before creation, false otherwise
     */
    default boolean requiresDataAvailability() {
        return false;
    }

    /**
     * Indicates whether a state requires an external event to re-awaken for processing.
     * 
     * @return true if the state is some external event to restart processing, false if events can be immediately
     *         dispatched.
     */
    default boolean requiresAwake() {
        return false;
    }

}
