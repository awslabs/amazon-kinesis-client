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

import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;

@KinesisClientInternalApi
public interface ConsumerTaskFactory {
    /**
     * Creates a shutdown task.
     */
    ConsumerTask createShutdownTask(ShardConsumerArgument argument, ShardConsumer consumer, ProcessRecordsInput input);

    /**
     * Creates a process task.
     */
    ConsumerTask createProcessTask(ShardConsumerArgument argument, ProcessRecordsInput processRecordsInput);

    /**
     * Creates an initialize task.
     */
    ConsumerTask createInitializeTask(ShardConsumerArgument argument);

    /**
     * Creates a block on parent task.
     */
    ConsumerTask createBlockOnParentTask(ShardConsumerArgument argument);

    /**
     * Creates a shutdown notification task.
     */
    ConsumerTask createShutdownNotificationTask(ShardConsumerArgument argument, ShardConsumer consumer);
}
