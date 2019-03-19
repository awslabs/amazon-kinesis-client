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

package software.amazon.kinesis.lifecycle.events;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;

/**
 * Provides a checkpointer that <b>must</b> be used to signal the completion of the shard to the Scheduler.
 */
@Builder
@Accessors(fluent = true)
@Getter
@EqualsAndHashCode
@ToString
public class ShardEndedInput {

    /**
     * The checkpointer used to record that the record processor has completed the shard.
     *
     * The record processor <b>must</b> call {@link RecordProcessorCheckpointer#checkpoint()} before returning from
     * {@link ShardRecordProcessor#shardEnded(ShardEndedInput)}. Failing to do so will trigger the Scheduler to retry
     * shutdown until a successful checkpoint occurs.
     */
    private final RecordProcessorCheckpointer checkpointer;

}
