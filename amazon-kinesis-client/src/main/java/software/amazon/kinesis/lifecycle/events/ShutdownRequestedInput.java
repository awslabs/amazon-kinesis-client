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
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;

/**
 * Provides access to a checkpointer so that {@link ShardRecordProcessor}'s can checkpoint
 * before the lease is released during shutdown.
 */
@Builder
@Accessors(fluent = true)
@Getter
@EqualsAndHashCode
@ToString
public class ShutdownRequestedInput {
    /**
     * Checkpointer used to record the current progress of the
     * {@link ShardRecordProcessor}.
     */
    private final RecordProcessorCheckpointer checkpointer;
}
