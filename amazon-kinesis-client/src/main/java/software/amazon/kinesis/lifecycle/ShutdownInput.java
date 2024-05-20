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

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;

/**
 * Container for the parameters to the IRecordProcessor's
 * {@link ShardRecordProcessor#shutdown(ShutdownInput
 * shutdownInput) shutdown} method.
 */
@Builder
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
public class ShutdownInput {

    /**
     * Get shutdown reason.
     *
     * -- GETTER --
     * @return Reason for the shutdown (ShutdownReason.SHARD_END indicates the shard is closed and there are no
     *         more records to process. Shutdown.LEASE_LOST indicates a fail over has occurred).
     */
    private final ShutdownReason shutdownReason;

    /**
     * Get Checkpointer.
     *
     * -- GETTER --
     * @return The checkpointer object that the record processor should use to checkpoint
     */
    private final RecordProcessorCheckpointer checkpointer;
}
