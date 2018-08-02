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
