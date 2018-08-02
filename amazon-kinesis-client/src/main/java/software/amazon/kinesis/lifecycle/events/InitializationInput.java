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
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Container for the parameters to the ShardRecordProcessor
 * {@link ShardRecordProcessor#initialize(InitializationInput initializationInput) initialize} method.
 */
@Builder
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
public class InitializationInput {
    /**
     * The shardId that the record processor is being initialized for.
     */
    private final String shardId;
    /**
     * The last extended sequence number that was successfully checkpointed by the previous record processor.
     */
    private final ExtendedSequenceNumber extendedSequenceNumber;
    /**
     * The pending extended sequence number that may have been started by the previous record processor.
     *
     * This will only be set if the previous record processor had prepared a checkpoint, but lost its lease before
     * completing the checkpoint.
     */
    private final ExtendedSequenceNumber pendingCheckpointSequenceNumber;
}
