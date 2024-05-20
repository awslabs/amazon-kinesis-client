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
package software.amazon.kinesis.checkpoint;

import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.processor.PreparedCheckpointer;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Objects of this class are prepared to checkpoint at a specific sequence number. They use an
 * RecordProcessorCheckpointer to do the actual checkpointing, so their checkpoint is subject to the same 'didn't go
 * backwards' validation as a normal checkpoint.
 */
public class ShardPreparedCheckpointer implements PreparedCheckpointer {

    private final ExtendedSequenceNumber pendingCheckpointSequenceNumber;
    private final RecordProcessorCheckpointer checkpointer;

    /**
     * Constructor.
     *
     * @param pendingCheckpointSequenceNumber sequence number to checkpoint at
     * @param checkpointer checkpointer to use
     */
    public ShardPreparedCheckpointer(
            ExtendedSequenceNumber pendingCheckpointSequenceNumber, RecordProcessorCheckpointer checkpointer) {
        this.pendingCheckpointSequenceNumber = pendingCheckpointSequenceNumber;
        this.checkpointer = checkpointer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExtendedSequenceNumber pendingCheckpoint() {
        return pendingCheckpointSequenceNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkpoint()
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
                    IllegalArgumentException {
        checkpointer.checkpoint(
                pendingCheckpointSequenceNumber.sequenceNumber(), pendingCheckpointSequenceNumber.subSequenceNumber());
    }
}
