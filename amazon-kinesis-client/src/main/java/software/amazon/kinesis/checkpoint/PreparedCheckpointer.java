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
package software.amazon.kinesis.checkpoint;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import software.amazon.kinesis.processor.IPreparedCheckpointer;
import software.amazon.kinesis.processor.IRecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Objects of this class are prepared to checkpoint at a specific sequence number. They use an
 * IRecordProcessorCheckpointer to do the actual checkpointing, so their checkpoint is subject to the same 'didn't go
 * backwards' validation as a normal checkpoint.
 */
public class PreparedCheckpointer implements IPreparedCheckpointer {

    private final ExtendedSequenceNumber pendingCheckpointSequenceNumber;
    private final IRecordProcessorCheckpointer checkpointer;

    /**
     * Constructor.
     *
     * @param pendingCheckpointSequenceNumber sequence number to checkpoint at
     * @param checkpointer checkpointer to use
     */
    public PreparedCheckpointer(ExtendedSequenceNumber pendingCheckpointSequenceNumber,
                                IRecordProcessorCheckpointer checkpointer) {
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
        checkpointer.checkpoint(pendingCheckpointSequenceNumber.getSequenceNumber(),
                pendingCheckpointSequenceNumber.getSubSequenceNumber());
    }
}
