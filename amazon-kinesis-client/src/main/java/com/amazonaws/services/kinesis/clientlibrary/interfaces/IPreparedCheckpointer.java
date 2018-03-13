package com.amazonaws.services.kinesis.clientlibrary.interfaces;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

/**
 * Objects of this class are prepared to checkpoint at a specific sequence number. They use an
 * IRecordProcessorCheckpointer to do the actual checkpointing, so their checkpoint is subject to the same 'didn't go
 * backwards' validation as a normal checkpoint.
 */
public interface IPreparedCheckpointer {

    /**
     * @return sequence number of pending checkpoint
     */
    ExtendedSequenceNumber getPendingCheckpoint();

    /**
     * This method will record a pending checkpoint.
     *
     * @throws ThrottlingException Can't store checkpoint. Can be caused by checkpointing too frequently.
     *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
     * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
     *         started processing some of these records already.
     *         The application should abort processing via this RecordProcessor instance.
     * @throws InvalidStateException Can't store checkpoint.
     *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
     * @throws KinesisClientLibDependencyException Encountered an issue when storing the checkpoint. The application can
     *         backoff and retry.
     * @throws IllegalArgumentException The sequence number being checkpointed is invalid because it is out of range,
     *         i.e. it is smaller than the last check point value (prepared or committed), or larger than the greatest
     *         sequence number seen by the associated record processor.
     */
    void checkpoint()
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
            IllegalArgumentException;

}