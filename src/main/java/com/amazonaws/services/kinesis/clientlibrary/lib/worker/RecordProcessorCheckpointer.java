/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;

/**
 * This class is used to enable RecordProcessors to checkpoint their progress.
 * The Amazon Kinesis Client Library will instantiate an object and provide a reference to the application
 * RecordProcessor instance. Amazon Kinesis Client Library will create one instance per shard assignment.
 */
class RecordProcessorCheckpointer implements IRecordProcessorCheckpointer {

    private static final Log LOG = LogFactory.getLog(RecordProcessorCheckpointer.class);

    private ICheckpoint checkpoint;

    private ExtendedSequenceNumber largestPermittedCheckpointValue;
    // Set to the last value set via checkpoint().
    // Sample use: verify application shutdown() invoked checkpoint() at the end of a shard.
    private ExtendedSequenceNumber lastCheckpointValue;

    private ShardInfo shardInfo;

    private SequenceNumberValidator sequenceNumberValidator;

    private ExtendedSequenceNumber sequenceNumberAtShardEnd;

    /**
     * Only has package level access, since only the Amazon Kinesis Client Library should be creating these.
     *
     * @param checkpoint Used to checkpoint progress of a RecordProcessor
     * @param validator Used for validating sequence numbers
     */
    RecordProcessorCheckpointer(ShardInfo shardInfo,
            ICheckpoint checkpoint,
            SequenceNumberValidator validator) {
        this.shardInfo = shardInfo;
        this.checkpoint = checkpoint;
        this.sequenceNumberValidator = validator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void checkpoint()
        throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Checkpointing " + shardInfo.getShardId() + ", " + " token " + shardInfo.getConcurrencyToken()
                    + " at largest permitted value " + this.largestPermittedCheckpointValue);
        }
        advancePosition(this.largestPermittedCheckpointValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void checkpoint(Record record)
        throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
        IllegalArgumentException {
        if (record == null) {
            throw new IllegalArgumentException("Could not checkpoint a null record");
        } else if (record instanceof UserRecord) {
            checkpoint(record.getSequenceNumber(), ((UserRecord) record).getSubSequenceNumber());
        } else {
            checkpoint(record.getSequenceNumber(), 0);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void checkpoint(String sequenceNumber)
        throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
        IllegalArgumentException {
        checkpoint(sequenceNumber, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void checkpoint(String sequenceNumber, long subSequenceNumber)
        throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
        IllegalArgumentException {

        if (subSequenceNumber < 0) {
            throw new IllegalArgumentException("Could not checkpoint at invalid, negative subsequence number "
                    + subSequenceNumber);
        }

        // throws exception if sequence number shouldn't be checkpointed for this shard
        sequenceNumberValidator.validateSequenceNumber(sequenceNumber);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Validated checkpoint sequence number " + sequenceNumber + " for " + shardInfo.getShardId()
                    + ", token " + shardInfo.getConcurrencyToken());
        }
        /*
         * If there isn't a last checkpoint value, we only care about checking the upper bound.
         * If there is a last checkpoint value, we want to check both the lower and upper bound.
         */
        ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber(sequenceNumber, subSequenceNumber);
        if ((lastCheckpointValue.compareTo(newCheckpoint) <= 0)
                && newCheckpoint.compareTo(largestPermittedCheckpointValue) <= 0) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("Checkpointing " + shardInfo.getShardId() + ", token " + shardInfo.getConcurrencyToken()
                        + " at specific extended sequence number " + newCheckpoint);
            }
            this.advancePosition(newCheckpoint);
        } else {
            throw new IllegalArgumentException(String.format(
                    "Could not checkpoint at extended sequence number %s as it did not fall into acceptable range "
                    + "between the last checkpoint %s and the greatest extended sequence number passed to this "
                    + "record processor %s",
                    newCheckpoint, this.lastCheckpointValue, this.largestPermittedCheckpointValue));
        }
    }

    /**
     * @return the lastCheckpointValue
     */
    ExtendedSequenceNumber getLastCheckpointValue() {
        return lastCheckpointValue;
    }

    synchronized void setInitialCheckpointValue(ExtendedSequenceNumber initialCheckpoint) {
        lastCheckpointValue = initialCheckpoint;
    }

    /**
     * Used for testing.
     *
     * @return the largest permitted checkpoint
     */
    synchronized ExtendedSequenceNumber getLargestPermittedCheckpointValue() {
        return largestPermittedCheckpointValue;
    }

    /**
     * @param checkpoint the checkpoint value to set
     */
    synchronized void setLargestPermittedCheckpointValue(ExtendedSequenceNumber largestPermittedCheckpointValue) {
        this.largestPermittedCheckpointValue = largestPermittedCheckpointValue;
    }

    /**
     * Used to remember the last extended sequence number before SHARD_END to allow us to prevent the checkpointer
     * from checkpointing at the end of the shard twice (i.e. at the last extended sequence number and then again
     * at SHARD_END).
     *
     * @param extendedSequenceNumber
     */
    synchronized void setSequenceNumberAtShardEnd(ExtendedSequenceNumber extendedSequenceNumber) {
        this.sequenceNumberAtShardEnd = extendedSequenceNumber;
    }


    /**
     * Internal API - has package level access only for testing purposes.
     *
     * @param sequenceNumber
     *
     * @throws KinesisClientLibDependencyException
     * @throws ThrottlingException
     * @throws ShutdownException
     * @throws InvalidStateException
     */
    void advancePosition(String sequenceNumber)
        throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        advancePosition(new ExtendedSequenceNumber(sequenceNumber));
    }

    void advancePosition(ExtendedSequenceNumber extendedSequenceNumber)
        throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        ExtendedSequenceNumber checkpointToRecord = extendedSequenceNumber;
        if (sequenceNumberAtShardEnd != null && sequenceNumberAtShardEnd.equals(extendedSequenceNumber)) {
            // If we are about to checkpoint the very last sequence number for this shard, we might as well
            // just checkpoint at SHARD_END
            checkpointToRecord = ExtendedSequenceNumber.SHARD_END;
        }
        // Don't checkpoint a value we already successfully checkpointed
        if (extendedSequenceNumber != null && !extendedSequenceNumber.equals(lastCheckpointValue)) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Setting " + shardInfo.getShardId() + ", token " + shardInfo.getConcurrencyToken()
                            + " checkpoint to " + checkpointToRecord);
                }
                checkpoint.setCheckpoint(shardInfo.getShardId(), checkpointToRecord, shardInfo.getConcurrencyToken());
                lastCheckpointValue = checkpointToRecord;
            } catch (ThrottlingException | ShutdownException | InvalidStateException
                    | KinesisClientLibDependencyException e) {
                throw e;
            } catch (KinesisClientLibException e) {
                LOG.warn("Caught exception setting checkpoint.", e);
                throw new KinesisClientLibDependencyException("Caught exception while checkpointing", e);
            }
        }
    }
}
