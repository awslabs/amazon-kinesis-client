/*
 * Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;

/**
 * This class is used to enable RecordProcessors to checkpoint their progress.
 * The Amazon Kinesis Client Library will instantiate an object and provide a reference to the application
 * RecordProcessor instance. Amazon Kinesis Client Library will create one instance per shard assignment.
 */
class RecordProcessorCheckpointer implements IRecordProcessorCheckpointer {

    private static final Log LOG = LogFactory.getLog(RecordProcessorCheckpointer.class);

    private ICheckpoint checkpoint;

    private String largestPermittedCheckpointValue;
    // Set to the last value set via checkpoint().
    // Sample use: verify application shutdown() invoked checkpoint() at the end of a shard.
    private String lastCheckpointValue;

    private ShardInfo shardInfo;

    private SequenceNumberValidator sequenceNumberValidator;

    private CheckpointValueComparator checkpointValueComparator;

    private String sequenceNumberAtShardEnd;

    /**
     * Only has package level access, since only the Amazon Kinesis Client Library should be creating these.
     * 
     * @param checkpoint Used to checkpoint progress of a RecordProcessor
     * @param validator Used for validating sequence numbers
     * @param comparator Used for checking the order of checkpoint values
     */
    RecordProcessorCheckpointer(ShardInfo shardInfo,
            ICheckpoint checkpoint,
            SequenceNumberValidator validator,
            CheckpointValueComparator comparator) {
        this.shardInfo = shardInfo;
        this.checkpoint = checkpoint;
        this.sequenceNumberValidator = validator;
        this.checkpointValueComparator = comparator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void checkpoint()
        throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        advancePosition(this.largestPermittedCheckpointValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void checkpoint(String sequenceNumber)
        throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException,
        IllegalArgumentException {

        // throws exception if sequence number shouldn't be checkpointed for this shard
        sequenceNumberValidator.validateSequenceNumber(sequenceNumber);
        /*
         * If there isn't a last checkpoint value, we only care about checking the upper bound.
         * If there is a last checkpoint value, we want to check both the lower and upper bound.
         */
        if ((lastCheckpointValue == null
                || checkpointValueComparator.compare(lastCheckpointValue, sequenceNumber) <= 0)
                && checkpointValueComparator.compare(sequenceNumber, largestPermittedCheckpointValue) <= 0) {

            this.advancePosition(sequenceNumber);
        } else {
            throw new IllegalArgumentException("Could not checkpoint at sequence number " + sequenceNumber
                    + " it did not fall into acceptable range between the last sequence number checkpointed "
                    + this.lastCheckpointValue + " and the greatest sequence number passed to this record processor "
                    + this.largestPermittedCheckpointValue);
        }

    }

    /**
     * @return the lastCheckpointValue
     */
    String getLastCheckpointValue() {
        return lastCheckpointValue;
    }

    /**
     * Used for testing.
     * 
     * @return the sequenceNumber
     */
    synchronized String getLargestPermittedCheckpointValue() {
        return largestPermittedCheckpointValue;
    }

    /**
     * @param largestPermittedCheckpointValue the checkpoint value to set
     */
    synchronized void setLargestPermittedCheckpointValue(String checkpointValue) {
        this.largestPermittedCheckpointValue = checkpointValue;
    }

    /**
     * Used to remember the last sequence number before SHARD_END to allow us to prevent the checkpointer from
     * checkpointing at the end of the shard twice (i.e. at the last sequence number and then again at SHARD_END).
     * 
     * @param sequenceNumber
     */
    synchronized void setSequenceNumberAtShardEnd(String sequenceNumber) {
        this.sequenceNumberAtShardEnd = sequenceNumber;
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
        String checkpointValue = sequenceNumber;
        if (sequenceNumberAtShardEnd != null && sequenceNumberAtShardEnd.equals(sequenceNumber)) {
            // If we are about to checkpoint the very last sequence number for this shard, we might as well
            // just checkpoint at SHARD_END
            checkpointValue = SentinelCheckpoint.SHARD_END.toString();
        }
        // Don't checkpoint a value we already successfully checkpointed
        if (sequenceNumber != null && !sequenceNumber.equals(lastCheckpointValue)) {
            try {
                checkpoint.setCheckpoint(shardInfo.getShardId(), checkpointValue, shardInfo.getConcurrencyToken());
                lastCheckpointValue = checkpointValue;
            } catch (ThrottlingException e) {
                throw e;
            } catch (ShutdownException e) {
                throw e;
            } catch (InvalidStateException e) {
                throw e;
            } catch (KinesisClientLibDependencyException e) {
                throw e;
            } catch (KinesisClientLibException e) {
                LOG.warn("Caught exception setting checkpoint.", e);
                throw new KinesisClientLibDependencyException("Caught exception while checkpointing", e);
            }
        }
    }
}
