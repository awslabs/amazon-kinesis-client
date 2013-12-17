/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

/**
 * This class is used to enable RecordProcessors to checkpoint their progress.
 * The Amazon Kinesis Client Library will instantiate an object and provide a reference to the application
 * RecordProcessor instance. Amazon Kinesis Client Library will create one instance per shard assignment.
 */
class RecordProcessorCheckpointer implements IRecordProcessorCheckpointer {

    private static final Log LOG = LogFactory.getLog(RecordProcessorCheckpointer.class);

    private ICheckpoint checkpoint;

    private String sequenceNumber;
    // Set to the last value set via checkpoint().
    // Sample use: verify application shutdown() invoked checkpoint() at the end of a shard.
    private String lastCheckpointValue;

    private ShardInfo shardInfo;

    /**
     * Only has package level access, since only the Amazon Kinesis Client Library should be creating these.
     * 
     * @param checkpoint Used to checkpoint progress of a RecordProcessor
     */
    RecordProcessorCheckpointer(ShardInfo shardInfo, ICheckpoint checkpoint) {
        this.shardInfo = shardInfo;
        this.checkpoint = checkpoint;
    }

    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer#checkpoint()
     */
    @Override
    public synchronized void checkpoint()
        throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        advancePosition();
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
    synchronized String getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * @param maxSequenceNumber the sequenceNumber to set
     */
    synchronized void setSequenceNumber(String sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Internal API - has package level access only for testing purposes.
     * 
     * @throws KinesisClientLibDependencyException
     * @throws ThrottlingException
     * @throws ShutdownException
     * @throws InvalidStateException
     */
    void advancePosition()
        throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        try {
            checkpoint.setCheckpoint(shardInfo.getShardId(), sequenceNumber, shardInfo.getConcurrencyToken());
            lastCheckpointValue = sequenceNumber;
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
