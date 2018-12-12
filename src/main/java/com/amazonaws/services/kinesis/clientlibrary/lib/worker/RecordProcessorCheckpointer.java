/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.impl.ThreadSafeMetricsDelegatingScope;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IPreparedCheckpointer;
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
    
    private IMetricsFactory metricsFactory;

    /**
     * Only has package level access, since only the Amazon Kinesis Client Library should be creating these.
     *
     * @param checkpoint Used to checkpoint progress of a RecordProcessor
     * @param validator Used for validating sequence numbers
     */
    RecordProcessorCheckpointer(ShardInfo shardInfo,
            ICheckpoint checkpoint,
            SequenceNumberValidator validator,
            IMetricsFactory metricsFactory) {
        this.shardInfo = shardInfo;
        this.checkpoint = checkpoint;
        this.sequenceNumberValidator = validator;
        this.metricsFactory = metricsFactory;
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
        if ((lastCheckpointValue == null || lastCheckpointValue.compareTo(newCheckpoint) <= 0)
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
     * {@inheritDoc}
     */
    @Override
    public synchronized IPreparedCheckpointer prepareCheckpoint()
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        return this.prepareCheckpoint(
                this.largestPermittedCheckpointValue.getSequenceNumber(),
                this.largestPermittedCheckpointValue.getSubSequenceNumber());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized IPreparedCheckpointer prepareCheckpoint(Record record)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        if (record == null) {
            throw new IllegalArgumentException("Could not prepare checkpoint a null record");
        } else if (record instanceof UserRecord) {
            return prepareCheckpoint(record.getSequenceNumber(), ((UserRecord) record).getSubSequenceNumber());
        } else {
            return prepareCheckpoint(record.getSequenceNumber(), 0);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized IPreparedCheckpointer prepareCheckpoint(String sequenceNumber)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
        return prepareCheckpoint(sequenceNumber, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized IPreparedCheckpointer prepareCheckpoint(String sequenceNumber, long subSequenceNumber)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {

        if (subSequenceNumber < 0) {
            throw new IllegalArgumentException("Could not checkpoint at invalid, negative subsequence number "
                    + subSequenceNumber);
        }

        // throws exception if sequence number shouldn't be checkpointed for this shard
        sequenceNumberValidator.validateSequenceNumber(sequenceNumber);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Validated prepareCheckpoint sequence number " + sequenceNumber + " for " + shardInfo.getShardId()
                    + ", token " + shardInfo.getConcurrencyToken());
        }
        /*
         * If there isn't a last checkpoint value, we only care about checking the upper bound.
         * If there is a last checkpoint value, we want to check both the lower and upper bound.
         */
        ExtendedSequenceNumber pendingCheckpoint = new ExtendedSequenceNumber(sequenceNumber, subSequenceNumber);
        if ((lastCheckpointValue == null || lastCheckpointValue.compareTo(pendingCheckpoint) <= 0)
                && pendingCheckpoint.compareTo(largestPermittedCheckpointValue) <= 0) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("Preparing checkpoint " + shardInfo.getShardId()
                        + ", token " + shardInfo.getConcurrencyToken()
                        + " at specific extended sequence number " + pendingCheckpoint);
            }
            return doPrepareCheckpoint(pendingCheckpoint);
        } else {
            throw new IllegalArgumentException(String.format(
                    "Could not prepare checkpoint at extended sequence number %s as it did not fall into acceptable "
                            + "range between the last checkpoint %s and the greatest extended sequence number passed "
                            + "to this record processor %s",
                    pendingCheckpoint, this.lastCheckpointValue, this.largestPermittedCheckpointValue));
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
     * @param largestPermittedCheckpointValue the largest permitted checkpoint
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
        
        boolean unsetMetrics = false;
        // Don't checkpoint a value we already successfully checkpointed
        try {
            if (!MetricsHelper.isMetricsScopePresent()) {
                MetricsHelper.setMetricsScope(new ThreadSafeMetricsDelegatingScope(metricsFactory.createMetrics()));
                unsetMetrics = true;
            }
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
        } finally {
            if (unsetMetrics) {
                MetricsHelper.unsetMetricsScope();
            }
        }
    }

    /**
     * This method stores the given sequenceNumber as a pending checkpooint in the lease table without overwriting the
     * current checkpoint, then returns a PreparedCheckpointer that is ready to checkpoint at the given sequence number.
     *
     * This method does not advance lastCheckpointValue, but calls to PreparedCheckpointer.checkpoint() on the returned
     * objects do. This allows customers to 'discard' prepared checkpoints by calling any of the 4 checkpoint methods on
     * this class before calling PreparedCheckpointer.checkpoint(). Some examples:
     *
     * 1) prepareCheckpoint(snA); checkpoint(snB). // this works regardless of whether snA or snB is bigger. It discards
     * the prepared checkpoint at snA.
     * 2) prepareCheckpoint(snA); prepareCheckpoint(snB). // this works regardless of whether snA or snB is bigger. It
     * replaces the preparedCheckpoint at snA with a new one at snB.
     * 3) checkpointerA = prepareCheckpoint(snA); checkpointerB = prepareCheckpoint(snB); checkpointerB.checkpoint();
     * checkpointerA.checkpoint(); // This replaces the prepared checkpoint at snA with a new one at snB, then
     * checkpoints at snB regardless of whether snA or snB is bigger. The checkpoint at snA only succeeds if snA > snB.
     *
     * @param extendedSequenceNumber the sequence number for the prepared checkpoint
     * @return a prepared checkpointer that is ready to checkpoint at the given sequence number.
     * @throws KinesisClientLibDependencyException
     * @throws InvalidStateException
     * @throws ThrottlingException
     * @throws ShutdownException
     */
    private IPreparedCheckpointer doPrepareCheckpoint(ExtendedSequenceNumber extendedSequenceNumber)
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {

        ExtendedSequenceNumber newPrepareCheckpoint = extendedSequenceNumber;
        if (sequenceNumberAtShardEnd != null && sequenceNumberAtShardEnd.equals(extendedSequenceNumber)) {
            // If we are about to checkpoint the very last sequence number for this shard, we might as well
            // just checkpoint at SHARD_END
            newPrepareCheckpoint = ExtendedSequenceNumber.SHARD_END;
        }

        // Don't actually prepare a checkpoint if they're trying to checkpoint at the current checkpointed value.
        // The only way this can happen is if they call prepareCheckpoint() in a record processor that was initialized
        // AND that has not processed any records since initialization.
        if (newPrepareCheckpoint.equals(lastCheckpointValue)) {
            return new DoesNothingPreparedCheckpointer(newPrepareCheckpoint);
        }

        try {
            checkpoint.prepareCheckpoint(shardInfo.getShardId(), newPrepareCheckpoint, shardInfo.getConcurrencyToken());
        } catch (ThrottlingException | ShutdownException | InvalidStateException
                | KinesisClientLibDependencyException e) {
            throw e;
        } catch (KinesisClientLibException e) {
            LOG.warn("Caught exception setting prepareCheckpoint.", e);
            throw new KinesisClientLibDependencyException("Caught exception while prepareCheckpointing", e);
        }

        PreparedCheckpointer result = new PreparedCheckpointer(newPrepareCheckpoint, this);
        return result;
    }
}
