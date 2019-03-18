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
package com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

/**
 * Everything is stored in memory and there is no fault-tolerance.
 */
public class InMemoryCheckpointImpl implements ICheckpoint {

    private static final Log LOG = LogFactory.getLog(InMemoryCheckpointImpl.class);

    private Map<String, ExtendedSequenceNumber> checkpoints = new HashMap<>();
    private Map<String, ExtendedSequenceNumber> flushpoints = new HashMap<>();
    private Map<String, ExtendedSequenceNumber> pendingCheckpoints = new HashMap<>();
    private final String startingSequenceNumber;

    /**
     * Constructor.
     *
     * @param startingSequenceNumber Initial checkpoint will be set to this sequenceNumber (for all shards).
     */
    public InMemoryCheckpointImpl(String startingSequenceNumber) {
        super();
        this.startingSequenceNumber = startingSequenceNumber;
    }

    ExtendedSequenceNumber getLastCheckpoint(String shardId) {
        ExtendedSequenceNumber checkpoint = checkpoints.get(shardId);
        if (checkpoint == null) {
            checkpoint = new ExtendedSequenceNumber(startingSequenceNumber);
        }
        LOG.debug("getLastCheckpoint shardId: " + shardId + " checkpoint: " + checkpoint);
        return checkpoint;
    }

    ExtendedSequenceNumber getLastFlushpoint(String shardId) {
        ExtendedSequenceNumber flushpoint = flushpoints.get(shardId);
        LOG.debug("getLastFlushpoint shardId: " + shardId + " flushpoint: " + flushpoint);
        return flushpoint;
    }

    void resetCheckpointToLastFlushpoint(String shardId) throws KinesisClientLibException {
        ExtendedSequenceNumber currentFlushpoint = flushpoints.get(shardId);
        if (currentFlushpoint == null) {
            checkpoints.put(shardId, new ExtendedSequenceNumber(startingSequenceNumber));
        } else {
            checkpoints.put(shardId, currentFlushpoint);
        }
    }

    ExtendedSequenceNumber getGreatestPrimaryFlushpoint(String shardId) throws KinesisClientLibException {
        verifyNotEmpty(shardId, "shardId must not be null.");
        ExtendedSequenceNumber greatestFlushpoint = getLastFlushpoint(shardId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("getGreatestPrimaryFlushpoint value for shardId " + shardId + " = " + greatestFlushpoint);
        }
        return greatestFlushpoint;
    };

    ExtendedSequenceNumber getRestartPoint(String shardId) {
        verifyNotEmpty(shardId, "shardId must not be null.");
        ExtendedSequenceNumber restartPoint = getLastCheckpoint(shardId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("getRestartPoint value for shardId " + shardId + " = " + restartPoint);
        }
        return restartPoint;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCheckpoint(String shardId, ExtendedSequenceNumber checkpointValue, String concurrencyToken)
        throws KinesisClientLibException {
        checkpoints.put(shardId, checkpointValue);
        flushpoints.put(shardId, checkpointValue);
        pendingCheckpoints.remove(shardId);

        if (LOG.isDebugEnabled()) {
            LOG.debug("shardId: " + shardId + " checkpoint: " + checkpointValue);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExtendedSequenceNumber getCheckpoint(String shardId) throws KinesisClientLibException {
        ExtendedSequenceNumber checkpoint = flushpoints.get(shardId);
        LOG.debug("getCheckpoint shardId: " + shardId + " checkpoint: " + checkpoint);
        return checkpoint;
    }

    @Override
    public void prepareCheckpoint(String shardId, ExtendedSequenceNumber pendingCheckpoint, String concurrencyToken)
            throws KinesisClientLibException {
        pendingCheckpoints.put(shardId, pendingCheckpoint);
    }

    @Override
    public Checkpoint getCheckpointObject(String shardId) throws KinesisClientLibException {
        ExtendedSequenceNumber checkpoint = flushpoints.get(shardId);
        ExtendedSequenceNumber pendingCheckpoint = pendingCheckpoints.get(shardId);

        Checkpoint checkpointObj = new Checkpoint(checkpoint, pendingCheckpoint);
        LOG.debug("getCheckpointObject shardId: " + shardId + ", " + checkpointObj);
        return checkpointObj;
    }

    /** Check that string is neither null nor empty.
     */
    static void verifyNotEmpty(String string, String message) {
        if ((string == null) || (string.isEmpty())) {
            throw new IllegalArgumentException(message);
        }
    }

}
