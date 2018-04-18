/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import software.amazon.kinesis.checkpoint.Checkpoint;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import lombok.extern.slf4j.Slf4j;

/**
 * Everything is stored in memory and there is no fault-tolerance.
 */
@Slf4j
public class InMemoryCheckpointer implements Checkpointer {
    private Map<String, ExtendedSequenceNumber> checkpoints = new HashMap<>();
    private Map<String, ExtendedSequenceNumber> flushpoints = new HashMap<>();
    private Map<String, ExtendedSequenceNumber> pendingCheckpoints = new HashMap<>();
    private final String startingSequenceNumber;

    /**
     * Constructor.
     *
     * @param startingSequenceNumber Initial checkpoint will be set to this sequenceNumber (for all shards).
     */
    public InMemoryCheckpointer(String startingSequenceNumber) {
        super();
        this.startingSequenceNumber = startingSequenceNumber;
    }

    ExtendedSequenceNumber getLastCheckpoint(String shardId) {
        ExtendedSequenceNumber checkpoint = checkpoints.get(shardId);
        if (checkpoint == null) {
            checkpoint = new ExtendedSequenceNumber(startingSequenceNumber);
        }
        log.debug("getLastCheckpoint shardId: {} checkpoint: {}", shardId, checkpoint);
        return checkpoint;
    }

    ExtendedSequenceNumber getLastFlushpoint(String shardId) {
        ExtendedSequenceNumber flushpoint = flushpoints.get(shardId);
        log.debug("getLastFlushpoint shardId: {} flushpoint: {}", shardId, flushpoint);
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
        if (log.isDebugEnabled()) {
            log.debug("getGreatestPrimaryFlushpoint value for shardId {} = {}", shardId, greatestFlushpoint);
        }
        return greatestFlushpoint;
    };

    ExtendedSequenceNumber getRestartPoint(String shardId) {
        verifyNotEmpty(shardId, "shardId must not be null.");
        ExtendedSequenceNumber restartPoint = getLastCheckpoint(shardId);
        if (log.isDebugEnabled()) {
            log.debug("getRestartPoint value for shardId {} = {}", shardId, restartPoint);
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

        if (log.isDebugEnabled()) {
            log.debug("shardId: {} checkpoint: {}", shardId, checkpointValue);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExtendedSequenceNumber getCheckpoint(String shardId) throws KinesisClientLibException {
        ExtendedSequenceNumber checkpoint = flushpoints.get(shardId);
        log.debug("getCheckpoint shardId: {} checkpoint: {}",  shardId, checkpoint);
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
        log.debug("getCheckpointObject shardId: {}, {}", shardId, checkpointObj);
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
