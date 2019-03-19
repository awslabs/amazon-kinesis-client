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

import java.util.HashMap;
import java.util.Map;

import software.amazon.kinesis.exceptions.KinesisClientLibException;
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

    private String operation;

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
        log.debug("checkpoint shardId: {} checkpoint: {}",  shardId, checkpoint);
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

    @Override
    public void operation(final String operation) {
        this.operation = operation;
    }

    @Override
    public String operation() {
        return operation;
    }
}
