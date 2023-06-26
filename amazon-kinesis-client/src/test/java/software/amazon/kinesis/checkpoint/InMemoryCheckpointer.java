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
    private Map<String, byte[]> pendingCheckpointStates = new HashMap<>();

    private String operation;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCheckpoint(String leaseKey, ExtendedSequenceNumber checkpointValue, String concurrencyToken) {
        checkpoints.put(leaseKey, checkpointValue);
        flushpoints.put(leaseKey, checkpointValue);
        pendingCheckpoints.remove(leaseKey);
        pendingCheckpointStates.remove(leaseKey);

        if (log.isDebugEnabled()) {
            log.debug("shardId: {} checkpoint: {}", leaseKey, checkpointValue);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExtendedSequenceNumber getCheckpoint(String leaseKey) {
        ExtendedSequenceNumber checkpoint = flushpoints.get(leaseKey);
        log.debug("checkpoint shardId: {} checkpoint: {}", leaseKey, checkpoint);
        return checkpoint;
    }

    @Override
    public void prepareCheckpoint(String leaseKey, ExtendedSequenceNumber pendingCheckpoint, String concurrencyToken) {
        prepareCheckpoint(leaseKey, pendingCheckpoint, concurrencyToken, null);
    }

    @Override
    public void prepareCheckpoint(String leaseKey, ExtendedSequenceNumber pendingCheckpoint, String concurrencyToken,
            byte[] pendingCheckpointState) {
        pendingCheckpoints.put(leaseKey, pendingCheckpoint);
        pendingCheckpointStates.put(leaseKey, pendingCheckpointState);
    }

    @Override
    public Checkpoint getCheckpointObject(String leaseKey) {
        ExtendedSequenceNumber checkpoint = flushpoints.get(leaseKey);
        ExtendedSequenceNumber pendingCheckpoint = pendingCheckpoints.get(leaseKey);
        byte[] pendingCheckpointState = pendingCheckpointStates.get(leaseKey);

        Checkpoint checkpointObj = new Checkpoint(checkpoint, pendingCheckpoint, pendingCheckpointState);
        log.debug("getCheckpointObject shardId: {}, {}", leaseKey, checkpointObj);
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
