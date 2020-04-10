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
package software.amazon.kinesis.processor;

import software.amazon.kinesis.exceptions.KinesisClientLibException;
import software.amazon.kinesis.checkpoint.Checkpoint;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Interface for checkpoint trackers.
 */
public interface Checkpointer {

    /**
     * Record a checkpoint for a shard (e.g. sequence and subsequence numbers of last record processed 
     * by application). Upon failover, record processing is resumed from this point.
     * 
     * @param leaseKey Checkpoint is specified for this shard.
     * @param checkpointValue Value of the checkpoint (e.g. Kinesis sequence number and subsequence number)
     * @param concurrencyToken Used with conditional writes to prevent stale updates
     *        (e.g. if there was a fail over to a different record processor, we don't want to 
     *        overwrite it's checkpoint)
     * @throws KinesisClientLibException Thrown if we were unable to save the checkpoint
     */
    void setCheckpoint(String leaseKey, ExtendedSequenceNumber checkpointValue, String concurrencyToken)
        throws KinesisClientLibException;

    /**
     * Get the current checkpoint stored for the specified shard. Useful for checking that the parent shard
     * has been completely processed before we start processing the child shard.
     *
     * @param leaseKey Current checkpoint for this shard is fetched
     * @return Current checkpoint for this shard, null if there is no record for this shard.
     * @throws KinesisClientLibException Thrown if we are unable to fetch the checkpoint
     */
    ExtendedSequenceNumber getCheckpoint(String leaseKey) throws KinesisClientLibException;

    /**
     * Get the current checkpoint stored for the specified shard, which holds the sequence numbers for the checkpoint
     * and pending checkpoint. Useful for checking that the parent shard has been completely processed before we start
     * processing the child shard.
     *
     * @param leaseKey Current checkpoint for this shard is fetched
     * @return Current checkpoint object for this shard, null if there is no record for this shard.
     * @throws KinesisClientLibException Thrown if we are unable to fetch the checkpoint
     */
    Checkpoint getCheckpointObject(String leaseKey) throws KinesisClientLibException;


    /**
     * Record intent to checkpoint for a shard. Upon failover, the pendingCheckpointValue will be passed to the new
     * ShardRecordProcessor's initialize() method.
     *
     * @param leaseKey Checkpoint is specified for this shard.
     * @param pendingCheckpoint Value of the pending checkpoint (e.g. Kinesis sequence number and subsequence number)
     * @param concurrencyToken Used with conditional writes to prevent stale updates
     *        (e.g. if there was a fail over to a different record processor, we don't want to
     *        overwrite it's checkpoint)
     * @throws KinesisClientLibException Thrown if we were unable to save the checkpoint
     */
    void prepareCheckpoint(String leaseKey, ExtendedSequenceNumber pendingCheckpoint, String concurrencyToken)
        throws KinesisClientLibException;

    /**
     * Record intent to checkpoint for a shard. Upon failover, the pendingCheckpoint and pendingCheckpointState will be
     * passed to the new ShardRecordProcessor's initialize() method.
     *
     * @param leaseKey Checkpoint is specified for this shard.
     * @param pendingCheckpoint Value of the pending checkpoint (e.g. Kinesis sequence number and subsequence number)
     * @param concurrencyToken Used with conditional writes to prevent stale updates
     *        (e.g. if there was a fail over to a different record processor, we don't want to
     *        overwrite it's checkpoint)
     * @param pendingCheckpointState Serialized application state at the pending checkpoint.
     *
     * @throws KinesisClientLibException Thrown if we were unable to save the checkpoint
     */
    void prepareCheckpoint(String leaseKey, ExtendedSequenceNumber pendingCheckpoint, String concurrencyToken, byte[] pendingCheckpointState)
            throws KinesisClientLibException;

    void operation(String operation);

    String operation();

}
