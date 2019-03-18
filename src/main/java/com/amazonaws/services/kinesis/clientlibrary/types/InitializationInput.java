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
package com.amazonaws.services.kinesis.clientlibrary.types;

/**
 * Container for the parameters to the IRecordProcessor's
 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#initialize(InitializationInput
 * initializationInput) initialize} method.
 */
public class InitializationInput {

    private String shardId;
    private ExtendedSequenceNumber extendedSequenceNumber;
    private ExtendedSequenceNumber pendingCheckpointSequenceNumber;

    /**
     * Default constructor.
     */
    public InitializationInput() {
    }

    /**
     * Get shard Id.
     *
     * @return The record processor will be responsible for processing records of this shard.
     */
    public String getShardId() {
        return shardId;
    }

    /**
     * Set shard Id.
     *
     * @param shardId The record processor will be responsible for processing records of this shard.
     * @return A reference to this updated object so that method calls can be chained together.
     */
    public InitializationInput withShardId(String shardId) {
        this.shardId = shardId;
        return this;
    }

    /**
     * Get starting {@link ExtendedSequenceNumber}.
     *
     * @return The {@link ExtendedSequenceNumber} in the shard from which records will be delivered to this
     *         record processor.
     */
    public ExtendedSequenceNumber getExtendedSequenceNumber() {
        return extendedSequenceNumber;
    }

    /**
     * Set starting {@link ExtendedSequenceNumber}.
     *
     * @param extendedSequenceNumber The {@link ExtendedSequenceNumber} in the shard from which records will be
     *        delivered to this record processor.
     * @return A reference to this updated object so that method calls can be chained together.
     */
    public InitializationInput withExtendedSequenceNumber(ExtendedSequenceNumber extendedSequenceNumber) {
        this.extendedSequenceNumber = extendedSequenceNumber;
        return this;
    }

    /**
     * Get pending checkpoint {@link ExtendedSequenceNumber}.
     *
     * @return The {@link ExtendedSequenceNumber} in the shard for which a checkpoint is pending
     */
    public ExtendedSequenceNumber getPendingCheckpointSequenceNumber() {
        return pendingCheckpointSequenceNumber;
    }

    /**
     * Set pending checkpoint {@link ExtendedSequenceNumber}.
     *
     * @param pendingCheckpointSequenceNumber The {@link ExtendedSequenceNumber} in the shard for which a checkpoint
     *                                        is pending
     * @return A reference to this updated object so that method calls can be chained together.
     */
    public InitializationInput withPendingCheckpointSequenceNumber(
            ExtendedSequenceNumber pendingCheckpointSequenceNumber) {
        this.pendingCheckpointSequenceNumber = pendingCheckpointSequenceNumber;
        return this;
    }
}
