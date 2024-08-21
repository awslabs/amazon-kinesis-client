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

import lombok.Data;
import lombok.experimental.Accessors;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * A class encapsulating the 2 pieces of state stored in a checkpoint.
 */
@Data
@Accessors(fluent = true)
public class Checkpoint {
    private final ExtendedSequenceNumber checkpoint;
    private final ExtendedSequenceNumber pendingCheckpoint;
    private final byte[] pendingCheckpointState;

    @Deprecated
    public Checkpoint(final ExtendedSequenceNumber checkpoint, final ExtendedSequenceNumber pendingCheckpoint) {
        this(checkpoint, pendingCheckpoint, null);
    }

    /**
     * Constructor.
     *
     * @param checkpoint the checkpoint sequence number - cannot be null or empty.
     * @param pendingCheckpoint the pending checkpoint sequence number - can be null.
     * @param pendingCheckpointState the pending checkpoint state - can be null.
     */
    public Checkpoint(
            final ExtendedSequenceNumber checkpoint,
            final ExtendedSequenceNumber pendingCheckpoint,
            byte[] pendingCheckpointState) {
        if (checkpoint == null || checkpoint.sequenceNumber().isEmpty()) {
            throw new IllegalArgumentException("Checkpoint cannot be null or empty");
        }
        this.checkpoint = checkpoint;
        this.pendingCheckpoint = pendingCheckpoint;
        this.pendingCheckpointState = pendingCheckpointState;
    }
}
