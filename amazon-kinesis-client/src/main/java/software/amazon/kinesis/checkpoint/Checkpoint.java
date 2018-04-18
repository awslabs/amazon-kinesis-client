/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
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

    /**
     * Constructor.
     *
     * @param checkpoint the checkpoint sequence number - cannot be null or empty.
     * @param pendingCheckpoint the pending checkpoint sequence number - can be null.
     */
    public Checkpoint(final ExtendedSequenceNumber checkpoint, final ExtendedSequenceNumber pendingCheckpoint) {
        if (checkpoint == null || checkpoint.getSequenceNumber().isEmpty()) {
            throw new IllegalArgumentException("Checkpoint cannot be null or empty");
        }
        this.checkpoint = checkpoint;
        this.pendingCheckpoint = pendingCheckpoint;
    }
}
