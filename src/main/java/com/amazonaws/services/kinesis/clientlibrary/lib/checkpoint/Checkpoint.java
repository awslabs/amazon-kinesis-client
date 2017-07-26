package com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import lombok.Data;

/**
 * A class encapsulating the 2 pieces of state stored in a checkpoint.
 */
@Data public class Checkpoint {

    private final ExtendedSequenceNumber checkpoint;
    private final ExtendedSequenceNumber pendingCheckpoint;

    /**
     * Constructor.
     *
     * @param checkpoint the checkpoint sequence number - cannot be null or empty.
     * @param pendingCheckpoint the pending checkpoint sequence number - can be null.
     */
    public Checkpoint(ExtendedSequenceNumber checkpoint, ExtendedSequenceNumber pendingCheckpoint) {
        if (checkpoint == null || checkpoint.getSequenceNumber().isEmpty()) {
            throw new IllegalArgumentException("Checkpoint cannot be null or empty");
        }
        this.checkpoint = checkpoint;
        this.pendingCheckpoint = pendingCheckpoint;
    }
}
