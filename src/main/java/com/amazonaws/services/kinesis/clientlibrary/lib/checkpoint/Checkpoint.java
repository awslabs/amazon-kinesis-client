package com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

import java.util.Objects;

/**
 * A class encapsulating the 2 pieces of state stored in a checkpoint.
 */
public class Checkpoint {

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

    @Override
    public String toString() {
        return "Checkpoint{" +
                "checkpoint=" + checkpoint +
                ", pendingCheckpoint=" + pendingCheckpoint +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Checkpoint that = (Checkpoint) o;
        return Objects.equals(checkpoint, that.checkpoint) &&
                Objects.equals(pendingCheckpoint, that.pendingCheckpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpoint, pendingCheckpoint);
    }

    public ExtendedSequenceNumber getPendingCheckpoint() {
        return pendingCheckpoint;
    }

    public ExtendedSequenceNumber getCheckpoint() {
        return checkpoint;
    }
}
