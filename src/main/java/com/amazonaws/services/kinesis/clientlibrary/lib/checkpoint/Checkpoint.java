package com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

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

    /**
     * @return checkpoint sequence number
     */
    public ExtendedSequenceNumber getCheckpoint() {
        return checkpoint;
    }

    /**
     * @return pending checkpoint sequence number
     */
    public ExtendedSequenceNumber getPendingCheckpoint() {
        return pendingCheckpoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Checkpoint that = (Checkpoint) o;

        if (checkpoint != null ? !checkpoint.equals(that.checkpoint) : that.checkpoint != null) return false;
        return pendingCheckpoint != null ? pendingCheckpoint.equals(that.pendingCheckpoint) : that.pendingCheckpoint == null;
    }

    @Override
    public int hashCode() {
        int result = checkpoint != null ? checkpoint.hashCode() : 0;
        result = 31 * result + (pendingCheckpoint != null ? pendingCheckpoint.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Checkpoint{"
                + "checkpoint=" + checkpoint
                + ", pendingCheckpoint=" + pendingCheckpoint
                + '}';
    }
}