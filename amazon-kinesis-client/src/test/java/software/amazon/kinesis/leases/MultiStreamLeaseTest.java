package software.amazon.kinesis.leases;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MultiStreamLeaseTest {

    @Test
    void testCopyingMultiStreamLease() {
        final String checkpointOwner = "checkpointOwner";
        final MultiStreamLease original = new MultiStreamLease();
        original.checkpointOwner(checkpointOwner);
        original.streamIdentifier("identifier");
        original.shardId("shardId");
        final MultiStreamLease copy = original.copy();
        assertEquals(checkpointOwner, copy.checkpointOwner());
    }
}
