package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IPreparedCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PreparedCheckpointerTest {

    /**
     * This test verifies the relationship between the constructor and getPendingCheckpoint.
     */
    @Test
    public void testGetSequenceNumber() {
        ExtendedSequenceNumber sn = new ExtendedSequenceNumber("sn");
        IPreparedCheckpointer checkpointer = new PreparedCheckpointer(sn, null);
        Assert.assertEquals(sn, checkpointer.getPendingCheckpoint());
    }

    /**
     * This test makes sure the PreparedCheckpointer calls the IRecordProcessorCheckpointer properly.
     *
     * @throws Exception
     */
    @Test
    public void testCheckpoint() throws Exception {
        ExtendedSequenceNumber sn = new ExtendedSequenceNumber("sn");
        IRecordProcessorCheckpointer mockRecordProcessorCheckpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
        IPreparedCheckpointer checkpointer = new PreparedCheckpointer(sn, mockRecordProcessorCheckpointer);
        checkpointer.checkpoint();
        Mockito.verify(mockRecordProcessorCheckpointer).checkpoint(sn.getSequenceNumber(), sn.getSubSequenceNumber());
    }

    /**
     * This test makes sure the PreparedCheckpointer calls the IRecordProcessorCheckpointer properly.
     *
     * @throws Exception
     */
    @Test
    public void testDoesNothingPreparedCheckpoint() throws Exception {
        ExtendedSequenceNumber sn = new ExtendedSequenceNumber("sn");
        IPreparedCheckpointer checkpointer = new DoesNothingPreparedCheckpointer(sn);
        Assert.assertEquals(sn, checkpointer.getPendingCheckpoint());
        // nothing happens here
        checkpointer.checkpoint();
    }
}