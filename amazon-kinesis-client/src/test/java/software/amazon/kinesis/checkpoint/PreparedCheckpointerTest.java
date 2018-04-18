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

import software.amazon.kinesis.processor.IPreparedCheckpointer;
import software.amazon.kinesis.processor.IRecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PreparedCheckpointerTest {

    /**
     * This test verifies the relationship between the constructor and pendingCheckpoint.
     */
    @Test
    public void testGetSequenceNumber() {
        ExtendedSequenceNumber sn = new ExtendedSequenceNumber("sn");
        IPreparedCheckpointer checkpointer = new PreparedCheckpointer(sn, null);
        Assert.assertEquals(sn, checkpointer.pendingCheckpoint());
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
        Assert.assertEquals(sn, checkpointer.pendingCheckpoint());
        // nothing happens here
        checkpointer.checkpoint();
    }
}