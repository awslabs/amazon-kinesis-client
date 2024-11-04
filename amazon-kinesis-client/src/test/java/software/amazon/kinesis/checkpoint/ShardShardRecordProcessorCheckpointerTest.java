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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.PreparedCheckpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ShardShardRecordProcessorCheckpointerTest {
    private String startingSequenceNumber = "13";
    private ExtendedSequenceNumber startingExtendedSequenceNumber = new ExtendedSequenceNumber(startingSequenceNumber);
    private String testConcurrencyToken = "testToken";
    private Checkpointer checkpoint;
    private ShardInfo shardInfo;
    private String shardId = "shardId-123";

    /**
     * @throws Exception
     */
    @Before
    public void setup() throws Exception {
        checkpoint = new InMemoryCheckpointer();
        // A real checkpoint will return a checkpoint value after it is initialized.
        checkpoint.setCheckpoint(shardId, startingExtendedSequenceNumber, testConcurrencyToken);
        assertThat(this.startingExtendedSequenceNumber, equalTo(checkpoint.getCheckpoint(shardId)));

        shardInfo = new ShardInfo(shardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
    }

    /**
     * Test method for
     * {@link ShardRecordProcessorCheckpointer#checkpoint()}.
     */
    @Test
    public final void testCheckpoint() throws Exception {
        // First call to checkpoint
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.largestPermittedCheckpointValue(startingExtendedSequenceNumber);
        processingCheckpointer.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(startingExtendedSequenceNumber));

        // Advance checkpoint
        ExtendedSequenceNumber sequenceNumber = new ExtendedSequenceNumber("5019");

        processingCheckpointer.largestPermittedCheckpointValue(sequenceNumber);
        processingCheckpointer.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(sequenceNumber));
    }

    private Record makeRecord(String seqNum) {
        return Record.builder().sequenceNumber(seqNum).build();
    }

    /**
     * Test method for
     * {@link ShardRecordProcessorCheckpointer#checkpoint(Record record)}.
     */
    @Test
    public final void testCheckpointRecord() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5025");
        Record record = makeRecord("5025");
        processingCheckpointer.largestPermittedCheckpointValue(extendedSequenceNumber);
        processingCheckpointer.checkpoint(record);
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(extendedSequenceNumber));
    }

    /**
     * Test method for
     * {@link ShardRecordProcessorCheckpointer#checkpoint(Record record)}.
     */
    @Test
    public final void testCheckpointSubRecord() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5030");
        Record record = makeRecord("5030");
        // UserRecord subRecord = new UserRecord(record);
        processingCheckpointer.largestPermittedCheckpointValue(extendedSequenceNumber);
        processingCheckpointer.checkpoint(record);
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(extendedSequenceNumber));
    }

    /**
     * Test method for
     * {@link ShardRecordProcessorCheckpointer#checkpoint(String sequenceNumber)}.
     */
    @Test
    public final void testCheckpointSequenceNumber() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5035");
        processingCheckpointer.largestPermittedCheckpointValue(extendedSequenceNumber);
        processingCheckpointer.checkpoint("5035");
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(extendedSequenceNumber));
    }

    /**
     * Test method for
     * {@link ShardRecordProcessorCheckpointer#checkpoint(String sequenceNumber, long subSequenceNumber)}.
     */
    @Test
    public final void testCheckpointExtendedSequenceNumber() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5040");
        processingCheckpointer.largestPermittedCheckpointValue(extendedSequenceNumber);
        processingCheckpointer.checkpoint("5040", 0);
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(extendedSequenceNumber));
    }

    /**
     * Test method for {@link ShardRecordProcessorCheckpointer#checkpoint(String SHARD_END)}.
     */
    @Test
    public final void testCheckpointAtShardEnd() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        ExtendedSequenceNumber extendedSequenceNumber = ExtendedSequenceNumber.SHARD_END;
        processingCheckpointer.largestPermittedCheckpointValue(extendedSequenceNumber);
        processingCheckpointer.checkpoint(ExtendedSequenceNumber.SHARD_END.sequenceNumber());
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(extendedSequenceNumber));
    }

    /**
     * Test method for
     * {@link ShardRecordProcessorCheckpointer#prepareCheckpoint()}.
     */
    @Test
    public final void testPrepareCheckpoint() throws Exception {
        // First call to checkpoint
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);

        ExtendedSequenceNumber sequenceNumber1 = new ExtendedSequenceNumber("5001");
        processingCheckpointer.largestPermittedCheckpointValue(sequenceNumber1);
        PreparedCheckpointer preparedCheckpoint = processingCheckpointer.prepareCheckpoint();
        assertThat(preparedCheckpoint.pendingCheckpoint(), equalTo(sequenceNumber1));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(sequenceNumber1));

        // Advance checkpoint
        ExtendedSequenceNumber sequenceNumber2 = new ExtendedSequenceNumber("5019");

        processingCheckpointer.largestPermittedCheckpointValue(sequenceNumber2);
        preparedCheckpoint = processingCheckpointer.prepareCheckpoint();
        assertThat(preparedCheckpoint.pendingCheckpoint(), equalTo(sequenceNumber2));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(sequenceNumber2));

        // Checkpoint using preparedCheckpoint
        preparedCheckpoint.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(sequenceNumber2));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(sequenceNumber2));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());
    }

    /**
     * Test method for
     * {@link ShardRecordProcessorCheckpointer#prepareCheckpoint(Record record)}.
     */
    @Test
    public final void testPrepareCheckpointRecord() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5025");
        Record record = makeRecord("5025");
        processingCheckpointer.largestPermittedCheckpointValue(extendedSequenceNumber);
        PreparedCheckpointer preparedCheckpoint = processingCheckpointer.prepareCheckpoint(record);
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(startingExtendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(startingExtendedSequenceNumber));
        assertThat(preparedCheckpoint.pendingCheckpoint(), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(extendedSequenceNumber));

        // Checkpoint using preparedCheckpoint
        preparedCheckpoint.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());
    }

    /**
     * Test method for
     * {@link ShardRecordProcessorCheckpointer#prepareCheckpoint(Record record)}.
     */
    @Test
    public final void testPrepareCheckpointSubRecord() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5030");
        Record record = makeRecord("5030");
        // UserRecord subRecord = new UserRecord(record);
        processingCheckpointer.largestPermittedCheckpointValue(extendedSequenceNumber);
        PreparedCheckpointer preparedCheckpoint = processingCheckpointer.prepareCheckpoint(record);
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(startingExtendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(startingExtendedSequenceNumber));
        assertThat(preparedCheckpoint.pendingCheckpoint(), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(extendedSequenceNumber));

        // Checkpoint using preparedCheckpoint
        preparedCheckpoint.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());
    }

    /**
     * Test method for
     * {@link ShardRecordProcessorCheckpointer#checkpoint(String sequenceNumber)}.
     */
    @Test
    public final void testPrepareCheckpointSequenceNumber() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5035");
        processingCheckpointer.largestPermittedCheckpointValue(extendedSequenceNumber);
        PreparedCheckpointer preparedCheckpoint = processingCheckpointer.prepareCheckpoint("5035");
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(startingExtendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(startingExtendedSequenceNumber));
        assertThat(preparedCheckpoint.pendingCheckpoint(), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(extendedSequenceNumber));

        // Checkpoint using preparedCheckpoint
        preparedCheckpoint.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());
    }

    /**
     * Test method for
     * {@link ShardRecordProcessorCheckpointer#checkpoint(String sequenceNumber, long subSequenceNumber)}.
     */
    @Test
    public final void testPrepareCheckpointExtendedSequenceNumber() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5040");
        processingCheckpointer.largestPermittedCheckpointValue(extendedSequenceNumber);
        PreparedCheckpointer preparedCheckpoint = processingCheckpointer.prepareCheckpoint("5040", 0);
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(startingExtendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(startingExtendedSequenceNumber));
        assertThat(preparedCheckpoint.pendingCheckpoint(), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(extendedSequenceNumber));

        // Checkpoint using preparedCheckpoint
        preparedCheckpoint.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());
    }

    /**
     * Test method for {@link ShardRecordProcessorCheckpointer#checkpoint(String SHARD_END)}.
     */
    @Test
    public final void testPrepareCheckpointAtShardEnd() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        ExtendedSequenceNumber extendedSequenceNumber = ExtendedSequenceNumber.SHARD_END;
        processingCheckpointer.largestPermittedCheckpointValue(extendedSequenceNumber);
        PreparedCheckpointer preparedCheckpoint =
                processingCheckpointer.prepareCheckpoint(ExtendedSequenceNumber.SHARD_END.sequenceNumber());
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(startingExtendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(startingExtendedSequenceNumber));
        assertThat(preparedCheckpoint.pendingCheckpoint(), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(extendedSequenceNumber));

        // Checkpoint using preparedCheckpoint
        preparedCheckpoint.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(extendedSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());
    }

    /**
     * Test that having multiple outstanding prepared checkpointers works if they are redeemed in the right order.
     */
    @Test
    public final void testMultipleOutstandingCheckpointersHappyCase() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        processingCheckpointer.largestPermittedCheckpointValue(new ExtendedSequenceNumber("6040"));

        ExtendedSequenceNumber sn1 = new ExtendedSequenceNumber("6010");
        PreparedCheckpointer firstPreparedCheckpoint = processingCheckpointer.prepareCheckpoint("6010", 0);
        assertThat(firstPreparedCheckpoint.pendingCheckpoint(), equalTo(sn1));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(sn1));

        ExtendedSequenceNumber sn2 = new ExtendedSequenceNumber("6020");
        PreparedCheckpointer secondPreparedCheckpoint = processingCheckpointer.prepareCheckpoint("6020", 0);
        assertThat(secondPreparedCheckpoint.pendingCheckpoint(), equalTo(sn2));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(sn2));

        // checkpoint in order
        firstPreparedCheckpoint.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(sn1));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(sn1));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());

        secondPreparedCheckpoint.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(sn2));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(sn2));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());
    }

    /**
     * Test that having multiple outstanding prepared checkpointers works if they are redeemed in the right order.
     */
    @Test
    public final void testMultipleOutstandingCheckpointersOutOfOrder() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
        processingCheckpointer.largestPermittedCheckpointValue(new ExtendedSequenceNumber("7040"));

        ExtendedSequenceNumber sn1 = new ExtendedSequenceNumber("7010");
        PreparedCheckpointer firstPreparedCheckpoint = processingCheckpointer.prepareCheckpoint("7010", 0);
        assertThat(firstPreparedCheckpoint.pendingCheckpoint(), equalTo(sn1));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(sn1));

        ExtendedSequenceNumber sn2 = new ExtendedSequenceNumber("7020");
        PreparedCheckpointer secondPreparedCheckpoint = processingCheckpointer.prepareCheckpoint("7020", 0);
        assertThat(secondPreparedCheckpoint.pendingCheckpoint(), equalTo(sn2));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(sn2));

        // checkpoint out of order
        secondPreparedCheckpoint.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(sn2));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(sn2));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());

        try {
            firstPreparedCheckpoint.checkpoint();
            fail("checkpoint() should have failed because the sequence number was too low");
        } catch (IllegalArgumentException e) {
        } catch (Exception e) {
            fail("checkpoint() should have thrown an IllegalArgumentException but instead threw " + e);
        }
    }

    /**
     * Test method for update()
     *
     */
    @Test
    public final void testUpdate() throws Exception {
        ShardRecordProcessorCheckpointer checkpointer = new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);

        ExtendedSequenceNumber sequenceNumber = new ExtendedSequenceNumber("10");
        checkpointer.largestPermittedCheckpointValue(sequenceNumber);
        assertThat(checkpointer.largestPermittedCheckpointValue(), equalTo(sequenceNumber));

        sequenceNumber = new ExtendedSequenceNumber("90259185948592875928375908214918273491783097");
        checkpointer.largestPermittedCheckpointValue(sequenceNumber);
        assertThat(checkpointer.largestPermittedCheckpointValue(), equalTo(sequenceNumber));
    }

    /**
     * This test is a mixed test of checking some basic functionality of checkpointing at a sequence number and making
     * sure certain bounds checks and validations are being performed inside the checkpointer to prevent clients from
     * checkpointing out of order/too big/non-numeric values that aren't valid strings for them to be checkpointing
     */
    @Test
    public final void testClientSpecifiedCheckpoint() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);

        // Several checkpoints we're gonna hit
        ExtendedSequenceNumber tooSmall = new ExtendedSequenceNumber("2");
        ExtendedSequenceNumber firstSequenceNumber = checkpoint.getCheckpoint(shardId); // 13
        ExtendedSequenceNumber secondSequenceNumber = new ExtendedSequenceNumber("127");
        ExtendedSequenceNumber thirdSequenceNumber = new ExtendedSequenceNumber("5019");
        ExtendedSequenceNumber lastSequenceNumberOfShard = new ExtendedSequenceNumber("6789");
        ExtendedSequenceNumber tooBigSequenceNumber = new ExtendedSequenceNumber("9000");

        processingCheckpointer.setInitialCheckpointValue(firstSequenceNumber);
        processingCheckpointer.largestPermittedCheckpointValue(thirdSequenceNumber);

        // confirm that we cannot move backward
        try {
            processingCheckpointer.checkpoint(tooSmall.sequenceNumber(), tooSmall.subSequenceNumber());
            fail("You shouldn't be able to checkpoint earlier than the initial checkpoint.");
        } catch (IllegalArgumentException e) {
            // yay!
        }

        // advance to first
        processingCheckpointer.checkpoint(
                firstSequenceNumber.sequenceNumber(), firstSequenceNumber.subSequenceNumber());
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(firstSequenceNumber));
        processingCheckpointer.checkpoint(
                firstSequenceNumber.sequenceNumber(), firstSequenceNumber.subSequenceNumber());
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(firstSequenceNumber));

        // advance to second
        processingCheckpointer.checkpoint(
                secondSequenceNumber.sequenceNumber(), secondSequenceNumber.subSequenceNumber());
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(secondSequenceNumber));

        ExtendedSequenceNumber[] valuesWeShouldNotBeAbleToCheckpointAt = {
            tooSmall, // Shouldn't be able to move before the first value we ever checkpointed
            firstSequenceNumber, // Shouldn't even be able to move back to a once used sequence number
            tooBigSequenceNumber, // Can't exceed the max sequence number in the checkpointer
            lastSequenceNumberOfShard, // Just another big value that we will use later
            null, // Not a valid sequence number
            new ExtendedSequenceNumber("bogus-checkpoint-value"), // Can't checkpoint at non-numeric string
            ExtendedSequenceNumber.SHARD_END, // Can't go to the end unless it is set as the max
            ExtendedSequenceNumber.TRIM_HORIZON, // Can't go back to an initial sentinel value
            ExtendedSequenceNumber.LATEST, // Can't go back to an initial sentinel value
        };
        for (ExtendedSequenceNumber badCheckpointValue : valuesWeShouldNotBeAbleToCheckpointAt) {
            try {
                processingCheckpointer.checkpoint(
                        badCheckpointValue.sequenceNumber(), badCheckpointValue.subSequenceNumber());
                fail("checkpointing at bad or out of order sequence didn't throw exception");
            } catch (IllegalArgumentException e) {

            } catch (NullPointerException e) {

            }
            assertThat(
                    "Checkpoint value should not have changed",
                    checkpoint.getCheckpoint(shardId),
                    equalTo(secondSequenceNumber));
            assertThat(
                    "Last checkpoint value should not have changed",
                    processingCheckpointer.lastCheckpointValue(),
                    equalTo(secondSequenceNumber));
            assertThat(
                    "Largest sequence number should not have changed",
                    processingCheckpointer.largestPermittedCheckpointValue(),
                    equalTo(thirdSequenceNumber));
        }

        // advance to third number
        processingCheckpointer.checkpoint(
                thirdSequenceNumber.sequenceNumber(), thirdSequenceNumber.subSequenceNumber());
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(thirdSequenceNumber));

        // Testing a feature that prevents checkpointing at SHARD_END twice
        processingCheckpointer.largestPermittedCheckpointValue(lastSequenceNumberOfShard);
        processingCheckpointer.sequenceNumberAtShardEnd(processingCheckpointer.largestPermittedCheckpointValue());
        processingCheckpointer.largestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
        processingCheckpointer.checkpoint(
                lastSequenceNumberOfShard.sequenceNumber(), lastSequenceNumberOfShard.subSequenceNumber());
        assertThat(
                "Checkpoing at the sequence number at the end of a shard should be the same as checkpointing at SHARD_END",
                processingCheckpointer.lastCheckpointValue(),
                equalTo(ExtendedSequenceNumber.SHARD_END));
    }

    /**
     * This test is a mixed test of checking some basic functionality of two phase checkpointing at a sequence number
     * and making sure certain bounds checks and validations are being performed inside the checkpointer to prevent
     * clients from checkpointing out of order/too big/non-numeric values that aren't valid strings for them to be
     * checkpointing
     */
    @Test
    public final void testClientSpecifiedTwoPhaseCheckpoint() throws Exception {
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);

        // Several checkpoints we're gonna hit
        ExtendedSequenceNumber tooSmall = new ExtendedSequenceNumber("2");
        ExtendedSequenceNumber firstSequenceNumber = checkpoint.getCheckpoint(shardId); // 13
        ExtendedSequenceNumber secondSequenceNumber = new ExtendedSequenceNumber("127");
        ExtendedSequenceNumber thirdSequenceNumber = new ExtendedSequenceNumber("5019");
        ExtendedSequenceNumber lastSequenceNumberOfShard = new ExtendedSequenceNumber("6789");
        ExtendedSequenceNumber tooBigSequenceNumber = new ExtendedSequenceNumber("9000");

        processingCheckpointer.setInitialCheckpointValue(firstSequenceNumber);
        processingCheckpointer.largestPermittedCheckpointValue(thirdSequenceNumber);

        // confirm that we cannot move backward
        try {
            processingCheckpointer.prepareCheckpoint(tooSmall.sequenceNumber(), tooSmall.subSequenceNumber());
            fail("You shouldn't be able to prepare a checkpoint earlier than the initial checkpoint.");
        } catch (IllegalArgumentException e) {
            // yay!
        }

        try {
            processingCheckpointer.checkpoint(tooSmall.sequenceNumber(), tooSmall.subSequenceNumber());
            fail("You shouldn't be able to checkpoint earlier than the initial checkpoint.");
        } catch (IllegalArgumentException e) {
            // yay!
        }

        // advance to first
        processingCheckpointer.checkpoint(
                firstSequenceNumber.sequenceNumber(), firstSequenceNumber.subSequenceNumber());
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(firstSequenceNumber));

        // prepare checkpoint at initial checkpoint value
        PreparedCheckpointer doesNothingPreparedCheckpoint = processingCheckpointer.prepareCheckpoint(
                firstSequenceNumber.sequenceNumber(), firstSequenceNumber.subSequenceNumber());
        assertThat(doesNothingPreparedCheckpoint instanceof DoesNothingPreparedCheckpointer, equalTo(true));
        assertThat(doesNothingPreparedCheckpoint.pendingCheckpoint(), equalTo(firstSequenceNumber));
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(firstSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(firstSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());

        // nothing happens after checkpointing a doesNothingPreparedCheckpoint
        doesNothingPreparedCheckpoint.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(firstSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).checkpoint(), equalTo(firstSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());

        // advance to second
        processingCheckpointer.prepareCheckpoint(
                secondSequenceNumber.sequenceNumber(), secondSequenceNumber.subSequenceNumber());
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(secondSequenceNumber));
        processingCheckpointer.checkpoint(
                secondSequenceNumber.sequenceNumber(), secondSequenceNumber.subSequenceNumber());
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(secondSequenceNumber));
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());

        ExtendedSequenceNumber[] valuesWeShouldNotBeAbleToCheckpointAt = {
            tooSmall, // Shouldn't be able to move before the first value we ever checkpointed
            firstSequenceNumber, // Shouldn't even be able to move back to a once used sequence number
            tooBigSequenceNumber, // Can't exceed the max sequence number in the checkpointer
            lastSequenceNumberOfShard, // Just another big value that we will use later
            null, // Not a valid sequence number
            new ExtendedSequenceNumber("bogus-checkpoint-value"), // Can't checkpoint at non-numeric string
            ExtendedSequenceNumber.SHARD_END, // Can't go to the end unless it is set as the max
            ExtendedSequenceNumber.TRIM_HORIZON, // Can't go back to an initial sentinel value
            ExtendedSequenceNumber.LATEST, // Can't go back to an initial sentinel value
        };
        for (ExtendedSequenceNumber badCheckpointValue : valuesWeShouldNotBeAbleToCheckpointAt) {
            try {
                processingCheckpointer.prepareCheckpoint(
                        badCheckpointValue.sequenceNumber(), badCheckpointValue.subSequenceNumber());
                fail("checkpointing at bad or out of order sequence didn't throw exception");
            } catch (IllegalArgumentException e) {

            } catch (NullPointerException e) {

            }
            assertThat(
                    "Checkpoint value should not have changed",
                    checkpoint.getCheckpoint(shardId),
                    equalTo(secondSequenceNumber));
            assertThat(
                    "Last checkpoint value should not have changed",
                    processingCheckpointer.lastCheckpointValue(),
                    equalTo(secondSequenceNumber));
            assertThat(
                    "Largest sequence number should not have changed",
                    processingCheckpointer.largestPermittedCheckpointValue(),
                    equalTo(thirdSequenceNumber));
            assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());
        }

        // advance to third number
        processingCheckpointer.prepareCheckpoint(
                thirdSequenceNumber.sequenceNumber(), thirdSequenceNumber.subSequenceNumber());
        assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), equalTo(thirdSequenceNumber));
        processingCheckpointer.checkpoint(
                thirdSequenceNumber.sequenceNumber(), thirdSequenceNumber.subSequenceNumber());
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(thirdSequenceNumber));

        // Testing a feature that prevents checkpointing at SHARD_END twice
        processingCheckpointer.largestPermittedCheckpointValue(lastSequenceNumberOfShard);
        processingCheckpointer.sequenceNumberAtShardEnd(processingCheckpointer.largestPermittedCheckpointValue());
        processingCheckpointer.largestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
        processingCheckpointer.prepareCheckpoint(
                lastSequenceNumberOfShard.sequenceNumber(), lastSequenceNumberOfShard.subSequenceNumber());
        assertThat(
                "Preparing a checkpoing at the sequence number at the end of a shard should be the same as preparing a checkpoint at SHARD_END",
                checkpoint.getCheckpointObject(shardId).pendingCheckpoint(),
                equalTo(ExtendedSequenceNumber.SHARD_END));
    }

    private enum CheckpointAction {
        NONE,
        NO_SEQUENCE_NUMBER,
        WITH_SEQUENCE_NUMBER;
    }

    private enum CheckpointerType {
        CHECKPOINTER,
        PREPARED_CHECKPOINTER,
        PREPARE_THEN_CHECKPOINTER;
    }

    /**
     * Tests a bunch of mixed calls between checkpoint() and checkpoint(sequenceNumber) using a helper function.
     *
     * Also covers an edge case scenario where a shard consumer is started on a shard that never receives any records
     * and is then shutdown
     *
     * @throws Exception
     */
    @Test
    public final void testMixedCheckpointCalls() throws Exception {
        for (LinkedHashMap<String, CheckpointAction> testPlan : getMixedCallsTestPlan()) {
            ShardRecordProcessorCheckpointer processingCheckpointer =
                    new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
            testMixedCheckpointCalls(processingCheckpointer, testPlan, CheckpointerType.CHECKPOINTER);
        }
    }

    /**
     * similar to
     * {@link ShardShardRecordProcessorCheckpointerTest#testMixedCheckpointCalls()} ,
     * but executes in two phase commit mode, where we prepare a checkpoint and then commit the prepared checkpoint
     *
     * @throws Exception
     */
    @Test
    public final void testMixedTwoPhaseCheckpointCalls() throws Exception {
        for (LinkedHashMap<String, CheckpointAction> testPlan : getMixedCallsTestPlan()) {
            ShardRecordProcessorCheckpointer processingCheckpointer =
                    new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
            testMixedCheckpointCalls(processingCheckpointer, testPlan, CheckpointerType.PREPARED_CHECKPOINTER);
        }
    }

    /**
     * similar to
     * {@link ShardShardRecordProcessorCheckpointerTest#testMixedCheckpointCalls()} ,
     * but executes in two phase commit mode, where we prepare a checkpoint, but we checkpoint using the
     * RecordProcessorCheckpointer instead of the returned PreparedCheckpointer
     *
     * @throws Exception
     */
    @SuppressWarnings("serial")
    @Test
    public final void testMixedTwoPhaseCheckpointCalls2() throws Exception {
        for (LinkedHashMap<String, CheckpointAction> testPlan : getMixedCallsTestPlan()) {
            ShardRecordProcessorCheckpointer processingCheckpointer =
                    new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
            testMixedCheckpointCalls(processingCheckpointer, testPlan, CheckpointerType.PREPARE_THEN_CHECKPOINTER);
        }
    }

    private List<LinkedHashMap<String, CheckpointAction>> getMixedCallsTestPlan() {
        List<LinkedHashMap<String, CheckpointAction>> testPlans =
                new ArrayList<LinkedHashMap<String, CheckpointAction>>();

        /*
         * Simulate a scenario where the checkpointer is created at "latest".
         *
         * Then the processor is called with no records (e.g. no more records are added, but the processor might be
         * called just to allow checkpointing).
         *
         * Then the processor is shutdown.
         */
        testPlans.add(new LinkedHashMap<String, CheckpointAction>() {
            {
                put(SentinelCheckpoint.LATEST.toString(), CheckpointAction.NO_SEQUENCE_NUMBER);
                put(SentinelCheckpoint.SHARD_END.toString(), CheckpointAction.NO_SEQUENCE_NUMBER);
            }
        });
        // Nearly the same as the previous test, but we don't call checkpoint after LATEST
        testPlans.add(new LinkedHashMap<String, CheckpointAction>() {
            {
                put(SentinelCheckpoint.LATEST.toString(), CheckpointAction.NONE);
                put(SentinelCheckpoint.SHARD_END.toString(), CheckpointAction.NO_SEQUENCE_NUMBER);
            }
        });

        // Start with TRIM_HORIZON
        testPlans.add(new LinkedHashMap<String, CheckpointAction>() {
            {
                put(SentinelCheckpoint.TRIM_HORIZON.toString(), CheckpointAction.NONE);
                put("1", CheckpointAction.NONE);
                put("2", CheckpointAction.NO_SEQUENCE_NUMBER);
                put("3", CheckpointAction.NONE);
                put("4", CheckpointAction.WITH_SEQUENCE_NUMBER);
                put(SentinelCheckpoint.SHARD_END.toString(), CheckpointAction.NO_SEQUENCE_NUMBER);
            }
        });

        // Start with LATEST and a bit more complexity
        testPlans.add(new LinkedHashMap<String, CheckpointAction>() {
            {
                put(SentinelCheckpoint.LATEST.toString(), CheckpointAction.NO_SEQUENCE_NUMBER);
                put("30", CheckpointAction.NONE);
                put("332", CheckpointAction.WITH_SEQUENCE_NUMBER);
                put("349", CheckpointAction.NONE);
                put("4332", CheckpointAction.NO_SEQUENCE_NUMBER);
                put("4338", CheckpointAction.NONE);
                put("5349", CheckpointAction.WITH_SEQUENCE_NUMBER);
                put("5358", CheckpointAction.NONE);
                put("64332", CheckpointAction.NO_SEQUENCE_NUMBER);
                put("64338", CheckpointAction.NO_SEQUENCE_NUMBER);
                put("65358", CheckpointAction.WITH_SEQUENCE_NUMBER);
                put("764338", CheckpointAction.WITH_SEQUENCE_NUMBER);
                put("765349", CheckpointAction.NO_SEQUENCE_NUMBER);
                put("765358", CheckpointAction.NONE);
                put(SentinelCheckpoint.SHARD_END.toString(), CheckpointAction.NO_SEQUENCE_NUMBER);
            }
        });

        return testPlans;
    }

    /**
     * A utility function to simplify various sequences of intermixed updates to the checkpointer, and calls to
     * checpoint() and checkpoint(sequenceNumber). Takes a map where the key is a new sequence number to set in the
     * checkpointer and the value is a CheckpointAction indicating an action to take: NONE -> Set the sequence number,
     * don't do anything else NO_SEQUENCE_NUMBER -> Set the sequence number and call checkpoint() WITH_SEQUENCE_NUMBER
     * -> Set the sequence number and call checkpoint(sequenceNumber) with that sequence number
     *
     * @param processingCheckpointer
     * @param checkpointValueAndAction
     *            A map describing which checkpoint value to set in the checkpointer, and what action to take
     * @throws Exception
     */
    private void testMixedCheckpointCalls(
            ShardRecordProcessorCheckpointer processingCheckpointer,
            LinkedHashMap<String, CheckpointAction> checkpointValueAndAction,
            CheckpointerType checkpointerType)
            throws Exception {

        for (Entry<String, CheckpointAction> entry : checkpointValueAndAction.entrySet()) {
            PreparedCheckpointer preparedCheckpoint = null;
            ExtendedSequenceNumber lastCheckpointValue = processingCheckpointer.lastCheckpointValue();

            if (SentinelCheckpoint.SHARD_END.toString().equals(entry.getKey())) {
                // Before shard end, we will pretend to do what we expect the shutdown task to do
                processingCheckpointer.sequenceNumberAtShardEnd(
                        processingCheckpointer.largestPermittedCheckpointValue());
            }
            // Advance the largest checkpoint and check that it is updated.
            processingCheckpointer.largestPermittedCheckpointValue(new ExtendedSequenceNumber(entry.getKey()));
            assertThat(
                    "Expected the largest checkpoint value to be updated after setting it",
                    processingCheckpointer.largestPermittedCheckpointValue(),
                    equalTo(new ExtendedSequenceNumber(entry.getKey())));
            switch (entry.getValue()) {
                case NONE:
                    // We were told to not checkpoint, so lets just make sure the last checkpoint value is the same as
                    // when this block started then continue to the next instruction
                    assertThat(
                            "Expected the last checkpoint value to stay the same if we didn't checkpoint",
                            processingCheckpointer.lastCheckpointValue(),
                            equalTo(lastCheckpointValue));
                    continue;
                case NO_SEQUENCE_NUMBER:
                    switch (checkpointerType) {
                        case CHECKPOINTER:
                            processingCheckpointer.checkpoint();
                            break;
                        case PREPARED_CHECKPOINTER:
                            preparedCheckpoint = processingCheckpointer.prepareCheckpoint();
                            preparedCheckpoint.checkpoint();
                        case PREPARE_THEN_CHECKPOINTER:
                            preparedCheckpoint = processingCheckpointer.prepareCheckpoint();
                            processingCheckpointer.checkpoint(
                                    preparedCheckpoint.pendingCheckpoint().sequenceNumber(),
                                    preparedCheckpoint.pendingCheckpoint().subSequenceNumber());
                    }
                    break;
                case WITH_SEQUENCE_NUMBER:
                    switch (checkpointerType) {
                        case CHECKPOINTER:
                            processingCheckpointer.checkpoint(entry.getKey());
                            break;
                        case PREPARED_CHECKPOINTER:
                            preparedCheckpoint = processingCheckpointer.prepareCheckpoint(entry.getKey());
                            preparedCheckpoint.checkpoint();
                        case PREPARE_THEN_CHECKPOINTER:
                            preparedCheckpoint = processingCheckpointer.prepareCheckpoint(entry.getKey());
                            processingCheckpointer.checkpoint(
                                    preparedCheckpoint.pendingCheckpoint().sequenceNumber(),
                                    preparedCheckpoint.pendingCheckpoint().subSequenceNumber());
                    }
                    break;
            }
            // We must have checkpointed to get here, so let's make sure our last checkpoint value is up to date
            assertThat(
                    "Expected the last checkpoint value to change after checkpointing",
                    processingCheckpointer.lastCheckpointValue(),
                    equalTo(new ExtendedSequenceNumber(entry.getKey())));
            assertThat(
                    "Expected the largest checkpoint value to remain the same since the last set",
                    processingCheckpointer.largestPermittedCheckpointValue(),
                    equalTo(new ExtendedSequenceNumber(entry.getKey())));

            assertThat(checkpoint.getCheckpoint(shardId), equalTo(new ExtendedSequenceNumber(entry.getKey())));
            assertThat(
                    checkpoint.getCheckpointObject(shardId).checkpoint(),
                    equalTo(new ExtendedSequenceNumber(entry.getKey())));
            assertThat(checkpoint.getCheckpointObject(shardId).pendingCheckpoint(), nullValue());
        }
    }

    @Test
    public final void testUnsetMetricsScopeDuringCheckpointing() throws Exception {
        // First call to checkpoint
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);
        ExtendedSequenceNumber sequenceNumber = new ExtendedSequenceNumber("5019");
        processingCheckpointer.largestPermittedCheckpointValue(sequenceNumber);
        processingCheckpointer.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(sequenceNumber));
    }

    @Test
    public final void testSetMetricsScopeDuringCheckpointing() throws Exception {
        // First call to checkpoint
        ShardRecordProcessorCheckpointer processingCheckpointer =
                new ShardRecordProcessorCheckpointer(shardInfo, checkpoint);

        ExtendedSequenceNumber sequenceNumber = new ExtendedSequenceNumber("5019");
        processingCheckpointer.largestPermittedCheckpointValue(sequenceNumber);
        processingCheckpointer.checkpoint();
        assertThat(checkpoint.getCheckpoint(shardId), equalTo(sequenceNumber));
    }
}
