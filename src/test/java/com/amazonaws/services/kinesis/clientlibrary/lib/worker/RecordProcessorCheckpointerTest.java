/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.InMemoryCheckpointImpl;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Matchers.anyString;

/**
 *
 */
public class RecordProcessorCheckpointerTest {
    private String startingSequenceNumber = "13";
    private ExtendedSequenceNumber startingExtendedSequenceNumber = new ExtendedSequenceNumber(startingSequenceNumber);
    private String testConcurrencyToken = "testToken";
    private ICheckpoint checkpoint;
    private String shardId = "shardId-123";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        checkpoint = new InMemoryCheckpointImpl(startingSequenceNumber);
        // A real checkpoint will return a checkpoint value after it is initialized.
        checkpoint.setCheckpoint(shardId, startingExtendedSequenceNumber, testConcurrencyToken);
        Assert.assertEquals(this.startingExtendedSequenceNumber, checkpoint.getCheckpoint(shardId));
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordProcessorCheckpointer#checkpoint()}.
     */
    @Test
    public final void testCheckpoint() throws Exception {
        ShardInfo shardInfo = new ShardInfo(shardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);

        // First call to checkpoint
        RecordProcessorCheckpointer processingCheckpointer =
                new RecordProcessorCheckpointer(shardInfo, checkpoint, null);
        processingCheckpointer.setLargestPermittedCheckpointValue(startingExtendedSequenceNumber);
        processingCheckpointer.checkpoint();
        Assert.assertEquals(startingExtendedSequenceNumber, checkpoint.getCheckpoint(shardId));

        // Advance checkpoint
        ExtendedSequenceNumber sequenceNumber = new ExtendedSequenceNumber("5019");

        processingCheckpointer.setLargestPermittedCheckpointValue(sequenceNumber);
        processingCheckpointer.checkpoint();
        Assert.assertEquals(sequenceNumber, checkpoint.getCheckpoint(shardId));
    }
    
    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordProcessorCheckpointer#checkpoint(Record record)}.
     */    
    @Test
    public final void testCheckpointRecord() throws Exception {
        ShardInfo shardInfo = new ShardInfo(shardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        SequenceNumberValidator sequenceNumberValidator = 
                new SequenceNumberValidator(null, shardId, false); 
    	RecordProcessorCheckpointer processingCheckpointer =
                new RecordProcessorCheckpointer(shardInfo, checkpoint, sequenceNumberValidator);
    	processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
    	ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5025");
    	Record record = new Record().withSequenceNumber("5025");
        processingCheckpointer.setLargestPermittedCheckpointValue(extendedSequenceNumber);
        processingCheckpointer.checkpoint(record);
        Assert.assertEquals(extendedSequenceNumber, checkpoint.getCheckpoint(shardId));
    }
    
    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordProcessorCheckpointer#checkpoint(UserRecord record)}.
     */
    @Test
    public final void testCheckpointSubRecord() throws Exception {
        ShardInfo shardInfo = new ShardInfo(shardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        SequenceNumberValidator sequenceNumberValidator = 
                new SequenceNumberValidator(null, shardId, false); 
    	RecordProcessorCheckpointer processingCheckpointer =
                new RecordProcessorCheckpointer(shardInfo, checkpoint, sequenceNumberValidator);
    	processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
    	ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5030");
    	Record record = new Record().withSequenceNumber("5030");
        UserRecord subRecord = new UserRecord(record);
    	processingCheckpointer.setLargestPermittedCheckpointValue(extendedSequenceNumber);
        processingCheckpointer.checkpoint(subRecord);
        Assert.assertEquals(extendedSequenceNumber, checkpoint.getCheckpoint(shardId));
    }
    
    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordProcessorCheckpointer#checkpoint(String sequenceNumber)}.
     */
    @Test
    public final void testCheckpointSequenceNumber() throws Exception {
        ShardInfo shardInfo = new ShardInfo(shardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        SequenceNumberValidator sequenceNumberValidator = 
                new SequenceNumberValidator(null, shardId, false); 
    	RecordProcessorCheckpointer processingCheckpointer =
                new RecordProcessorCheckpointer(shardInfo, checkpoint, sequenceNumberValidator);
    	processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
    	ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5035");
    	processingCheckpointer.setLargestPermittedCheckpointValue(extendedSequenceNumber);
        processingCheckpointer.checkpoint("5035");
        Assert.assertEquals(extendedSequenceNumber, checkpoint.getCheckpoint(shardId));
    }
    
    /**
     * Test method for
     * {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordProcessorCheckpointer#checkpoint(String sequenceNumber, long subSequenceNumber)}.
     */
    @Test
    public final void testCheckpointExtendedSequenceNumber() throws Exception {
        ShardInfo shardInfo = new ShardInfo(shardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        SequenceNumberValidator sequenceNumberValidator = 
                new SequenceNumberValidator(null, shardId, false); 
    	RecordProcessorCheckpointer processingCheckpointer =
                new RecordProcessorCheckpointer(shardInfo, checkpoint, sequenceNumberValidator);
    	processingCheckpointer.setInitialCheckpointValue(startingExtendedSequenceNumber);
    	ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber("5040");
    	processingCheckpointer.setLargestPermittedCheckpointValue(extendedSequenceNumber);
        processingCheckpointer.checkpoint("5040", 0);
        Assert.assertEquals(extendedSequenceNumber, checkpoint.getCheckpoint(shardId));
    }

    /**
     * Test method for update()
     *
     */
    @Test
    public final void testUpdate() throws Exception {
        ShardInfo shardInfo = new ShardInfo(shardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);

        RecordProcessorCheckpointer checkpointer = new RecordProcessorCheckpointer(shardInfo, checkpoint, null);

        ExtendedSequenceNumber sequenceNumber = new ExtendedSequenceNumber("10");
        checkpointer.setLargestPermittedCheckpointValue(sequenceNumber);
        Assert.assertEquals(sequenceNumber, checkpointer.getLargestPermittedCheckpointValue());

        sequenceNumber = new ExtendedSequenceNumber("90259185948592875928375908214918273491783097");
        checkpointer.setLargestPermittedCheckpointValue(sequenceNumber);
        Assert.assertEquals(sequenceNumber, checkpointer.getLargestPermittedCheckpointValue());
    }

    /*
     * This test is a mixed test of checking some basic functionality of checkpointing at a sequence number and making
     * sure certain bounds checks and validations are being performed inside the checkpointer to prevent clients from
     * checkpointing out of order/too big/non-numeric values that aren't valid strings for them to be checkpointing
     */
    @Test
    public final void testClientSpecifiedCheckpoint() throws Exception {
        ShardInfo shardInfo = new ShardInfo(shardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);

        SequenceNumberValidator validator = mock(SequenceNumberValidator.class);
        Mockito.doNothing().when(validator).validateSequenceNumber(anyString());
        RecordProcessorCheckpointer processingCheckpointer =
                new RecordProcessorCheckpointer(shardInfo, checkpoint, validator);

        // Several checkpoints we're gonna hit
        ExtendedSequenceNumber tooSmall = new ExtendedSequenceNumber("2");
        ExtendedSequenceNumber firstSequenceNumber = checkpoint.getCheckpoint(shardId); // 13
        ExtendedSequenceNumber secondSequenceNumber = new ExtendedSequenceNumber("127");
        ExtendedSequenceNumber thirdSequenceNumber = new ExtendedSequenceNumber("5019");
        ExtendedSequenceNumber lastSequenceNumberOfShard = new ExtendedSequenceNumber("6789");
        ExtendedSequenceNumber tooBigSequenceNumber = new ExtendedSequenceNumber("9000");

        processingCheckpointer.setInitialCheckpointValue(firstSequenceNumber);
        processingCheckpointer.setLargestPermittedCheckpointValue(thirdSequenceNumber);

        // confirm that we cannot move backward
        try {
            processingCheckpointer.checkpoint(tooSmall.getSequenceNumber(), tooSmall.getSubSequenceNumber());
            Assert.fail("You shouldn't be able to checkpoint earlier than the initial checkpoint.");
        } catch (IllegalArgumentException e) {
            // yay!
        }

        // advance to first
        processingCheckpointer.checkpoint(firstSequenceNumber.getSequenceNumber(), firstSequenceNumber.getSubSequenceNumber());
        Assert.assertEquals(firstSequenceNumber, checkpoint.getCheckpoint(shardId));
        processingCheckpointer.checkpoint(firstSequenceNumber.getSequenceNumber(), firstSequenceNumber.getSubSequenceNumber());
        Assert.assertEquals(firstSequenceNumber, checkpoint.getCheckpoint(shardId));

        // advance to second
        processingCheckpointer.checkpoint(secondSequenceNumber.getSequenceNumber(), secondSequenceNumber.getSubSequenceNumber());
        Assert.assertEquals(secondSequenceNumber, checkpoint.getCheckpoint(shardId));

        ExtendedSequenceNumber[] valuesWeShouldNotBeAbleToCheckpointAt =
        { tooSmall, // Shouldn't be able to move before the first value we ever checkpointed
                firstSequenceNumber, // Shouldn't even be able to move back to a once used sequence number
                tooBigSequenceNumber, // Can't exceed the max sequence number in the checkpointer
                lastSequenceNumberOfShard, // Just another big value that we will use later
                null, // Not a valid sequence number
                new ExtendedSequenceNumber("bogus-checkpoint-value"), // Can't checkpoint at non-numeric string
                ExtendedSequenceNumber.SHARD_END, // Can't go to the end unless it is set as the max
                ExtendedSequenceNumber.TRIM_HORIZON, // Can't go back to an initial sentinel value
                ExtendedSequenceNumber.LATEST // Can't go back to an initial sentinel value
        };
        for (ExtendedSequenceNumber badCheckpointValue : valuesWeShouldNotBeAbleToCheckpointAt) {
            try {
                processingCheckpointer.checkpoint(badCheckpointValue.getSequenceNumber(), badCheckpointValue.getSubSequenceNumber());
                fail("checkpointing at bad or out of order sequence didn't throw exception");
            } catch (IllegalArgumentException e) {

            } catch (NullPointerException e) {
            
            }
            Assert.assertEquals("Checkpoint value should not have changed",
                    secondSequenceNumber,
                    checkpoint.getCheckpoint(shardId));
            Assert.assertEquals("Last checkpoint value should not have changed",
                    secondSequenceNumber,
                    processingCheckpointer.getLastCheckpointValue());
            Assert.assertEquals("Largest sequence number should not have changed",
                    thirdSequenceNumber,
                    processingCheckpointer.getLargestPermittedCheckpointValue());
        }

        // advance to third number
        processingCheckpointer.checkpoint(thirdSequenceNumber.getSequenceNumber(), thirdSequenceNumber.getSubSequenceNumber());
        Assert.assertEquals(thirdSequenceNumber, checkpoint.getCheckpoint(shardId));

        // Testing a feature that prevents checkpointing at SHARD_END twice
        processingCheckpointer.setLargestPermittedCheckpointValue(lastSequenceNumberOfShard);
        processingCheckpointer.setSequenceNumberAtShardEnd(processingCheckpointer.getLargestPermittedCheckpointValue());
        processingCheckpointer.setLargestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
        processingCheckpointer.checkpoint(lastSequenceNumberOfShard.getSequenceNumber(), lastSequenceNumberOfShard.getSubSequenceNumber());
        Assert.assertEquals("Checkpoing at the sequence number at the end of a shard should be the same as "
                + "checkpointing at SHARD_END",
                ExtendedSequenceNumber.SHARD_END,
                processingCheckpointer.getLastCheckpointValue());
    }

    private enum CheckpointAction {
        NONE, NO_SEQUENCE_NUMBER, WITH_SEQUENCE_NUMBER;
    }

    /**
     * Tests a bunch of mixed calls between checkpoint() and checkpoint(sequenceNumber) using a helper function.
     *
     * Also covers an edge case scenario where a shard consumer is started on a shard that never receives any records
     * and is then shutdown
     *
     * @throws Exception
     */
    @SuppressWarnings("serial")
    @Test
    public final void testMixedCheckpointCalls() throws Exception {
        ShardInfo shardInfo = new ShardInfo(shardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);

        SequenceNumberValidator validator = mock(SequenceNumberValidator.class);
        Mockito.doNothing().when(validator).validateSequenceNumber(anyString());

        RecordProcessorCheckpointer processingCheckpointer =
                new RecordProcessorCheckpointer(shardInfo, checkpoint, validator);

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

        for (LinkedHashMap<String, CheckpointAction> testPlan : testPlans) {
            processingCheckpointer =
                    new RecordProcessorCheckpointer(shardInfo, checkpoint, validator);
            testMixedCheckpointCalls(processingCheckpointer, testPlan);
        }
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
    private void testMixedCheckpointCalls(RecordProcessorCheckpointer processingCheckpointer,
            LinkedHashMap<String, CheckpointAction> checkpointValueAndAction) throws Exception {

        for (Entry<String, CheckpointAction> entry : checkpointValueAndAction.entrySet()) {
            ExtendedSequenceNumber lastCheckpointValue = processingCheckpointer.getLastCheckpointValue();

            if (SentinelCheckpoint.SHARD_END.toString().equals(entry.getKey())) {
                // Before shard end, we will pretend to do what we expect the shutdown task to do
                processingCheckpointer.setSequenceNumberAtShardEnd(processingCheckpointer
                        .getLargestPermittedCheckpointValue());
            }
            // Advance the largest checkpoint and check that it is updated.
            processingCheckpointer.setLargestPermittedCheckpointValue(new ExtendedSequenceNumber(entry.getKey()));
            Assert.assertEquals("Expected the largest checkpoint value to be updated after setting it",
                    new ExtendedSequenceNumber(entry.getKey()),
                    processingCheckpointer.getLargestPermittedCheckpointValue());
            switch (entry.getValue()) {
            case NONE:
                // We were told to not checkpoint, so lets just make sure the last checkpoint value is the same as
                // when this block started then continue to the next instruction
                Assert.assertEquals("Expected the last checkpoint value to stay the same if we didn't checkpoint",
                        lastCheckpointValue,
                        processingCheckpointer.getLastCheckpointValue());
                continue;
            case NO_SEQUENCE_NUMBER:
                processingCheckpointer.checkpoint();
                break;
            case WITH_SEQUENCE_NUMBER:
                processingCheckpointer.checkpoint(entry.getKey());
                break;
            }
            // We must have checkpointed to get here, so let's make sure our last checkpoint value is up to date
            Assert.assertEquals("Expected the last checkpoint value to change after checkpointing",
                    new ExtendedSequenceNumber(entry.getKey()),
                    processingCheckpointer.getLastCheckpointValue());
            Assert.assertEquals("Expected the largest checkpoint value to remain the same since the last set",
            		new ExtendedSequenceNumber(entry.getKey()),
                    processingCheckpointer.getLargestPermittedCheckpointValue());
        }
    }
}
