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
package com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.metrics.MetricsHelper;
import software.amazon.kinesis.metrics.NullMetricsFactory;

/**
 * Base class for unit testing checkpoint implementations.
 * This class has tests common to InMemory and FileBased implementations.
 */
public abstract class CheckpointImplTestBase {

    protected final String startingSequenceNumber = "0001000";
    protected final String testConcurrencyToken = "testToken";
    protected Checkpointer checkpoint;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        MetricsHelper.startScope(new NullMetricsFactory(), "CheckpointImplTestBase");
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * Constructor.
     */
    public CheckpointImplTestBase() {
        super();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testInitialSetCheckpoint() throws Exception {
    	String sequenceNumber = "1";
        String shardId = "myShardId";
    	ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber(sequenceNumber);
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(sequenceNumber), testConcurrencyToken);
        ExtendedSequenceNumber registeredCheckpoint = checkpoint.getCheckpoint(shardId);
        Assert.assertEquals(extendedSequenceNumber, registeredCheckpoint);
    }
    
    @Test
    public final void testAdvancingSetCheckpoint() throws Exception {
        String shardId = "myShardId";
        for (Integer i = 0; i < 10; i++) {
        	String sequenceNumber = i.toString();
        	ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber(sequenceNumber);
            checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(sequenceNumber), testConcurrencyToken);
            ExtendedSequenceNumber registeredCheckpoint = checkpoint.getCheckpoint(shardId);
            Assert.assertEquals(extendedSequenceNumber, registeredCheckpoint);
        }
    }

    /**
     * Test method to verify setCheckpoint and getCheckpoint methods.
     *
     * @throws Exception
     */
    @Test
    public final void testSetAndGetCheckpoint() throws Exception {
        String checkpointValue = "12345";
        String shardId = "testShardId-1";
        String concurrencyToken = "token-1";
    	ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber(checkpointValue);
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(checkpointValue), concurrencyToken);
        Assert.assertEquals(extendedSequenceNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(extendedSequenceNumber, checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(null, checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
    }

    @Test
    public final void testInitialPrepareCheckpoint() throws Exception {
        String sequenceNumber = "1";
        String pendingCheckpointValue = "99999";
        String shardId = "myShardId";
        ExtendedSequenceNumber extendedCheckpointNumber = new ExtendedSequenceNumber(sequenceNumber);
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(sequenceNumber), testConcurrencyToken);

        ExtendedSequenceNumber extendedPendingCheckpointNumber = new ExtendedSequenceNumber(pendingCheckpointValue);
        checkpoint.prepareCheckpoint(shardId, new ExtendedSequenceNumber(pendingCheckpointValue), testConcurrencyToken);

        Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(extendedPendingCheckpointNumber, checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
    }

    @Test
    public final void testAdvancingPrepareCheckpoint() throws Exception {
        String shardId = "myShardId";
        String checkpointValue = "12345";
        ExtendedSequenceNumber extendedCheckpointNumber = new ExtendedSequenceNumber(checkpointValue);
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(checkpointValue), testConcurrencyToken);

        for (Integer i = 0; i < 10; i++) {
            String sequenceNumber = i.toString();
            ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber(sequenceNumber);
            checkpoint.prepareCheckpoint(shardId, new ExtendedSequenceNumber(sequenceNumber), testConcurrencyToken);
            Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpoint(shardId));
            Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpointObject(shardId).checkpoint());
            Assert.assertEquals(extendedSequenceNumber, checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
        }
    }

    @Test
    public final void testPrepareAndSetCheckpoint() throws Exception {
        String checkpointValue = "12345";
        String shardId = "testShardId-1";
        String concurrencyToken = "token-1";
        String pendingCheckpointValue = "99999";

        // set initial checkpoint
        ExtendedSequenceNumber extendedCheckpointNumber = new ExtendedSequenceNumber(checkpointValue);
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(checkpointValue), concurrencyToken);
        Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(null, checkpoint.getCheckpointObject(shardId).pendingCheckpoint());

        // prepare checkpoint
        ExtendedSequenceNumber extendedPendingCheckpointNumber = new ExtendedSequenceNumber(pendingCheckpointValue);
        checkpoint.prepareCheckpoint(shardId, new ExtendedSequenceNumber(pendingCheckpointValue), concurrencyToken);
        Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(extendedPendingCheckpointNumber, checkpoint.getCheckpointObject(shardId).pendingCheckpoint());

        // do checkpoint
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(pendingCheckpointValue), concurrencyToken);
        Assert.assertEquals(extendedPendingCheckpointNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(extendedPendingCheckpointNumber, checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(null, checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
    }
}
