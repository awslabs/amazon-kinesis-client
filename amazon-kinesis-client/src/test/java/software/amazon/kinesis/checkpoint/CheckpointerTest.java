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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Base class for unit testing checkpoint implementations.
 * This class has tests common to InMemory and FileBased implementations.
 */
public class CheckpointerTest {

    private final String testConcurrencyToken = "testToken";
    private Checkpointer checkpoint;

    @Before
    public void setup() {
        checkpoint = new InMemoryCheckpointer();
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
     * Test method to verify checkpoint and checkpoint methods.
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
        Assert.assertEquals(
                extendedSequenceNumber, checkpoint.getCheckpointObject(shardId).checkpoint());
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
        Assert.assertEquals(
                extendedCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(
                extendedPendingCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
    }

    @Test
    public final void testInitialPrepareCheckpointWithApplicationState() throws Exception {
        String sequenceNumber = "1";
        String pendingCheckpointValue = "99999";
        String shardId = "myShardId";
        byte[] applicationState = "applicationState".getBytes();
        ExtendedSequenceNumber extendedCheckpointNumber = new ExtendedSequenceNumber(sequenceNumber);
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(sequenceNumber), testConcurrencyToken);

        ExtendedSequenceNumber extendedPendingCheckpointNumber = new ExtendedSequenceNumber(pendingCheckpointValue);
        checkpoint.prepareCheckpoint(
                shardId, new ExtendedSequenceNumber(pendingCheckpointValue), testConcurrencyToken, applicationState);

        Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(
                extendedCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(
                extendedPendingCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
        Assert.assertEquals(
                applicationState, checkpoint.getCheckpointObject(shardId).pendingCheckpointState());
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
            Assert.assertEquals(
                    extendedCheckpointNumber,
                    checkpoint.getCheckpointObject(shardId).checkpoint());
            Assert.assertEquals(
                    extendedSequenceNumber,
                    checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
        }
    }

    @Test
    public final void testAdvancingPrepareCheckpointWithApplicationState() throws Exception {
        String shardId = "myShardId";
        String checkpointValue = "12345";
        byte[] applicationState = "applicationState".getBytes();
        ExtendedSequenceNumber extendedCheckpointNumber = new ExtendedSequenceNumber(checkpointValue);
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(checkpointValue), testConcurrencyToken);

        for (Integer i = 0; i < 10; i++) {
            String sequenceNumber = i.toString();
            ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber(sequenceNumber);
            checkpoint.prepareCheckpoint(
                    shardId, new ExtendedSequenceNumber(sequenceNumber), testConcurrencyToken, applicationState);
            Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpoint(shardId));
            Assert.assertEquals(
                    extendedCheckpointNumber,
                    checkpoint.getCheckpointObject(shardId).checkpoint());
            Assert.assertEquals(
                    extendedSequenceNumber,
                    checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
            Assert.assertEquals(
                    applicationState, checkpoint.getCheckpointObject(shardId).pendingCheckpointState());
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
        Assert.assertEquals(
                extendedCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(null, checkpoint.getCheckpointObject(shardId).pendingCheckpoint());

        // prepare checkpoint
        ExtendedSequenceNumber extendedPendingCheckpointNumber = new ExtendedSequenceNumber(pendingCheckpointValue);
        checkpoint.prepareCheckpoint(shardId, new ExtendedSequenceNumber(pendingCheckpointValue), concurrencyToken);
        Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(
                extendedCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(
                extendedPendingCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).pendingCheckpoint());

        // do checkpoint
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(pendingCheckpointValue), concurrencyToken);
        Assert.assertEquals(extendedPendingCheckpointNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(
                extendedPendingCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(null, checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
    }

    @Test
    public final void testPrepareAndSetCheckpointWithApplicationState() throws Exception {
        String checkpointValue = "12345";
        String shardId = "testShardId-1";
        String concurrencyToken = "token-1";
        String pendingCheckpointValue = "99999";
        byte[] applicationState = "applicationState".getBytes();

        // set initial checkpoint
        ExtendedSequenceNumber extendedCheckpointNumber = new ExtendedSequenceNumber(checkpointValue);
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(checkpointValue), concurrencyToken);
        Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(
                extendedCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(null, checkpoint.getCheckpointObject(shardId).pendingCheckpoint());

        // prepare checkpoint
        ExtendedSequenceNumber extendedPendingCheckpointNumber = new ExtendedSequenceNumber(pendingCheckpointValue);
        checkpoint.prepareCheckpoint(
                shardId, new ExtendedSequenceNumber(pendingCheckpointValue), concurrencyToken, applicationState);
        Assert.assertEquals(extendedCheckpointNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(
                extendedCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(
                extendedPendingCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
        Assert.assertEquals(
                applicationState, checkpoint.getCheckpointObject(shardId).pendingCheckpointState());

        // do checkpoint
        checkpoint.setCheckpoint(shardId, new ExtendedSequenceNumber(pendingCheckpointValue), concurrencyToken);
        Assert.assertEquals(extendedPendingCheckpointNumber, checkpoint.getCheckpoint(shardId));
        Assert.assertEquals(
                extendedPendingCheckpointNumber,
                checkpoint.getCheckpointObject(shardId).checkpoint());
        Assert.assertEquals(null, checkpoint.getCheckpointObject(shardId).pendingCheckpoint());
        Assert.assertEquals(null, checkpoint.getCheckpointObject(shardId).pendingCheckpointState());
    }
}
