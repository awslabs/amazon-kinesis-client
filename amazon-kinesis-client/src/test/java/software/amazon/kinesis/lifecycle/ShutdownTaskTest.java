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
package software.amazon.kinesis.lifecycle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.ShardObjectHelper;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ShutdownTaskTest {
    private static final long TASK_BACKOFF_TIME_MILLIS = 1L;
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final ShutdownReason SHARD_END_SHUTDOWN_REASON = ShutdownReason.SHARD_END;
    private static final ShutdownReason LEASE_LOST_SHUTDOWN_REASON  = ShutdownReason.LEASE_LOST;
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    private final String concurrencyToken = "testToken4398";
    private final String shardId = "shardId-0";
    private boolean cleanupLeasesOfCompletedShards = false;
    private boolean ignoreUnexpectedChildShards = false;
    private ShardInfo shardInfo;
    private ShutdownTask task;
    
    @Mock
    private RecordsPublisher recordsPublisher;
    @Mock
    private ShardRecordProcessorCheckpointer recordProcessorCheckpointer;
    @Mock
    private Checkpointer checkpointer;
    @Mock
    private LeaseRefresher leaseRefresher;
    @Mock
    private LeaseCoordinator leaseCoordinator;
    @Mock
    private ShardDetector shardDetector;
    @Mock
    private HierarchicalShardSyncer hierarchicalShardSyncer;
    @Mock
    private ShardRecordProcessor shardRecordProcessor;

    @Before
    public void setUp() throws Exception {
        doNothing().when(recordsPublisher).shutdown();
        when(recordProcessorCheckpointer.checkpointer()).thenReturn(checkpointer);

        shardInfo = new ShardInfo(shardId, concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);

        task = new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                hierarchicalShardSyncer, NULL_METRICS_FACTORY);
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that customer doesn't implement checkpoint in their implementation
     */
    @Test
    public final void testCallWhenApplicationDoesNotCheckpoint() {
        when(shardDetector.listShards()).thenReturn(constructShardListGraphA());
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("3298"));

        final TaskResult result = task.call();
        assertNotNull(result.getException());
        assertTrue(result.getException() instanceof IllegalArgumentException);
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that checkAndCreateLeaseForNewShards throws an exception.
     */
    @Test
    public final void testCallWhenSyncingShardsThrows() throws Exception {
        List<Shard> latestShards = constructShardListGraphA();
        when(shardDetector.listShards()).thenReturn(latestShards);
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);

        doAnswer((invocation) -> {
            throw new KinesisClientLibIOException("KinesisClientLibIOException");
        }).when(hierarchicalShardSyncer)
                .checkAndCreateLeaseForNewShards(shardDetector, leaseRefresher, INITIAL_POSITION_TRIM_HORIZON,
                        cleanupLeasesOfCompletedShards, ignoreUnexpectedChildShards,
                        NULL_METRICS_FACTORY.createMetrics(), latestShards);

        final TaskResult result = task.call();
        assertNotNull(result.getException());
        assertTrue(result.getException() instanceof KinesisClientLibIOException);
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor, never()).leaseLost(LeaseLostInput.builder().build());
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that ShutdownTask is created for ShardConsumer reaching the Shard End.
     */
    @Test
    public final void testCallWhenTrueShardEnd() {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                                  ExtendedSequenceNumber.LATEST);
        task = new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                                SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                                hierarchicalShardSyncer, NULL_METRICS_FACTORY);

        when(shardDetector.listShards()).thenReturn(constructShardListGraphA());
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor, never()).leaseLost(LeaseLostInput.builder().build());
        verify(shardDetector, times(1)).listShards();
        verify(leaseCoordinator, never()).getAssignments();
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that a ShutdownTask is created for detecting a false Shard End.
     */
    @Test
    public final void testCallWhenFalseShardEnd() {
        shardInfo = new ShardInfo("shardId-4", concurrencyToken, Collections.emptySet(),
                                  ExtendedSequenceNumber.LATEST);
        task = new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                                SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                                hierarchicalShardSyncer, NULL_METRICS_FACTORY);

        when(shardDetector.listShards()).thenReturn(constructShardListGraphA());

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never()).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor).leaseLost(LeaseLostInput.builder().build());
        verify(shardDetector, times(1)).listShards();
        verify(leaseCoordinator).getCurrentlyHeldLease(shardInfo.shardId());
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that a ShutdownTask is created for the ShardConsumer losing the lease.
     */
    @Test
    public final void testCallWhenLeaseLost() {
        shardInfo = new ShardInfo("shardId-4", concurrencyToken, Collections.emptySet(),
                                  ExtendedSequenceNumber.LATEST);
        task = new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                                LEASE_LOST_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                                hierarchicalShardSyncer, NULL_METRICS_FACTORY);

        when(shardDetector.listShards()).thenReturn(constructShardListGraphA());

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never()).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor).leaseLost(LeaseLostInput.builder().build());
        verify(shardDetector, never()).listShards();
        verify(leaseCoordinator, never()).getAssignments();
    }

    /**
     * Test method for {@link ShutdownTask#taskType()}.
     */
    @Test
    public final void testGetTaskType() {
        assertEquals(TaskType.SHUTDOWN, task.taskType());
    }


    /*
     * Helper method to construct a shard list for graph A. Graph A is defined below. Shard structure (y-axis is
     * epochs): 0 1 2 3 4   5  - shards till
     *          \ / \ / |   |
     *           6   7  4   5  - shards from epoch 103 - 205
     *            \ /   |  /\
     *             8    4 9 10 - shards from epoch 206 (open - no ending sequenceNumber)
     */
    private List<Shard> constructShardListGraphA() {
        final SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("11", "102");
        final SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("11", null);
        final SequenceNumberRange range2 = ShardObjectHelper.newSequenceNumberRange("11", "205");
        final SequenceNumberRange range3 = ShardObjectHelper.newSequenceNumberRange("103", "205");
        final SequenceNumberRange range4 = ShardObjectHelper.newSequenceNumberRange("206", null);

        return Arrays.asList(
                ShardObjectHelper.newShard("shardId-0", null, null, range0,
                                           ShardObjectHelper.newHashKeyRange("0", "99")),
                ShardObjectHelper.newShard("shardId-1", null, null, range0,
                                           ShardObjectHelper.newHashKeyRange("100", "199")),
                ShardObjectHelper.newShard("shardId-2", null, null, range0,
                                           ShardObjectHelper.newHashKeyRange("200", "299")),
                ShardObjectHelper.newShard("shardId-3", null, null, range0,
                                           ShardObjectHelper.newHashKeyRange("300", "399")),
                ShardObjectHelper.newShard("shardId-4", null, null, range1,
                                           ShardObjectHelper.newHashKeyRange("400", "499")),
                ShardObjectHelper.newShard("shardId-5", null, null, range2,
                                           ShardObjectHelper.newHashKeyRange("500", ShardObjectHelper.MAX_HASH_KEY)),
                ShardObjectHelper.newShard("shardId-6", "shardId-0", "shardId-1", range3,
                                           ShardObjectHelper.newHashKeyRange("0", "199")),
                ShardObjectHelper.newShard("shardId-7", "shardId-2", "shardId-3", range3,
                                           ShardObjectHelper.newHashKeyRange("200", "399")),
                ShardObjectHelper.newShard("shardId-8", "shardId-6", "shardId-7", range4,
                                           ShardObjectHelper.newHashKeyRange("0", "399")),
                ShardObjectHelper.newShard("shardId-9", "shardId-5", null, range4,
                                           ShardObjectHelper.newHashKeyRange("500", "799")),
                ShardObjectHelper.newShard("shardId-10", null, "shardId-5", range4,
                                           ShardObjectHelper.newHashKeyRange("800", ShardObjectHelper.MAX_HASH_KEY)));
    }

}
