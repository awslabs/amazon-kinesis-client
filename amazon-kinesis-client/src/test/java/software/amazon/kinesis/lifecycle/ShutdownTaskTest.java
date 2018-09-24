/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.lifecycle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.utils.TestStreamlet;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ShutdownTaskTest {
    private static final long TASK_BACKOFF_TIME_MILLIS = 1L;
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final ShutdownReason TERMINATE_SHUTDOWN_REASON = ShutdownReason.SHARD_END;
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    private final String concurrencyToken = "testToken4398";
    private final String shardId = "shardId-0000397840";
    private boolean cleanupLeasesOfCompletedShards = false;
    private boolean ignoreUnexpectedChildShards = false;
    private ShardRecordProcessor shardRecordProcessor;
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
    private ShardDetector shardDetector;
    @Mock
    private HierarchicalShardSyncer hierarchicalShardSyncer;

    @Before
    public void setUp() throws Exception {
        doNothing().when(recordsPublisher).shutdown();
        when(recordProcessorCheckpointer.checkpointer()).thenReturn(checkpointer);

        shardInfo = new ShardInfo(shardId, concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        shardRecordProcessor = new TestStreamlet();

        task = new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                TERMINATE_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards, leaseRefresher, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                hierarchicalShardSyncer, NULL_METRICS_FACTORY);
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     */
    @Test
    public final void testCallWhenApplicationDoesNotCheckpoint() {
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("3298"));
        final TaskResult result = task.call();
        assertNotNull(result.getException());
        assertTrue(result.getException() instanceof IllegalArgumentException);
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     */
    @Test
    public final void testCallWhenSyncingShardsThrows() throws Exception {
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        when(shardDetector.listShards()).thenReturn(null);
        doAnswer((invocation) -> {
            throw new KinesisClientLibIOException("KinesisClientLibIOException");
        }).when(hierarchicalShardSyncer)
                .checkAndCreateLeaseForNewShards(shardDetector, leaseRefresher, INITIAL_POSITION_TRIM_HORIZON,
                        cleanupLeasesOfCompletedShards, ignoreUnexpectedChildShards,
                        NULL_METRICS_FACTORY.createMetrics());

        TaskResult result = task.call();
        assertNotNull(result.getException());
        assertTrue(result.getException() instanceof KinesisClientLibIOException);
        verify(recordsPublisher).shutdown();
    }

    /**
     * Test method for {@link ShutdownTask#taskType()}.
     */
    @Test
    public final void testGetTaskType() {
        assertEquals(TaskType.SHUTDOWN, task.taskType());
    }

}
