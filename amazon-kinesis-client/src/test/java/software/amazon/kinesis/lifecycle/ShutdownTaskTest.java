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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.KinesisClientLibIOException;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;

import software.amazon.kinesis.checkpoint.RecordProcessorCheckpointer;
import software.amazon.kinesis.leases.LeaseManager;
import software.amazon.kinesis.leases.KinesisClientLease;
import software.amazon.kinesis.leases.LeaseManagerProxy;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.processor.RecordProcessor;
import software.amazon.kinesis.retrieval.GetRecordsCache;
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
    private static final ShutdownReason TERMINATE_SHUTDOWN_REASON = ShutdownReason.TERMINATE;

    private final String concurrencyToken = "testToken4398";
    private final String shardId = "shardId-0000397840";
    private boolean cleanupLeasesOfCompletedShards = false;
    private boolean ignoreUnexpectedChildShards = false;
    private RecordProcessor recordProcessor;
    private ShardInfo shardInfo;
    private ShutdownTask task;
    
    @Mock
    private GetRecordsCache getRecordsCache;
    @Mock
    private RecordProcessorCheckpointer checkpointer;
    @Mock
    private LeaseManager<KinesisClientLease> leaseManager;
    @Mock
    private LeaseManagerProxy leaseManagerProxy;

    @Before
    public void setUp() throws Exception {
        doNothing().when(getRecordsCache).shutdown();

        shardInfo = new ShardInfo(shardId, concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        recordProcessor = new TestStreamlet();

        task = new ShutdownTask(shardInfo, leaseManagerProxy, recordProcessor, checkpointer,
                TERMINATE_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards, leaseManager, TASK_BACKOFF_TIME_MILLIS, getRecordsCache);
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     */
    @Test
    public final void testCallWhenApplicationDoesNotCheckpoint() {
        when(checkpointer.lastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("3298"));
        final TaskResult result = task.call();
        assertNotNull(result.getException());
        assertTrue(result.getException() instanceof IllegalArgumentException);
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     */
    @Test
    public final void testCallWhenSyncingShardsThrows() {
        when(checkpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        when(leaseManagerProxy.listShards()).thenReturn(null);

        TaskResult result = task.call();
        assertNotNull(result.getException());
        assertTrue(result.getException() instanceof KinesisClientLibIOException);
        verify(getRecordsCache).shutdown();
    }

    /**
     * Test method for {@link ShutdownTask#taskType()}.
     */
    @Test
    public final void testGetTaskType() {
        assertEquals(TaskType.SHUTDOWN, task.taskType());
    }

}
