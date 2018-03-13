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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.KinesisClientLibIOException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ShutdownTaskTest {
    private static final long TASK_BACKOFF_TIME_MILLIS = 1L;
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

    Set<String> defaultParentShardIds = new HashSet<>();
    String defaultConcurrencyToken = "testToken4398";
    String defaultShardId = "shardId-0000397840";
    ShardInfo defaultShardInfo = new ShardInfo(defaultShardId,
            defaultConcurrencyToken,
            defaultParentShardIds,
            ExtendedSequenceNumber.LATEST);
    IRecordProcessor defaultRecordProcessor = new TestStreamlet();
    
    @Mock
    private GetRecordsCache getRecordsCache;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        doNothing().when(getRecordsCache).shutdown();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     */
    @Test
    public final void testCallWhenApplicationDoesNotCheckpoint() {
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("3298"));
        IKinesisProxy kinesisProxy = mock(IKinesisProxy.class);
        ILeaseManager<KinesisClientLease> leaseManager = mock(KinesisClientLeaseManager.class);
        boolean cleanupLeasesOfCompletedShards = false;
        boolean ignoreUnexpectedChildShards = false;
        ShutdownTask task = new ShutdownTask(defaultShardInfo,
                defaultRecordProcessor,
                checkpointer,
                ShutdownReason.TERMINATE,
                kinesisProxy,
                INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards,
                leaseManager,
                TASK_BACKOFF_TIME_MILLIS,
                getRecordsCache);
        TaskResult result = task.call();
        Assert.assertNotNull(result.getException());
        Assert.assertTrue(result.getException() instanceof IllegalArgumentException);
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     */
    @Test
    public final void testCallWhenSyncingShardsThrows() {
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        IKinesisProxy kinesisProxy = mock(IKinesisProxy.class);
        when(kinesisProxy.getShardList()).thenReturn(null);
        ILeaseManager<KinesisClientLease> leaseManager = mock(KinesisClientLeaseManager.class);
        boolean cleanupLeasesOfCompletedShards = false;
        boolean ignoreUnexpectedChildShards = false;
        ShutdownTask task = new ShutdownTask(defaultShardInfo,
                defaultRecordProcessor,
                checkpointer,
                ShutdownReason.TERMINATE,
                kinesisProxy,
                INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards,
                leaseManager,
                TASK_BACKOFF_TIME_MILLIS,
                getRecordsCache);
        TaskResult result = task.call();
        Assert.assertNotNull(result.getException());
        Assert.assertTrue(result.getException() instanceof KinesisClientLibIOException);
        verify(getRecordsCache).shutdown();
    }

    /**
     * Test method for {@link ShutdownTask#getTaskType()}.
     */
    @Test
    public final void testGetTaskType() {
        ShutdownTask task = new ShutdownTask(null, null, null, null, null, null, false, false, null, 0, getRecordsCache);
        Assert.assertEquals(TaskType.SHUTDOWN, task.getTaskType());
    }

}
