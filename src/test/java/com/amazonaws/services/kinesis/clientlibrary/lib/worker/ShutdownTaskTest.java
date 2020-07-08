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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.amazonaws.services.kinesis.clientlibrary.proxies.ShardListWrappingShardClosureVerificationResponse;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.model.ChildShard;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
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
    String defaultConcurrencyToken = UUID.randomUUID().toString();
    String defaultShardId = "shardId-0";
    ShardInfo defaultShardInfo = new ShardInfo(defaultShardId,
            defaultConcurrencyToken,
            defaultParentShardIds,
            ExtendedSequenceNumber.LATEST);
    IRecordProcessor defaultRecordProcessor = new TestStreamlet();
    ShardSyncer shardSyncer = new KinesisShardSyncer(new KinesisLeaseCleanupValidator());


    @Mock
    private IKinesisProxy kinesisProxy;
    @Mock
    private GetRecordsCache getRecordsCache;
    @Mock
    private ShardSyncStrategy shardSyncStrategy;
    @Mock
    private ILeaseManager<KinesisClientLease> leaseManager;
    @Mock
    private KinesisClientLibLeaseCoordinator leaseCoordinator;

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
        final KinesisClientLease parentLease = createLease(defaultShardId, "leaseOwner", Collections.emptyList());
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
        when(leaseCoordinator.getCurrentlyHeldLease(defaultShardId)).thenReturn(parentLease);
        when(leaseCoordinator.updateLease(any(KinesisClientLease.class), any(UUID.class))).thenReturn(true);
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
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
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
                leaseCoordinator,
                TASK_BACKOFF_TIME_MILLIS,
                getRecordsCache,
                shardSyncer,
                shardSyncStrategy,
                constructChildShards());
        TaskResult result = task.call();
        Assert.assertNotNull(result.getException());
        Assert.assertTrue(result.getException() instanceof IllegalArgumentException);
        final String expectedExceptionMessage = "Application didn't checkpoint at end of shard shardId-0. " +
                "Application must checkpoint upon shutdown. See IRecordProcessor.shutdown javadocs for more information.";
        Assert.assertEquals(expectedExceptionMessage, result.getException().getMessage());
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     */
    @Test
    public final void testCallWhenCreatingLeaseThrows() throws Exception {
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
        boolean cleanupLeasesOfCompletedShards = false;
        boolean ignoreUnexpectedChildShards = false;

        final String exceptionMessage = "InvalidStateException is thrown.";
        when(leaseManager.createLeaseIfNotExists(any(KinesisClientLease.class))).thenThrow(new InvalidStateException(exceptionMessage));
        ShutdownTask task = new ShutdownTask(defaultShardInfo,
                defaultRecordProcessor,
                checkpointer,
                ShutdownReason.TERMINATE,
                kinesisProxy,
                INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards,
                leaseCoordinator,
                TASK_BACKOFF_TIME_MILLIS,
                getRecordsCache,
                shardSyncer,
                shardSyncStrategy,
                constructChildShards());
        TaskResult result = task.call();
        verify(getRecordsCache).shutdown();
        verify(leaseCoordinator).dropLease(any(KinesisClientLease.class));
        Assert.assertNull(result.getException());
    }

    @Test
    public final void testCallWhenShardEnd() throws Exception {
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
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
                                             leaseCoordinator,
                                             TASK_BACKOFF_TIME_MILLIS,
                                             getRecordsCache,
                                             shardSyncer,
                                             shardSyncStrategy,
                                             constructChildShards());
        TaskResult result = task.call();
        verify(leaseManager, times(2)).createLeaseIfNotExists(any(KinesisClientLease.class));
        verify(leaseCoordinator).updateLease(any(KinesisClientLease.class), any(UUID.class));
        Assert.assertNull(result.getException());
        verify(getRecordsCache).shutdown();
    }

    @Test
    public final void testCallWhenShardNotFound() throws Exception {
        ShardInfo shardInfo = new ShardInfo("shardId-4",
                                                   defaultConcurrencyToken,
                                                   defaultParentShardIds,
                                                   ExtendedSequenceNumber.LATEST);
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
        boolean cleanupLeasesOfCompletedShards = false;
        boolean ignoreUnexpectedChildShards = false;

        ShutdownTask task = new ShutdownTask(shardInfo,
                                             defaultRecordProcessor,
                                             checkpointer,
                                             ShutdownReason.TERMINATE,
                                             kinesisProxy,
                                             INITIAL_POSITION_TRIM_HORIZON,
                                             cleanupLeasesOfCompletedShards,
                                             ignoreUnexpectedChildShards,
                                             leaseCoordinator,
                                             TASK_BACKOFF_TIME_MILLIS,
                                             getRecordsCache,
                                             shardSyncer,
                                             shardSyncStrategy,
                                             Collections.emptyList());
        TaskResult result = task.call();
        verify(leaseManager, never()).createLeaseIfNotExists(any(KinesisClientLease.class));
        verify(leaseCoordinator, never()).updateLease(any(KinesisClientLease.class), any(UUID.class));
        Assert.assertNull(result.getException());
        verify(getRecordsCache).shutdown();
    }

    @Test
    public final void testCallWhenLeaseLost() throws Exception {
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("3298"));
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
        boolean cleanupLeasesOfCompletedShards = false;
        boolean ignoreUnexpectedChildShards = false;

        ShutdownTask task = new ShutdownTask(defaultShardInfo,
                                             defaultRecordProcessor,
                                             checkpointer,
                                             ShutdownReason.ZOMBIE,
                                             kinesisProxy,
                                             INITIAL_POSITION_TRIM_HORIZON,
                                             cleanupLeasesOfCompletedShards,
                                             ignoreUnexpectedChildShards,
                                             leaseCoordinator,
                                             TASK_BACKOFF_TIME_MILLIS,
                                             getRecordsCache,
                                             shardSyncer,
                                             shardSyncStrategy,
                                             Collections.emptyList());
        TaskResult result = task.call();
        verify(leaseManager, never()).createLeaseIfNotExists(any(KinesisClientLease.class));
        verify(leaseCoordinator, never()).updateLease(any(KinesisClientLease.class), any(UUID.class));
        Assert.assertNull(result.getException());
        verify(getRecordsCache).shutdown();
    }

    /**
     * Test method for {@link ShutdownTask#getTaskType()}.
     */
    @Test
    public final void testGetTaskType() {
        KinesisClientLibLeaseCoordinator leaseCoordinator = mock(KinesisClientLibLeaseCoordinator.class);
        ShutdownTask task = new ShutdownTask(null, null, null, null,
                                             null, null, false,
                                             false, leaseCoordinator, 0,
                                             getRecordsCache, shardSyncer, shardSyncStrategy, Collections.emptyList());
        Assert.assertEquals(TaskType.SHUTDOWN, task.getTaskType());
    }

    private List<ChildShard> constructChildShards() {
        List<ChildShard> childShards = new ArrayList<>();
        List<String> parentShards = new ArrayList<>();
        parentShards.add(defaultShardId);

        ChildShard leftChild = new ChildShard();
        leftChild.setShardId("ShardId-1");
        leftChild.setParentShards(parentShards);
        leftChild.setHashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"));
        childShards.add(leftChild);

        ChildShard rightChild = new ChildShard();
        rightChild.setShardId("ShardId-2");
        rightChild.setParentShards(parentShards);
        rightChild.setHashKeyRange(ShardObjectHelper.newHashKeyRange("50", "99"));
        childShards.add(rightChild);
        return childShards;
    }

    private KinesisClientLease createLease(String leaseKey, String leaseOwner, Collection<String> parentShardIds) {
        KinesisClientLease lease = new KinesisClientLease();
        lease.setLeaseKey(leaseKey);
        lease.setLeaseOwner(leaseOwner);
        lease.setParentShardIds(parentShardIds);
        return lease;
    }

    /*
     * Helper method to construct a shard list for graph A. Graph A is defined below.
     * Shard structure (y-axis is epochs):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |  |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9 10 - shards from epoch 206 (open - no ending sequenceNumber)
     */
    private List<Shard> constructShardListForGraphA() {
        List<Shard> shards = new ArrayList<Shard>();

        SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("11", "102");
        SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("11", null);
        SequenceNumberRange range2 = ShardObjectHelper.newSequenceNumberRange("11", "210");
        SequenceNumberRange range3 = ShardObjectHelper.newSequenceNumberRange("103", "210");
        SequenceNumberRange range4 = ShardObjectHelper.newSequenceNumberRange("211", null);

        HashKeyRange hashRange0 = ShardObjectHelper.newHashKeyRange("0", "99");
        HashKeyRange hashRange1 = ShardObjectHelper.newHashKeyRange("100", "199");
        HashKeyRange hashRange2 = ShardObjectHelper.newHashKeyRange("200", "299");
        HashKeyRange hashRange3 = ShardObjectHelper.newHashKeyRange("300", "399");
        HashKeyRange hashRange4 = ShardObjectHelper.newHashKeyRange("400", "499");
        HashKeyRange hashRange5 = ShardObjectHelper.newHashKeyRange("500", ShardObjectHelper.MAX_HASH_KEY);
        HashKeyRange hashRange6 = ShardObjectHelper.newHashKeyRange("0", "199");
        HashKeyRange hashRange7 = ShardObjectHelper.newHashKeyRange("200", "399");
        HashKeyRange hashRange8 = ShardObjectHelper.newHashKeyRange("0", "399");
        HashKeyRange hashRange9 = ShardObjectHelper.newHashKeyRange("500", "799");
        HashKeyRange hashRange10 = ShardObjectHelper.newHashKeyRange("800", ShardObjectHelper.MAX_HASH_KEY);

        shards.add(ShardObjectHelper.newShard("shardId-0", null, null, range0, hashRange0));
        shards.add(ShardObjectHelper.newShard("shardId-1", null, null, range0, hashRange1));
        shards.add(ShardObjectHelper.newShard("shardId-2", null, null, range0, hashRange2));
        shards.add(ShardObjectHelper.newShard("shardId-3", null, null, range0, hashRange3));
        shards.add(ShardObjectHelper.newShard("shardId-4", null, null, range1, hashRange4));
        shards.add(ShardObjectHelper.newShard("shardId-5", null, null, range2, hashRange5));

        shards.add(ShardObjectHelper.newShard("shardId-6", "shardId-0", "shardId-1", range3, hashRange6));
        shards.add(ShardObjectHelper.newShard("shardId-7", "shardId-2", "shardId-3", range3, hashRange7));

        shards.add(ShardObjectHelper.newShard("shardId-8", "shardId-6", "shardId-7", range4, hashRange8));
        shards.add(ShardObjectHelper.newShard("shardId-9", "shardId-5", null, range4, hashRange9));
        shards.add(ShardObjectHelper.newShard("shardId-10", null, "shardId-5", range4, hashRange10));

        return shards;
    }

}