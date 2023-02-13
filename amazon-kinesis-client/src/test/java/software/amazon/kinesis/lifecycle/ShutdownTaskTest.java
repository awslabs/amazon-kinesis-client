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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.lifecycle.ShutdownTask.RETRY_RANDOM_MAX_RANGE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.exceptions.internal.BlockedOnParentShardException;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseHelper;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.ShardObjectHelper;
import software.amazon.kinesis.leases.UpdateField;
import software.amazon.kinesis.leases.exceptions.CustomerApplicationException;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.LeasePendingDeletion;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
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

    private final String concurrencyToken = "0-1-2-3-4";
    private final String shardId = "shardId-0";
    private boolean cleanupLeasesOfCompletedShards = false;
    private boolean ignoreUnexpectedChildShards = false;
    private ShardInfo shardInfo;
    private ShutdownTask task;
    private StreamIdentifier streamIdentifier = StreamIdentifier.singleStreamInstance("streamName");
    
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
    @Mock
    private LeaseCleanupManager leaseCleanupManager;

    @Before
    public void setUp() throws Exception {
        doNothing().when(recordsPublisher).shutdown();
        when(recordProcessorCheckpointer.checkpointer()).thenReturn(checkpointer);
        final Lease childLease = new Lease();
        childLease.leaseKey("childShardLeaseKey");
        when(hierarchicalShardSyncer.createLeaseForChildShard(Matchers.any(ChildShard.class), Matchers.any(StreamIdentifier.class)))
                .thenReturn(childLease);

        shardInfo = new ShardInfo(shardId, concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);

        task = new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                hierarchicalShardSyncer, NULL_METRICS_FACTORY, constructChildShards(), streamIdentifier, leaseCleanupManager);
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that customer doesn't implement checkpoint in their implementation
     */
    @Test
    public final void testCallWhenApplicationDoesNotCheckpoint() throws Exception {
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("3298"));
        Lease heldLease = LeaseHelper.createLease("shardId-0", "leaseOwner", Collections.singleton("parentShardId"), Collections.emptyList(), ExtendedSequenceNumber.LATEST);
        when(leaseCoordinator.getCurrentlyHeldLease("shardId-0")).thenReturn(heldLease);
        when(leaseRefresher.getLease("shardId-0")).thenReturn(heldLease);
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(leaseCoordinator.updateLease(Matchers.any(Lease.class), Matchers.any(UUID.class), Matchers.anyString(), Matchers.anyString())).thenReturn(true);

        final TaskResult result = task.call();
        assertNotNull(result.getException());
        assertTrue(result.getException() instanceof CustomerApplicationException);
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that checkAndCreateLeaseForNewShards throws an exception.
     */
    @Test
    public final void testCallWhenCreatingNewLeasesThrows() throws Exception {
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        Lease heldLease = LeaseHelper.createLease("shardId-0", "leaseOwner", Collections.singleton("parentShardId"));
        when(leaseCoordinator.getCurrentlyHeldLease("shardId-0")).thenReturn(heldLease);
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(hierarchicalShardSyncer.createLeaseForChildShard(Matchers.any(ChildShard.class), Matchers.any(StreamIdentifier.class)))
                .thenThrow(new InvalidStateException("InvalidStateException is thrown"));

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never()).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor).leaseLost(LeaseLostInput.builder().build());
        verify(leaseCoordinator).dropLease(Matchers.any(Lease.class));
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that ShutdownTask is created for ShardConsumer reaching the Shard End.
     */
    @Test
    public final void testCallWhenTrueShardEnd() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                                  ExtendedSequenceNumber.LATEST);
        task = new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                                SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                                hierarchicalShardSyncer, NULL_METRICS_FACTORY, constructChildShards(), streamIdentifier, leaseCleanupManager);

        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        Lease heldLease = LeaseHelper.createLease("shardId-0", "leaseOwner", Collections.singleton("parentShardId"));
        when(leaseCoordinator.getCurrentlyHeldLease("shardId-0")).thenReturn(heldLease);
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(leaseCoordinator.updateLease(Matchers.any(Lease.class), Matchers.any(UUID.class), Matchers.anyString(), Matchers.anyString())).thenReturn(true);
        when(leaseRefresher.getLease("shardId-0")).thenReturn(heldLease);

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor, never()).leaseLost(LeaseLostInput.builder().build());
        verify(leaseRefresher).updateLeaseWithMetaInfo(Matchers.any(Lease.class), Matchers.any(UpdateField.class));
        verify(leaseRefresher, times(2)).createLeaseIfNotExists(Matchers.any(Lease.class));
        verify(leaseCoordinator, never()).dropLease(Matchers.any(Lease.class));
        verify(leaseCleanupManager, times(1)).enqueueForDeletion(any(LeasePendingDeletion.class));
    }

    @Test
    public final void testCallThrowsUntilParentInfoNotPresentInLease() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        Lease heldLease = LeaseHelper.createLease("shardId-0", "leaseOwner", ImmutableList.of("parent1", "parent2"));
        Lease parentLease = LeaseHelper.createLease("shardId-1", "leaseOwner", Collections.emptyList());
        when(leaseCoordinator.getCurrentlyHeldLease("shardId-0")).thenReturn(heldLease);
        when(leaseCoordinator.getCurrentlyHeldLease("shardId-1"))
                .thenReturn(null, null, null, null, null, parentLease);
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(leaseCoordinator.updateLease(Matchers.any(Lease.class), Matchers.any(UUID.class), Matchers.anyString(), Matchers.anyString())).thenReturn(true);
        when(leaseRefresher.getLease("shardId-0")).thenReturn(heldLease);
        // Return null lease first time to simulate partial parent lease info
        when(leaseRefresher.getLease("shardId-1"))
                .thenReturn(null, null, null, null, null, parentLease);

        // Make first 5 attempts with partial parent info in lease table
        for (int i = 0; i < 5; i++) {
            ShutdownTask task = spy(new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                    SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                    ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                    hierarchicalShardSyncer, NULL_METRICS_FACTORY, constructChildShard(), streamIdentifier, leaseCleanupManager));
            when(task.isOneInNProbability(RETRY_RANDOM_MAX_RANGE)).thenReturn(false);
            TaskResult result = task.call();
            assertNotNull(result.getException());
            assertTrue(result.getException() instanceof BlockedOnParentShardException);
            assertTrue(result.getException().getMessage().contains("has partial parent information in lease table"));
            verify(recordsPublisher, never()).shutdown();
            verify(shardRecordProcessor, never())
                    .shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
            verify(shardRecordProcessor, never()).leaseLost(LeaseLostInput.builder().build());
            verify(leaseCoordinator, never())
                    .updateLease(Matchers.any(Lease.class), Matchers.any(UUID.class), Matchers.anyString(), Matchers.anyString());
            verify(leaseRefresher, never()).createLeaseIfNotExists(Matchers.any(Lease.class));
            verify(task, times(1)).isOneInNProbability(RETRY_RANDOM_MAX_RANGE);
            verify(leaseCoordinator, never()).dropLease(Matchers.any(Lease.class));
            verify(leaseCleanupManager, never()).enqueueForDeletion(any(LeasePendingDeletion.class));
        }

        // make next attempt with complete parent info in lease table
        ShutdownTask task = spy(new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                hierarchicalShardSyncer, NULL_METRICS_FACTORY, constructChildShard(), streamIdentifier, leaseCleanupManager));
        when(task.isOneInNProbability(RETRY_RANDOM_MAX_RANGE)).thenReturn(false);
        TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor, never()).leaseLost(LeaseLostInput.builder().build());
        verify(leaseRefresher).updateLeaseWithMetaInfo(Matchers.any(Lease.class), Matchers.any(UpdateField.class));
        verify(leaseRefresher, times(1)).createLeaseIfNotExists(Matchers.any(Lease.class));
        verify(task, never()).isOneInNProbability(RETRY_RANDOM_MAX_RANGE);
        verify(leaseCoordinator, never()).dropLease(Matchers.any(Lease.class));
        verify(leaseCleanupManager, times(1)).enqueueForDeletion(any(LeasePendingDeletion.class));
    }

    @Test
    public final void testCallTriggersLeaseLossWhenParentInfoNotPresentInLease() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        shardInfo = new ShardInfo("shardId-0", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);

        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        Lease heldLease = LeaseHelper.createLease("shardId-0", "leaseOwner", ImmutableList.of("parent1", "parent2"));
        when(leaseCoordinator.getCurrentlyHeldLease("shardId-0")).thenReturn(heldLease);
        when(leaseCoordinator.getCurrentlyHeldLease("shardId-1"))
                .thenReturn(null, null, null, null, null, null, null, null, null, null, null);
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(leaseCoordinator.updateLease(Matchers.any(Lease.class), Matchers.any(UUID.class), Matchers.anyString(), Matchers.anyString())).thenReturn(true);
        when(leaseRefresher.getLease("shardId-0")).thenReturn(heldLease);
        // Return null lease first time to simulate partial parent lease info
        when(leaseRefresher.getLease("shardId-1"))
                .thenReturn(null, null, null, null, null, null, null, null, null, null, null);

        // Make first 10 attempts with partial parent info in lease table
        for (int i = 0; i < 10; i++) {
            ShutdownTask task = spy(new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                    SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                    ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                    hierarchicalShardSyncer, NULL_METRICS_FACTORY, constructChildShard(), streamIdentifier, leaseCleanupManager));
            when(task.isOneInNProbability(RETRY_RANDOM_MAX_RANGE)).thenReturn(false);
            TaskResult result = task.call();
            assertNotNull(result.getException());
            assertTrue(result.getException() instanceof BlockedOnParentShardException);
            assertTrue(result.getException().getMessage().contains("has partial parent information in lease table"));
            verify(recordsPublisher, never()).shutdown();
            verify(shardRecordProcessor, never())
                    .shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
            verify(shardRecordProcessor, never()).leaseLost(LeaseLostInput.builder().build());
            verify(leaseCoordinator, never())
                    .updateLease(Matchers.any(Lease.class), Matchers.any(UUID.class), Matchers.anyString(), Matchers.anyString());
            verify(leaseRefresher, never()).createLeaseIfNotExists(Matchers.any(Lease.class));
            verify(task, times(1)).isOneInNProbability(RETRY_RANDOM_MAX_RANGE);
            verify(leaseCoordinator, never()).dropLease(Matchers.any(Lease.class));
            verify(leaseCleanupManager, never()).enqueueForDeletion(any(LeasePendingDeletion.class));
        }

        // make final attempt with incomplete parent info in lease table
        ShutdownTask task = spy(new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                hierarchicalShardSyncer, NULL_METRICS_FACTORY, constructChildShard(), streamIdentifier, leaseCleanupManager));
        when(task.isOneInNProbability(RETRY_RANDOM_MAX_RANGE)).thenReturn(true);
        TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never()).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor).leaseLost(LeaseLostInput.builder().build());
        verify(leaseRefresher, never()).updateLeaseWithMetaInfo(Matchers.any(Lease.class), Matchers.any(UpdateField.class));
        verify(leaseRefresher, never()).createLeaseIfNotExists(Matchers.any(Lease.class));
        verify(task, times(1)).isOneInNProbability(RETRY_RANDOM_MAX_RANGE);
        verify(leaseCoordinator).dropLease(Matchers.any(Lease.class));
        verify(leaseCleanupManager, never()).enqueueForDeletion(any(LeasePendingDeletion.class));
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that a ShutdownTask is created for detecting a false Shard End.
     */
    @Test
    public final void testCallWhenShardNotFound() throws Exception {
        final Lease heldLease = LeaseHelper.createLease("shardId-4", "leaseOwner", Collections.emptyList());
        shardInfo = new ShardInfo("shardId-4", concurrencyToken, Collections.emptySet(),
                                  ExtendedSequenceNumber.LATEST);
        task = new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                                SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                                hierarchicalShardSyncer, NULL_METRICS_FACTORY, new ArrayList<>(), streamIdentifier, leaseCleanupManager);

        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(leaseRefresher.getLease("shardId-4")).thenReturn(heldLease);
        when(leaseCoordinator.getCurrentlyHeldLease("shardId-4")).thenReturn(heldLease);

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never()).leaseLost(LeaseLostInput.builder().build());
        verify(leaseRefresher, never()).createLeaseIfNotExists(Matchers.any(Lease.class));
        verify(leaseCoordinator, never()).dropLease(Matchers.any(Lease.class));
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that a ShutdownTask is created for the ShardConsumer losing the lease.
     */
    @Test
    public final void testCallWhenLeaseLost() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        shardInfo = new ShardInfo("shardId-4", concurrencyToken, Collections.emptySet(),
                ExtendedSequenceNumber.LATEST);
        task = new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                LEASE_LOST_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                hierarchicalShardSyncer, NULL_METRICS_FACTORY, new ArrayList<>(), streamIdentifier, leaseCleanupManager);

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never()).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor).leaseLost(LeaseLostInput.builder().build());
        verify(leaseCoordinator, never()).getAssignments();
        verify(leaseRefresher, never()).createLeaseIfNotExists(any(Lease.class));
        verify(leaseCoordinator, never()).dropLease(any(Lease.class));
    }
    /**
     * Test method for {@link ShutdownTask#taskType()}.
     */
    @Test
    public final void testGetTaskType() {
        assertEquals(TaskType.SHUTDOWN, task.taskType());
    }

    private List<ChildShard> constructChildShards() {
        List<ChildShard> childShards = new ArrayList<>();
        List<String> parentShards = new ArrayList<>();
        parentShards.add(shardId);
        ChildShard leftChild = ChildShard.builder()
                                         .shardId("ShardId-1")
                                         .parentShards(parentShards)
                                         .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"))
                                         .build();
        ChildShard rightChild = ChildShard.builder()
                                         .shardId("ShardId-2")
                                         .parentShards(parentShards)
                                         .hashKeyRange(ShardObjectHelper.newHashKeyRange("50", "99"))
                                         .build();
        childShards.add(leftChild);
        childShards.add(rightChild);
        return  childShards;
    }

    private List<ChildShard> constructChildShard() {
        List<ChildShard> childShards = new ArrayList<>();
        List<String> parentShards = new ArrayList<>();
        parentShards.add(shardId);
        parentShards.add("shardId-1");
        ChildShard leftChild = ChildShard.builder()
                .shardId("shardId-2")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"))
                .build();
        childShards.add(leftChild);
        return  childShards;
    }
}
