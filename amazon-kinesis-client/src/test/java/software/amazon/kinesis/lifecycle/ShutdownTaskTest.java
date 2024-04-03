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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.lifecycle.ShutdownReason.LEASE_LOST;
import static software.amazon.kinesis.lifecycle.ShutdownReason.SHARD_END;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
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
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    private static final StreamIdentifier STREAM_IDENTIFIER = StreamIdentifier.singleStreamInstance("streamName");
    private static final StreamConfig TEST_STREAM_CONFIG =
            new StreamConfig(STREAM_IDENTIFIER, INITIAL_POSITION_TRIM_HORIZON);

    /**
     * Shard id for the default-provided {@link ShardInfo} and {@link Lease}.
     */
    private static final String SHARD_ID = "shardId-0";
    private static final ShardInfo SHARD_INFO = new ShardInfo(SHARD_ID, "concurrencyToken",
            Collections.emptySet(), ExtendedSequenceNumber.LATEST, TEST_STREAM_CONFIG);

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
    @Mock
    private LeaseCleanupManager leaseCleanupManager;
    @Mock
    private ShutdownNotification shutdownNotification;

    @Before
    public void setUp() throws Exception {
        when(recordProcessorCheckpointer.checkpointer()).thenReturn(checkpointer);
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        final Lease childLease = new Lease();
        childLease.leaseKey("childShardLeaseKey");
        when(hierarchicalShardSyncer.createLeaseForChildShard(Matchers.any(ChildShard.class), Matchers.any(StreamIdentifier.class)))
                .thenReturn(childLease);
        setupLease(SHARD_ID, Collections.emptyList());

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(shardDetector.streamIdentifier()).thenReturn(STREAM_IDENTIFIER);

        task = createShutdownTask(SHARD_END, constructChildrenFromSplit());
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that customer doesn't implement checkpoint in their implementation
     */
    @Test
    public final void testCallWhenApplicationDoesNotCheckpoint() {
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("3298"));

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
        when(hierarchicalShardSyncer.createLeaseForChildShard(Matchers.any(ChildShard.class), Matchers.any(StreamIdentifier.class)))
                .thenThrow(new InvalidStateException("InvalidStateException is thrown"));

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never()).shardEnded(any(ShardEndedInput.class));
        verify(shardRecordProcessor).leaseLost(LeaseLostInput.builder().build());
        verify(leaseCoordinator).dropLease(Matchers.any(Lease.class));
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that ShutdownTask is created for ShardConsumer reaching the Shard End.
     */
    @Test
    public final void testCallWhenTrueShardEnd() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final TaskResult result = task.call();
        assertNull(result.getException());
        verifyShutdownAndNoDrop();
        verify(shardRecordProcessor).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(leaseRefresher).updateLeaseWithMetaInfo(Matchers.any(Lease.class), Matchers.any(UpdateField.class));
        verify(leaseRefresher, times(2)).createLeaseIfNotExists(Matchers.any(Lease.class));
        verify(leaseCleanupManager).enqueueForDeletion(any(LeasePendingDeletion.class));
    }

    /**
     * Tests the scenario when one, but not both, parent shards are accessible.
     * This test should drop the lease so another worker can make an attempt.
     */
    @Test
    public void testMergeChildWhereOneParentHasLeaseAndInvalidState() throws Exception {
        testMergeChildWhereOneParentHasLease(false);
    }

    /**
     * Tests the scenario when one, but not both, parent shards are accessible.
     * This test should retain the lease.
     */
    @Test
    public void testMergeChildWhereOneParentHasLeaseAndBlockOnParent() throws Exception {
        testMergeChildWhereOneParentHasLease(true);
    }

    private void testMergeChildWhereOneParentHasLease(final boolean blockOnParent) throws Exception {
        // the @Before setup makes the `SHARD_ID` parent accessible
        final ChildShard mergeChild = constructChildFromMerge();
        final TaskResult result = createShutdownTaskSpy(blockOnParent, Collections.singletonList(mergeChild)).call();

        if (blockOnParent) {
            assertNotNull(result.getException());
            assertEquals(BlockedOnParentShardException.class, result.getException().getClass());

            verify(leaseCoordinator, never()).dropLease(any(Lease.class));
            verify(shardRecordProcessor, never()).leaseLost(any(LeaseLostInput.class));
            verify(recordsPublisher, never()).shutdown();
        } else {
            assertNull(result.getException());

            // verify that only the accessible parent was dropped
            final ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);
            verify(leaseCoordinator).dropLease(leaseCaptor.capture());
            assertEquals(SHARD_ID, leaseCaptor.getValue().leaseKey());

            verify(shardRecordProcessor).leaseLost(any(LeaseLostInput.class));
            verify(recordsPublisher).shutdown();
        }

        // verify that an attempt was made to retrieve both parents
        final ArgumentCaptor<String> leaseKeyCaptor = ArgumentCaptor.forClass(String.class);
        verify(leaseRefresher, times(mergeChild.parentShards().size())).getLease(leaseKeyCaptor.capture());
        assertEquals(mergeChild.parentShards(), leaseKeyCaptor.getAllValues());

        verify(leaseCleanupManager, never()).enqueueForDeletion(any(LeasePendingDeletion.class));
        verify(leaseRefresher, never()).updateLeaseWithMetaInfo(any(Lease.class), any(UpdateField.class));
        verify(leaseRefresher, never()).createLeaseIfNotExists(any(Lease.class));
        verify(shardRecordProcessor, never()).shardEnded(any(ShardEndedInput.class));
    }

    @Test
    public final void testMergeChildWhereBothParentsHaveLeases() throws Exception {
        // the @Before test setup makes the `SHARD_ID` parent accessible
        final ChildShard mergeChild = constructChildFromMerge();
        // make second parent accessible
        setupLease(mergeChild.parentShards().get(1), Collections.emptyList());

        final Lease mockChildLease = mock(Lease.class);
        when(hierarchicalShardSyncer.createLeaseForChildShard(mergeChild, STREAM_IDENTIFIER))
                .thenReturn(mockChildLease);

        final TaskResult result = createShutdownTask(SHARD_END, Collections.singletonList(mergeChild)).call();

        assertNull(result.getException());
        verify(leaseCleanupManager).enqueueForDeletion(any(LeasePendingDeletion.class));

        final ArgumentCaptor<Lease> updateLeaseCaptor = ArgumentCaptor.forClass(Lease.class);
        verify(leaseRefresher).updateLeaseWithMetaInfo(updateLeaseCaptor.capture(), eq(UpdateField.CHILD_SHARDS));
        final Lease updatedLease = updateLeaseCaptor.getValue();
        assertEquals(SHARD_ID, updatedLease.leaseKey());
        assertEquals(Collections.singleton(mergeChild.shardId()), updatedLease.childShardIds());

        verify(leaseRefresher).createLeaseIfNotExists(mockChildLease);

        // verify all parent+child leases were retrieved
        final Set<String> expectedShardIds = new HashSet<>(mergeChild.parentShards());
        expectedShardIds.add(mergeChild.shardId());
        final ArgumentCaptor<String> leaseKeyCaptor = ArgumentCaptor.forClass(String.class);
        verify(leaseRefresher, atLeast(expectedShardIds.size())).getLease(leaseKeyCaptor.capture());
        assertEquals(expectedShardIds, new HashSet<>(leaseKeyCaptor.getAllValues()));

        verifyShutdownAndNoDrop();
        verify(shardRecordProcessor).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that a ShutdownTask is created for detecting a false Shard End.
     */
    @Test
    public final void testCallWhenShardNotFound() throws Exception {
        final Lease lease = setupLease("shardId-4", Collections.emptyList());
        final ShardInfo shardInfo = new ShardInfo(lease.leaseKey(), "concurrencyToken", Collections.emptySet(),
                ExtendedSequenceNumber.LATEST, TEST_STREAM_CONFIG);

        final TaskResult result = createShutdownTask(SHARD_END, Collections.emptyList(), shardInfo).call();

        assertNull(result.getException());
        verifyShutdownAndNoDrop();
        verify(leaseRefresher, never()).createLeaseIfNotExists(any(Lease.class));
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that a ShutdownTask is created for the ShardConsumer losing the lease.
     */
    @Test
    public final void testCallWhenLeaseLost() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final TaskResult result = createShutdownTask(LEASE_LOST, Collections.emptyList()).call();

        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never()).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor).leaseLost(LeaseLostInput.builder().build());
        verify(leaseCoordinator, never()).getAssignments();
        verify(leaseRefresher, never()).createLeaseIfNotExists(any(Lease.class));
        verify(leaseCoordinator, never()).dropLease(any(Lease.class));
    }

    @Test
    public void testNullChildShards() throws Exception {
        final TaskResult result = createShutdownTask(SHARD_END, null).call();

        assertNull(result.getException());
        verifyShutdownAndNoDrop();
        verify(leaseCleanupManager).enqueueForDeletion(any(LeasePendingDeletion.class));
        verify(leaseRefresher, never()).createLeaseIfNotExists(any(Lease.class));
    }

    /**
     * shutdownNotification is only set when ShardConsumer.gracefulShutdown() is called and should be null otherwise.
     * The task should still call recordsPublisher.shutdown() regardless of the notification
     */
    @Test
    public void testCallWhenShutdownNotificationIsSet() {
        final TaskResult result = createShutdownTaskWithNotification(LEASE_LOST, Collections.emptyList()).call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shutdownNotification).shutdownComplete();
    }

    @Test
    public void testCallWhenShutdownNotificationIsNull() {
        final TaskResult result = createShutdownTask(LEASE_LOST, Collections.emptyList()).call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shutdownNotification, never()).shutdownComplete();
    }

    /**
     * Test method for {@link ShutdownTask#taskType()}.
     */
    @Test
    public final void testGetTaskType() {
        assertEquals(TaskType.SHUTDOWN, task.taskType());
    }

    private void verifyShutdownAndNoDrop() {
        verify(recordsPublisher).shutdown();
        verify(leaseCoordinator, never()).dropLease(any(Lease.class));
        verify(shardRecordProcessor, never()).leaseLost(any(LeaseLostInput.class));
    }

    private Lease setupLease(final String leaseKey, final Collection<String> parentShardIds) throws Exception {
        final Lease lease = LeaseHelper.createLease(leaseKey, "leaseOwner", parentShardIds);
        when(leaseCoordinator.getCurrentlyHeldLease(lease.leaseKey())).thenReturn(lease);
        when(leaseRefresher.getLease(lease.leaseKey())).thenReturn(lease);
        return lease;
    }

    /**
     * Constructs two {@link ChildShard}s that mimic a shard split operation.
     */
    private List<ChildShard> constructChildrenFromSplit() {
        List<String> parentShards = Collections.singletonList(SHARD_ID);
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
        return Arrays.asList(leftChild, rightChild);
    }

    /**
     * Constructs a {@link ChildShard} that mimics a shard merge operation.
     */
    private ChildShard constructChildFromMerge() {
        List<String> parentShards = Arrays.asList(SHARD_ID, "shardId-1");
        return ChildShard.builder()
                .shardId("shardId-2")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"))
                .build();
    }

    private ShutdownTask createShutdownTaskSpy(final boolean blockOnParent, final List<ChildShard> childShards) {
        final ShutdownTask spy = spy(createShutdownTask(SHARD_END, childShards));
        when(spy.isOneInNProbability(ShutdownTask.RETRY_RANDOM_MAX_RANGE)).thenReturn(!blockOnParent);
        return spy;
    }

    private ShutdownTask createShutdownTask(final ShutdownReason reason, final List<ChildShard> childShards) {
        return createShutdownTask(reason, childShards, SHARD_INFO);
    }

    private ShutdownTask createShutdownTask(final ShutdownReason reason, final List<ChildShard> childShards,
            final ShardInfo shardInfo) {
        return new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                reason, null, false, false,
                leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher, hierarchicalShardSyncer,
                NULL_METRICS_FACTORY, childShards, leaseCleanupManager);
    }

    private ShutdownTask createShutdownTaskWithNotification(final ShutdownReason reason,
            final List<ChildShard> childShards) {
        return new ShutdownTask(SHARD_INFO, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                reason, shutdownNotification, false, false,
                leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher, hierarchicalShardSyncer,
                NULL_METRICS_FACTORY, childShards, leaseCleanupManager);
    }
}
