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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.LeasePendingDeletion;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.lifecycle.ShutdownReason.LEASE_LOST;
import static software.amazon.kinesis.lifecycle.ShutdownReason.SHARD_END;

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

    /**
     * Shard id for the default-provided {@link ShardInfo} and {@link Lease}.
     */
    private static final String SHARD_ID = "shardId-0";

    private static final String LEASE_OWNER = "leaseOwner";
    private static final ShardInfo SHARD_INFO =
            new ShardInfo(SHARD_ID, "concurrencyToken", Collections.emptySet(), ExtendedSequenceNumber.LATEST);

    private ShutdownTask task;
    private Lease lease;

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
        when(hierarchicalShardSyncer.createLeaseForChildShard(
                        Matchers.any(ChildShard.class), Matchers.any(StreamIdentifier.class)))
                .thenReturn(childLease);
        lease = setupLease(SHARD_ID, Collections.emptyList());

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
        when(hierarchicalShardSyncer.createLeaseForChildShard(
                        Matchers.any(ChildShard.class), Matchers.any(StreamIdentifier.class)))
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
    public final void testCallWhenTrueShardEnd()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final TaskResult result = task.call();
        assertNull(result.getException());
        verifyShutdownAndNoDrop();
        verify(shardRecordProcessor)
                .shardEnded(ShardEndedInput.builder()
                        .checkpointer(recordProcessorCheckpointer)
                        .build());
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
        final TaskResult result = createShutdownTaskSpy(blockOnParent, Collections.singletonList(mergeChild))
                .call();

        if (blockOnParent) {
            assertNotNull(result.getException());
            assertEquals(
                    BlockedOnParentShardException.class, result.getException().getClass());

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

        final TaskResult result = createShutdownTask(SHARD_END, Collections.singletonList(mergeChild))
                .call();

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
        verify(shardRecordProcessor)
                .shardEnded(ShardEndedInput.builder()
                        .checkpointer(recordProcessorCheckpointer)
                        .build());
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that a ShutdownTask is created for detecting a false Shard End.
     */
    @Test
    public final void testCallWhenShardNotFound() throws Exception {
        final Lease lease = setupLease("shardId-4", Collections.emptyList());
        final ShardInfo shardInfo = new ShardInfo(
                lease.leaseKey(), "concurrencyToken", Collections.emptySet(), ExtendedSequenceNumber.LATEST);

        final TaskResult result = createShutdownTask(SHARD_END, Collections.emptyList(), shardInfo)
                .call();

        assertNull(result.getException());
        verifyShutdownAndNoDrop();
        verify(leaseRefresher, never()).createLeaseIfNotExists(any(Lease.class));
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that a ShutdownTask is created for the ShardConsumer losing the lease.
     */
    @Test
    public final void testCallWhenLeaseLost()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final TaskResult result =
                createShutdownTask(LEASE_LOST, Collections.emptyList()).call();

        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never())
                .shardEnded(ShardEndedInput.builder()
                        .checkpointer(recordProcessorCheckpointer)
                        .build());
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

    @Test
    public void testCallWhenShutdownNotificationIsNull() {
        final TaskResult result =
                createShutdownTask(LEASE_LOST, Collections.emptyList()).call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shutdownNotification, never()).shutdownComplete();
    }

    @Test
    public void testLeaseLostWithShutdownRequestedTransfersAndDropsLease() throws Exception {
        lease.checkpointOwner(LEASE_OWNER);
        when(leaseCoordinator.workerIdentifier()).thenReturn(LEASE_OWNER);

        final TaskResult result =
                createShutdownTask(LEASE_LOST, Collections.emptyList()).call();

        assertNull(result.getException());
        verify(leaseRefresher).assignLease(lease, lease.leaseOwner());
        verify(leaseCoordinator).dropLease(lease);
        verify(recordsPublisher).shutdown();
    }

    @Test
    public void testLeaseLostWithShutdownRequestedDropsLeaseEvenOnTransferFailure() throws Exception {
        lease.checkpointOwner(LEASE_OWNER);
        when(leaseCoordinator.workerIdentifier()).thenReturn(LEASE_OWNER);
        when(leaseRefresher.assignLease(any(Lease.class), any(String.class)))
                .thenThrow(new DependencyException(new RuntimeException()));

        final TaskResult result =
                createShutdownTask(LEASE_LOST, Collections.emptyList()).call();

        assertNull(result.getException());
        verify(leaseCoordinator).dropLease(lease);
        verify(recordsPublisher).shutdown();
    }

    @Test
    public void testLeaseLostWithoutShutdownRequestedDoesNotTransferOrDrop() throws Exception {
        // lease exists but shutdownRequested is false (default)
        final TaskResult result =
                createShutdownTask(LEASE_LOST, Collections.emptyList()).call();

        assertNull(result.getException());
        verify(leaseRefresher, never()).assignLease(any(Lease.class), any(String.class));
        verify(leaseCoordinator, never()).dropLease(any(Lease.class));
        verify(recordsPublisher).shutdown();
    }

    /**
     * The leader evicts the lease while the application's shardEnded() is still running, after its SHARD_END
     * checkpoint (the earliest the leader can evict: it only evicts leases that are at SHARD_END in the lease table).
     * The task already read the held lease when it started, so it finishes the normal SHARD_END path: the lease is
     * queued for this worker's cleanup, and it's not dropped and the application doesn't get leaseLost().
     */
    @Test
    public final void testShardEndWhenLeaseEvictedDuringShardEnded_completesWithoutLeaseLost() throws Exception {
        final AtomicBoolean evicted = new AtomicBoolean(false);
        when(leaseCoordinator.getCurrentlyHeldLease(lease.leaseKey())).thenAnswer(i -> evicted.get() ? null : lease);
        doAnswer(invocation -> {
                    lease.checkpoint(ExtendedSequenceNumber.SHARD_END); // the application's checkpoint
                    evicted.set(true); // the leader evicts the lease
                    return null;
                })
                .when(shardRecordProcessor)
                .shardEnded(any(ShardEndedInput.class));

        final TaskResult result = task.call();

        assertNull(result.getException());
        verify(shardRecordProcessor, times(1)).shardEnded(any(ShardEndedInput.class));
        verifyShutdownAndNoDrop();
        verify(leaseCleanupManager).enqueueForDeletion(any(LeasePendingDeletion.class));
        // the task reads the held lease once, when it starts, so the eviction during shardEnded() doesn't reach it
        verify(leaseCoordinator, times(1)).getCurrentlyHeldLease(lease.leaseKey());
    }

    /**
     * The baseline for the two tests below. The application's shardEnded() checkpoints at SHARD_END and then throws,
     * so the task fails and is retried, and the lease is still held at the retry, as it always was before leader-side
     * SHARD_END cleanup was introduced. The retry sees the lease already at SHARD_END in the lease table, so it
     * doesn't call shardEnded() again; it queues the lease for cleanup, and the application gets no further calls.
     */
    @Test
    public final void testShardEndRetryAfterShardEndedThrowsAndLeaseStillHeld_noFurtherApplicationCalls()
            throws Exception {
        doAnswer(invocation -> {
                    lease.checkpoint(ExtendedSequenceNumber.SHARD_END); // the application's checkpoint
                    throw new RuntimeException("work after the checkpoint failed");
                })
                .when(shardRecordProcessor)
                .shardEnded(any(ShardEndedInput.class));

        final TaskResult firstAttempt = task.call();
        assertTrue(firstAttempt.getException() instanceof CustomerApplicationException);
        // Not queued yet: the finally block in takeShardEndAction only queues the lease if the attempt succeeded
        // (or the shard has no child shards, which isn't the case here).
        verify(leaseCleanupManager, never()).enqueueForDeletion(any(LeasePendingDeletion.class));

        final TaskResult retry =
                createShutdownTask(SHARD_END, constructChildrenFromSplit()).call();

        assertNull(retry.getException());
        verify(shardRecordProcessor, times(1)).shardEnded(any(ShardEndedInput.class));
        verifyShutdownAndNoDrop();
        // Queued by the retry, which succeeds.
        verify(leaseCleanupManager, times(1)).enqueueForDeletion(any(LeasePendingDeletion.class));
    }

    /**
     * The application's shardEnded() checkpoints at SHARD_END and then throws, so the task fails and is retried.
     * Before the retry, the leader evicts the lease (it's at SHARD_END in the lease table), so this worker no longer
     * holds it. The retry finishes without calling the application again: it gets no second shardEnded() and no
     * leaseLost(), the same as when the lease is still held (see the baseline test above).
     */
    @Test
    public final void testShardEndRetryAfterShardEndedThrowsAndLeaseEvicted_noFurtherApplicationCalls()
            throws Exception {
        failFirstShardEndedAfterCheckpointThenEvict();

        final TaskResult retry =
                createShutdownTask(SHARD_END, constructChildrenFromSplit()).call();

        assertNull(retry.getException());
        verifyRetryMadeNoApplicationCalls();
    }

    /**
     * Same as above, but the Scheduler noticed the lost lease before the retry was created, so the retry is a
     * LEASE_LOST shutdown task (the ShardConsumer creates it with its current shutdown reason; see
     * ShardConsumerTest#testFailedShardEndShutdown_schedulerSawLeaseLossBeforeRetry_retryIsLeaseLostShutdownTask).
     * It also finishes without calling the application again.
     */
    @Test
    public final void testLeaseLostRetryAfterShardEndedThrowsAndLeaseEvicted_noFurtherApplicationCalls()
            throws Exception {
        failFirstShardEndedAfterCheckpointThenEvict();

        final TaskResult retry =
                createShutdownTask(LEASE_LOST, constructChildrenFromSplit()).call();

        assertNull(retry.getException());
        verifyRetryMadeNoApplicationCalls();
    }

    /**
     * shardEnded() was called but threw before checkpointing, so the shard isn't done. The lease isn't at SHARD_END,
     * so the leader doesn't evict it, but it can still be lost for real (for example, it expired and another worker
     * took it). The retry then calls the record processor's leaseLost(), as it did before leader-side SHARD_END
     * cleanup was introduced. This test and the next cover both retry reasons: SHARD_END here (row 5 of the table in
     * ShardConsumerTest: the Scheduler hasn't noticed the lost lease yet), LEASE_LOST in the next (row 6).
     *
     * <p>The check for an already-completed shard end at the top of call() doesn't apply, because lastCheckpointValue
     * isn't SHARD_END: the check requires this processor's own SHARD_END checkpoint, not just a shardEnded() call. It's
     * based on this processor's checkpointer rather than the lease table, because the table can show the shard at
     * SHARD_END (or deleted) after another worker finished it, while this processor still needs to be told it lost
     * the lease.
     *
     * <p>How a SHARD_END task gets to leaseLost(): takeShardEndAction() throws InvalidStateException ("Lease not
     * owned by the current worker") because the lease isn't held, before doing any shard-end work. The SHARD_END
     * branch catches it, logs a warning with the exception, and finishes as a lease loss: dropLease() only logs
     * (there's no held lease), and then it calls the record processor's leaseLost(). The task's reason stays
     * SHARD_END; this is the SHARD_END path's existing fallback when it can't do the shard-end work. The exception
     * doesn't leave call(), so this test checks that none of the shard-end work ran: the SHARD_END path's other ways
     * to reach leaseLost() (an InvalidStateException while creating child leases, or a missing lease when
     * checkpointing) come after it reads the lease or writes child leases.
     */
    @Test
    public final void testLeaseLostAfterShardEndedThrewBeforeCheckpoint_shardEndRetry_callsLeaseLostViaNotOwnedPath()
            throws Exception {
        leaseLostAfterShardEndedThrewBeforeCheckpoint();

        final TaskResult result =
                createShutdownTask(SHARD_END, constructChildrenFromSplit()).call();

        assertNull(result.getException());
        verify(shardRecordProcessor).leaseLost(any(LeaseLostInput.class));
        verify(shardRecordProcessor, never()).shardEnded(any(ShardEndedInput.class));
        // The task stopped at takeShardEndAction()'s lease check: no lease read, no child leases created, and nothing
        // queued for cleanup.
        verify(leaseRefresher, never()).getLease(any(String.class));
        verify(leaseRefresher, never()).createLeaseIfNotExists(any(Lease.class));
        verify(leaseCleanupManager, never()).enqueueForDeletion(any(LeasePendingDeletion.class));
    }

    /**
     * The same case as the test above, with a LEASE_LOST retry (row 6 of the table in ShardConsumerTest: the
     * Scheduler noticed the lost lease before the retry was created). The LEASE_LOST branch calls the record
     * processor's leaseLost() directly, without going through takeShardEndAction(). The branch only transfers and
     * drops the lease first if it's still held with a graceful shutdown requested, which doesn't apply here (see the
     * testLeaseLostWithShutdownRequested... tests for that case).
     */
    @Test
    public final void testLeaseLostAfterShardEndedThrewBeforeCheckpoint_leaseLostRetry_callsLeaseLost()
            throws Exception {
        leaseLostAfterShardEndedThrewBeforeCheckpoint();

        final TaskResult result =
                createShutdownTask(LEASE_LOST, constructChildrenFromSplit()).call();

        assertNull(result.getException());
        verify(shardRecordProcessor).leaseLost(any(LeaseLostInput.class));
        verify(shardRecordProcessor, never()).shardEnded(any(ShardEndedInput.class));
    }

    /**
     * shardEnded() was called (sequenceNumberAtShardEnd is set) but didn't checkpoint (lastCheckpointValue is an
     * earlier sequence number), and this worker no longer holds the lease.
     */
    private void leaseLostAfterShardEndedThrewBeforeCheckpoint() {
        when(recordProcessorCheckpointer.sequenceNumberAtShardEnd()).thenReturn(new ExtendedSequenceNumber("1234"));
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("1000"));
        when(leaseCoordinator.getCurrentlyHeldLease(lease.leaseKey())).thenReturn(null);
    }

    /**
     * This worker took a lease with no childShardIds that was already at SHARD_END, then lost it before its
     * ShutdownTask ran. The record processor still gets leaseLost(), with either shutdown reason, as before
     * leader-side SHARD_END cleanup was introduced.
     *
     * <p>Such a lease comes from a fetch that hit ResourceNotFound (for example, the stream was deleted). The leader
     * never deletes or evicts it, and it stays assignable, so this worker can get it when an earlier owner's lease
     * expired. An earlier owner's shardEnded() wrote the SHARD_END checkpoint. This worker's checkpointer starts at
     * SHARD_END (it's seeded from the lease when the consumer initializes), and the ShardConsumer goes straight to a
     * SHARD_END shutdown, where shardEnded() isn't called again because the lease is already at SHARD_END. So
     * sequenceNumberAtShardEnd is never set, and the check for an already-completed shard end doesn't apply.
     *
     * <p>Since the leader never removes this lease, this worker can only lose it in ways unrelated to leader-side
     * SHARD_END cleanup: its own renewals fail until the lease expires, a rebalance moves the lease, or the earlier
     * owner's LeaseCleanupManager deletes it (its in-memory queue entry outlives its ownership, and for a lease with
     * no children it deletes once Kinesis returns ResourceNotFound for the shard).
     *
     * <p>Either way, this worker really lost the lease, and its record processor got initialize() but never
     * shardEnded(), so leaseLost() is its one terminal callback. Callbacks are per processor: the earlier owner's
     * processor got its own.
     */
    @Test
    public final void testLeaseLostWhenCheckpointerStartedAtShardEnd_leaseWithoutChildren_callsLeaseLost()
            throws Exception {
        assertLeaseLostWhenCheckpointerStartedAtShardEnd(Collections.emptyList());
    }

    /**
     * Same as above, for a lease with childShardIds. The leader never assigns such a lease once it's at SHARD_END,
     * so this worker can only get it through a race: the leader's assignment is based on a lease table scan from
     * before the lease reached SHARD_END, and assignLease only checks the lease's owner, not its checkpoint. For
     * example, the owner was alive but its renewals were failing, so the leader saw the lease as expired; the owner
     * then wrote the SHARD_END checkpoint before the leader's assignLease call.
     *
     * <p>This worker can lose such a lease the same ways as above, and also through leader-side SHARD_END cleanup:
     * the leader evicts it on its next run (or deletes it, once the children have started), because it's owned and
     * at SHARD_END with childShardIds. The record processor still gets leaseLost(), for the same reasons.
     */
    @Test
    public final void testLeaseLostWhenCheckpointerStartedAtShardEnd_leaseWithChildren_callsLeaseLost()
            throws Exception {
        assertLeaseLostWhenCheckpointerStartedAtShardEnd(constructChildrenFromSplit());
    }

    /**
     * Runs a shutdown with each reason (SHARD_END, then LEASE_LOST) for a lease this worker no longer holds, and
     * checks the record processor gets leaseLost() each time. The record processor's checkpointer is at SHARD_END:
     * setUp stubs its lastCheckpointValue() to SHARD_END, standing in for the value it was seeded with from the lease
     * when the consumer initialized. sequenceNumberAtShardEnd is never set, because shardEnded() was never called.
     *
     * <p>The lease table's copy of the lease ({@code lease}, which leaseRefresher.getLease() returns) is set to
     * SHARD_END too, to match the scenario, but neither run reads it: the SHARD_END path stops at its not-held check
     * before reading the lease, and the LEASE_LOST path never reads it.
     */
    private void assertLeaseLostWhenCheckpointerStartedAtShardEnd(final List<ChildShard> childShards) throws Exception {
        lease.checkpoint(ExtendedSequenceNumber.SHARD_END);
        lease.childShardIds(childShards.stream().map(ChildShard::shardId).collect(Collectors.toList()));
        when(leaseCoordinator.getCurrentlyHeldLease(lease.leaseKey())).thenReturn(null);

        for (ShutdownReason reason : Arrays.asList(SHARD_END, LEASE_LOST)) {
            final TaskResult result = createShutdownTask(reason, childShards).call();

            assertNull(reason.toString(), result.getException());
            verify(shardRecordProcessor).leaseLost(any(LeaseLostInput.class));
            verify(shardRecordProcessor, never()).shardEnded(any(ShardEndedInput.class));
            reset(shardRecordProcessor);
        }
        verify(leaseRefresher, never()).getLease(any(String.class));
    }

    /**
     * The first attempt: shardEnded() checkpoints at SHARD_END (recorded in the lease and in the mocked
     * checkpointer, which ShutdownTask marks with sequenceNumberAtShardEnd just before calling shardEnded()), then
     * throws. Then the leader evicts the lease, so this worker no longer holds it.
     */
    private void failFirstShardEndedAfterCheckpointThenEvict() throws Exception {
        doAnswer(invocation -> {
                    lease.checkpoint(ExtendedSequenceNumber.SHARD_END); // the application's checkpoint
                    when(recordProcessorCheckpointer.sequenceNumberAtShardEnd())
                            .thenReturn(new ExtendedSequenceNumber("1234"));
                    throw new RuntimeException("work after the checkpoint failed");
                })
                .when(shardRecordProcessor)
                .shardEnded(any(ShardEndedInput.class));

        final TaskResult firstAttempt = task.call();
        assertTrue(firstAttempt.getException() instanceof CustomerApplicationException);

        when(leaseCoordinator.getCurrentlyHeldLease(lease.leaseKey())).thenReturn(null); // the leader evicts
    }

    private void verifyRetryMadeNoApplicationCalls() {
        verify(shardRecordProcessor, times(1)).shardEnded(any(ShardEndedInput.class));
        verifyShutdownAndNoDrop();
        verify(leaseCleanupManager, never()).enqueueForDeletion(any(LeasePendingDeletion.class));
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
        final Lease lease = LeaseHelper.createLease(leaseKey, LEASE_OWNER, parentShardIds);
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

    private ShutdownTask createShutdownTask(
            final ShutdownReason reason, final List<ChildShard> childShards, final ShardInfo shardInfo) {
        return new ShutdownTask(
                shardInfo,
                shardDetector,
                shardRecordProcessor,
                recordProcessorCheckpointer,
                reason,
                INITIAL_POSITION_TRIM_HORIZON,
                false,
                false,
                leaseCoordinator,
                TASK_BACKOFF_TIME_MILLIS,
                recordsPublisher,
                hierarchicalShardSyncer,
                NULL_METRICS_FACTORY,
                childShards,
                STREAM_IDENTIFIER,
                leaseCleanupManager);
    }
}
