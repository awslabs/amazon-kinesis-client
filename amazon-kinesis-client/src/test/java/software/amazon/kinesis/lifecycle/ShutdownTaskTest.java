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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.kinesis.model.ChildShard;
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
import software.amazon.kinesis.leases.exceptions.CustomerApplicationException;
import software.amazon.kinesis.leases.exceptions.DependencyException;
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
                hierarchicalShardSyncer, NULL_METRICS_FACTORY, constructChildShards());
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that customer doesn't implement checkpoint in their implementation
     */
    @Test
    public final void testCallWhenApplicationDoesNotCheckpoint() {
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("3298"));
        when(leaseCoordinator.getCurrentlyHeldLease("shardId-0")).thenReturn(createLease());
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);

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
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(leaseRefresher.createLeaseIfNotExists(Matchers.any(Lease.class))).thenThrow(new KinesisClientLibIOException("KinesisClientLibIOException"));

        final TaskResult result = task.call();
        assertNotNull(result.getException());
        assertTrue(result.getException() instanceof KinesisClientLibIOException);
        verify(recordsPublisher, never()).shutdown();
        verify(shardRecordProcessor, never()).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor, never()).leaseLost(LeaseLostInput.builder().build());
        verify(leaseCoordinator, never()).dropLease(Matchers.any(Lease.class));
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
                                hierarchicalShardSyncer, NULL_METRICS_FACTORY, constructChildShards());

        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        when(leaseCoordinator.getCurrentlyHeldLease("shardId-0")).thenReturn(createLease());
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor, never()).leaseLost(LeaseLostInput.builder().build());
        verify(leaseCoordinator).updateLease(Matchers.any(Lease.class), Matchers.any(UUID.class), Matchers.anyString(), Matchers.anyString());
        verify(leaseRefresher, times(2)).createLeaseIfNotExists(Matchers.any(Lease.class));
        verify(leaseCoordinator, never()).dropLease(Matchers.any(Lease.class));
    }

    /**
     * Test method for {@link ShutdownTask#call()}.
     * This test is for the scenario that a ShutdownTask is created for detecting a false Shard End.
     */
    @Test
    public final void testCallWhenShardNotFound() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        shardInfo = new ShardInfo("shardId-4", concurrencyToken, Collections.emptySet(),
                                  ExtendedSequenceNumber.LATEST);
        task = new ShutdownTask(shardInfo, shardDetector, shardRecordProcessor, recordProcessorCheckpointer,
                                SHARD_END_SHUTDOWN_REASON, INITIAL_POSITION_TRIM_HORIZON, cleanupLeasesOfCompletedShards,
                                ignoreUnexpectedChildShards, leaseCoordinator, TASK_BACKOFF_TIME_MILLIS, recordsPublisher,
                                hierarchicalShardSyncer, NULL_METRICS_FACTORY, new ArrayList<>());

        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor, never()).leaseLost(LeaseLostInput.builder().build());
        verify(leaseCoordinator, never()).getCurrentlyHeldLease(shardInfo.shardId());
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
                                hierarchicalShardSyncer, NULL_METRICS_FACTORY, new ArrayList<>());

        final TaskResult result = task.call();
        assertNull(result.getException());
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never()).shardEnded(ShardEndedInput.builder().checkpointer(recordProcessorCheckpointer).build());
        verify(shardRecordProcessor).leaseLost(LeaseLostInput.builder().build());
        verify(leaseCoordinator, never()).getAssignments();
        verify(leaseRefresher, never()).createLeaseIfNotExists(Matchers.any(Lease.class));
        verify(leaseCoordinator, never()).dropLease(Matchers.any(Lease.class));
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

    private Lease createLease() {
        Lease lease = new Lease();
        lease.leaseKey("shardId-0");
        lease.leaseOwner("leaseOwner");
        lease.parentShardIds(Collections.singleton("parentShardIds"));

        return lease;
    }

}
