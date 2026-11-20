/*
 * Copyright 2026 Amazon.com, Inc. or its affiliates.
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InitializeTaskTest {

    private static final String SHARD_ID = "shardId-000000000000";
    private static final InitialPositionInStreamExtended INITIAL_POSITION =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

    @Mock
    private ShardRecordProcessor shardRecordProcessor;

    @Mock
    private LeaseCoordinator leaseCoordinator;

    @Mock
    private ShardRecordProcessorCheckpointer recordProcessorCheckpointer;

    @Mock
    private RecordsPublisher recordsPublisher;

    private InitializeTask task;

    @Before
    public void setup() {
        final ShardInfo shardInfo = new ShardInfo(SHARD_ID, "concurrencyToken", null, ExtendedSequenceNumber.LATEST);
        task = new InitializeTask(
                shardInfo,
                shardRecordProcessor,
                leaseCoordinator,
                recordProcessorCheckpointer,
                INITIAL_POSITION,
                recordsPublisher,
                0L,
                new NullMetricsFactory());
    }

    /** The checkpoint comes from the lease this worker holds, not from a DynamoDB read. */
    @Test
    public void call_usesCheckpointFromHeldLease() {
        final ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("1000");
        final ExtendedSequenceNumber pendingCheckpoint = new ExtendedSequenceNumber("2000");
        final Lease lease = new Lease();
        lease.leaseKey(SHARD_ID);
        lease.checkpoint(checkpoint);
        lease.pendingCheckpoint(pendingCheckpoint);
        when(leaseCoordinator.getCurrentlyHeldLease(SHARD_ID)).thenReturn(lease);

        final TaskResult result = task.call();

        assertNull(result.getException());
        verify(recordsPublisher).start(checkpoint, INITIAL_POSITION);
        verify(recordProcessorCheckpointer).largestPermittedCheckpointValue(checkpoint);
        verify(recordProcessorCheckpointer).setInitialCheckpointValue(checkpoint);
        final ArgumentCaptor<InitializationInput> input = ArgumentCaptor.forClass(InitializationInput.class);
        verify(shardRecordProcessor).initialize(input.capture());
        assertEquals(checkpoint, input.getValue().extendedSequenceNumber());
        assertEquals(pendingCheckpoint, input.getValue().pendingCheckpointSequenceNumber());
    }

    /** If the lease is no longer held, the task fails without starting the publisher or the record processor. */
    @Test
    public void call_leaseNotHeld_failsWithoutInitializing() {
        when(leaseCoordinator.getCurrentlyHeldLease(SHARD_ID)).thenReturn(null);

        final TaskResult result = task.call();

        assertTrue(result.getException() instanceof ShutdownException);
        verify(recordsPublisher, never()).start(any(), any());
        verify(shardRecordProcessor, never()).initialize(any());
    }
}
