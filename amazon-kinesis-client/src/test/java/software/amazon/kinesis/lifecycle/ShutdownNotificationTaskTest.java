package software.amazon.kinesis.lifecycle;

import java.util.Collections;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseHelper;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseCoordinator;
import software.amazon.kinesis.processor.ShardRecordProcessor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShutdownNotificationTaskTest {
    private static final String LEASE_OWNER = "leaseOwner";
    private final Lease lease = LeaseHelper.createLease("shardId-9", LEASE_OWNER, Collections.emptyList());

    @Mock
    private ShardRecordProcessorCheckpointer mockRecordProcessorCheckpointer;

    @Mock
    private LeaseRefresher mockLeaseRefresher;

    @Mock
    private LeaseCoordinator mockLeaseCoordinator;

    @Mock
    private ShardRecordProcessor mockShardRecordProcessor;

    private ShutdownNotificationTask shutdownNotificationTask;

    @BeforeEach
    void setup() {
        MockitoAnnotations.initMocks(this);
        lease.checkpointOwner("checkpoint_owner");
        lease.concurrencyToken(UUID.randomUUID());
        final ShardInfo shardInfo = DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease);
        shutdownNotificationTask = new ShutdownNotificationTask(
                mockShardRecordProcessor, mockRecordProcessorCheckpointer, null, shardInfo, mockLeaseCoordinator);
        when(mockLeaseCoordinator.getCurrentlyHeldLease(lease.leaseKey())).thenReturn(lease);
        when(mockLeaseCoordinator.workerIdentifier()).thenReturn(LEASE_OWNER);
        when(mockLeaseCoordinator.leaseRefresher()).thenReturn(mockLeaseRefresher);
    }

    @Test
    void testLeaseTransferCalledAsCheckpointOwnerExist() throws Exception {
        lease.checkpointOwner(LEASE_OWNER);
        shutdownNotificationTask.call();
        verify(mockLeaseRefresher).assignLease(lease, lease.leaseOwner());
        verify(mockLeaseCoordinator).dropLease(lease);
    }

    @Test
    void testLeaseTransferNotCalledAsCheckpointOwnerMisMatch() throws Exception {
        lease.checkpointOwner(null);
        shutdownNotificationTask.call();
        verify(mockLeaseRefresher, never()).assignLease(any(Lease.class), any(String.class));
        verify(mockLeaseCoordinator).dropLease(lease);

        lease.checkpointOwner("else");
        shutdownNotificationTask.call();
        verify(mockLeaseRefresher, never()).assignLease(any(Lease.class), any(String.class));
    }
}
