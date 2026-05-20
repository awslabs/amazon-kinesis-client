package software.amazon.kinesis.lifecycle;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseHelper;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseCoordinator;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LeaseGracefulShutdownHandlerTest {

    private static final String WORKER_ID = "workerId";
    private static final long SHUTDOWN_TIMEOUT = 5000L;

    private final Lease lease = LeaseHelper.createLease("shardId-0", "leaseOwner", Collections.emptyList());
    private final ConcurrentMap<ShardInfo, ShardConsumer> shardConsumerMap = new ConcurrentHashMap<>();

    private LeaseGracefulShutdownHandler handler;
    private Runnable gracefulShutdownRunnable;

    @Mock
    private LeaseCoordinator mockLeaseCoordinator;

    @Mock
    private Supplier<Long> mockTimeSupplier;

    @Mock
    private ShardConsumer mockShardConsumer;

    @Mock
    private ScheduledExecutorService mockScheduledExecutorService;

    @Mock
    private LeaseRefresher mockLeaseRefresher;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(mockScheduledExecutorService.scheduleAtFixedRate(
                        any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    Object[] args = invocation.getArguments();
                    this.gracefulShutdownRunnable = (Runnable) args[0];
                    return mock(ScheduledFuture.class);
                });
        when(mockLeaseCoordinator.leaseRefresher()).thenReturn(mockLeaseRefresher);
        when(mockLeaseRefresher.assignLease(any(Lease.class), any(String.class)))
                .thenReturn(true);

        when(mockLeaseCoordinator.workerIdentifier()).thenReturn(WORKER_ID);
        when(mockTimeSupplier.get()).thenReturn(0L);

        handler = new LeaseGracefulShutdownHandler(
                SHUTDOWN_TIMEOUT,
                shardConsumerMap,
                mockLeaseCoordinator,
                mockTimeSupplier,
                mockScheduledExecutorService);

        lease.checkpointOwner(WORKER_ID);
        lease.concurrencyToken(UUID.randomUUID());
        when(mockLeaseCoordinator.getCurrentlyHeldLease(lease.leaseKey())).thenReturn(lease);
        handler.start();
    }

    @Test
    void testSubsequentStarts() {
        handler.start();
        handler.start();
        verify(mockScheduledExecutorService)
                .scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void testSubsequentShutdowns() {
        handler.start();
        handler.stop();
        handler.stop();
        verify(mockScheduledExecutorService).shutdown();
    }

    @Test
    void testIgnoreDuplicatEnqueues() {
        final ShardInfo shardInfo = DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease);
        shardConsumerMap.put(shardInfo, mockShardConsumer);
        handler.enqueueShutdown(lease);
        // check gracefulShutdown is called
        verify(mockShardConsumer, times(1)).gracefulShutdown(null);

        // enqueue the same lease again to make sure it doesn't cause another shutdown call
        handler.enqueueShutdown(lease);
        verify(mockShardConsumer, times(1)).gracefulShutdown(null);

        // adding another lease to check it's enqueued
        final Lease lease2 = createShardConsumerForLease("shardId-2");
        handler.enqueueShutdown(lease2);
        verify(shardConsumerMap.get(DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease2)), times(1))
                .gracefulShutdown(null);
    }

    @Test
    void testEnqueueShutdownTransfersLeaseWhenConsumerWasNeverStarted() throws Exception {
        // Reproduces the bug where a worker discovers a lease with checkpointOwner=self
        // in DDB after a restart (or right after acquiring the lease via cold transfer
        // and before the ShardConsumer state machine reaches PROCESSING). In this case
        // shardInfoShardConsumerMap has no entry, AND shardInfoLeasePendingShutdownMap
        // has no entry (the renewer has not previously called enqueueShutdown for this
        // lease). The fix must still perform the DDB transfer to clear the
        // checkpointOwner field; otherwise the lease becomes a permanent zombie.
        // Note: shardConsumerMap is intentionally empty, no prior enqueueShutdown call
        // was made, so the pendingShutdown map is also empty.
        handler.enqueueShutdown(lease);
        verify(mockLeaseRefresher).assignLease(lease, lease.leaseOwner());
    }

    @Test
    void testMonitorTransfersLeaseWhenConsumerShutsDownBeforeTimeout() throws Exception {
        // Reproduces the most common manifestation of the bug. The worker starts a
        // consumer for the lease, the renewer fires enqueueShutdown which schedules
        // gracefulShutdown and adds an entry to the pendingShutdown map. The consumer
        // shuts down quickly (faster than the 30s timeout, common for low-traffic
        // shards). The next monitor cycle observes consumer == null in
        // shardInfoShardConsumerMap (Branch A path). The original code silently
        // removed the pending entry without performing the DDB transfer. The fix
        // performs the transfer.
        final ShardInfo shardInfo = DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease);
        shardConsumerMap.put(shardInfo, mockShardConsumer);
        when(mockShardConsumer.isShutdown()).thenReturn(false);
        handler.enqueueShutdown(lease);

        // simulate the consumer completing its shutdown and being removed from the map
        // before the configured timeout fires
        shardConsumerMap.remove(shardInfo);
        gracefulShutdownRunnable.run();

        verify(mockLeaseRefresher).assignLease(lease, lease.leaseOwner());
    }

    @Test
    void testIgnoreNonPendingShutdownLease() throws Exception {
        // enqueue a none shutdown lease
        lease.checkpointOwner(null);
        handler.enqueueShutdown(lease);
        verify(mockShardConsumer, never()).gracefulShutdown(null);
        verify(mockLeaseRefresher, never()).assignLease(any(Lease.class), any((String.class)));
    }

    @Test
    void testMonitorGracefulShutdownLeases() throws Exception {
        final ShardInfo shardInfo = DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease);
        shardConsumerMap.put(shardInfo, mockShardConsumer);
        handler.enqueueShutdown(lease);

        handler.start();
        gracefulShutdownRunnable.run();

        // check gracefulShutdown is called
        verify(mockShardConsumer).gracefulShutdown(null);

        // run again. this is no op because shutdown is already called and checkpoint is not expired
        gracefulShutdownRunnable.run();
        verify(mockShardConsumer).gracefulShutdown(null);

        // make it return true which should cause
        when(mockShardConsumer.isShutdown()).thenReturn(true);
        gracefulShutdownRunnable.run();
        verify(mockLeaseRefresher, never()).assignLease(any(Lease.class), any((String.class)));
    }

    @Test
    void testEnqueueShutdownTransfersLeaseWhenShardConsumerNotFound() throws Exception {
        // When enqueueShutdown is called for a lease that has shutdownRequested but no
        // ShardConsumer is registered (e.g., LAM initiated graceful handoff before the
        // worker started a consumer for this lease, or the consumer has already
        // completed shutdown), the worker must still perform the DDB lease transfer
        // to clear the checkpointOwner field. Otherwise the lease becomes a zombie:
        // the new owner can't take over and no consumer is processing records.
        when(mockShardConsumer.isShutdown()).thenReturn(true);
        handler.enqueueShutdown(lease);
        verify(mockLeaseRefresher).assignLease(lease, lease.leaseOwner());
    }

    @Test
    void testAssignLeaseIsCalledBecauseTimeoutReached() throws Exception {
        final ShardInfo shardInfo = DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease);
        shardConsumerMap.put(shardInfo, mockShardConsumer);
        when(mockShardConsumer.isShutdown()).thenReturn(false);
        when(mockTimeSupplier.get()).thenReturn(0L);
        handler.enqueueShutdown(lease);

        handler.start();
        gracefulShutdownRunnable.run();

        verify(mockShardConsumer).gracefulShutdown(null);

        // Timeout << SHUTDOWN_TIMEOUT
        verify(mockLeaseRefresher, never()).assignLease(lease, lease.leaseOwner());

        // Timeout < SHUTDOWN_TIMEOUT
        when(mockTimeSupplier.get()).thenReturn(SHUTDOWN_TIMEOUT - 1000);
        gracefulShutdownRunnable.run();
        verify(mockLeaseRefresher, never()).assignLease(lease, lease.leaseOwner());

        // Timeout > SHUTDOWN_TIMEOUT
        when(mockTimeSupplier.get()).thenReturn(SHUTDOWN_TIMEOUT + 1000);
        gracefulShutdownRunnable.run();
        verify(mockLeaseRefresher).assignLease(lease, lease.leaseOwner());
    }

    @Test
    void testTransferLeaseWhenLeaseCoordinatorNoLongerHoldsItInMemory() throws Exception {
        final ShardInfo shardInfo = DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease);
        shardConsumerMap.put(shardInfo, mockShardConsumer);
        when(mockShardConsumer.isShutdown()).thenReturn(false);
        // fast-forward and time out the shutdown lease.
        when(mockTimeSupplier.get()).thenReturn(SHUTDOWN_TIMEOUT + 1000);
        // The lease coordinator no longer holds this lease in its in-memory map
        // (e.g., the lease was dropped locally). However, the DDB record still has
        // checkpointOwner=self because the previous code path silently removed the
        // pending-shutdown entry without performing the transfer. The fix performs
        // the transfer so the DDB state is cleaned up and the next owner can pick
        // up the lease.
        when(mockLeaseCoordinator.getCurrentlyHeldLease(lease.leaseKey())).thenReturn(null);
        handler.enqueueShutdown(lease);

        gracefulShutdownRunnable.run();
        verify(mockLeaseRefresher).assignLease(lease, lease.leaseOwner());
    }

    @Test
    void testAssignLeaseIsNotCalledIfCheckpointOwnerIsNotTheSameWorker() throws Exception {
        final ShardInfo shardInfo = DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease);
        shardConsumerMap.put(shardInfo, mockShardConsumer);
        when(mockShardConsumer.isShutdown()).thenReturn(false);
        handler.enqueueShutdown(lease);
        // make it expire during timeout check
        when(mockTimeSupplier.get()).thenReturn(0L).thenReturn(SHUTDOWN_TIMEOUT + 1000);
        // set checkpoint owner to some random worker
        lease.checkpointOwner("random_owner");

        handler.start();
        gracefulShutdownRunnable.run();

        verify(mockLeaseRefresher, never()).assignLease(any(Lease.class), any((String.class)));
    }

    private Lease createShardConsumerForLease(String shardId) {
        final Lease lease = LeaseHelper.createLease(shardId, "leaseOwner", Collections.emptyList());
        lease.checkpointOwner(WORKER_ID);
        lease.concurrencyToken(UUID.randomUUID());
        shardConsumerMap.put(DynamoDBLeaseCoordinator.convertLeaseToAssignment(lease), mock(ShardConsumer.class));
        when(mockLeaseCoordinator.getCurrentlyHeldLease(lease.leaseKey())).thenReturn(lease);
        return lease;
    }
}
