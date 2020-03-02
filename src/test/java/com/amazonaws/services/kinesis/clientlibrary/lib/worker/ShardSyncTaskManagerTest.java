package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.KinesisClientLibIOException;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;
import com.amazonaws.services.kinesis.model.Shard;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.matches;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardSyncTaskManagerTest {
    private static final InitialPositionInStreamExtended INITIAL_POSITION_IN_STREAM_EXTENDED = InitialPositionInStreamExtended
            .newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final boolean CLEANUP_LEASES_SHARD_COMPLETION = Boolean.TRUE;
    private static final boolean IGNORE_UNEXPECTED_CHILD_SHARDS = Boolean.TRUE;
    private static final long SHARD_SYNC_IDLE_TIME_MILLIS = 0;

    @Mock private IKinesisProxy mockKinesisProxy;
    @Mock private ILeaseManager<KinesisClientLease> mockLeaseManager;
    @Mock private IMetricsFactory mockMetricsFactory;
    @Mock private IMetricsScope mockMetricsScope;

    private ShardSyncTaskManager shardSyncTaskManager;
    private PausableNoOpShardSyncer pausableNoOpShardSyncer;
    private ShardSyncer mockShardSyncer;

    @Before public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(mockMetricsFactory.createMetrics()).thenReturn(mockMetricsScope);
        pausableNoOpShardSyncer = new PausableNoOpShardSyncer();
        mockShardSyncer = mock(ShardSyncer.class, delegatesTo(pausableNoOpShardSyncer));
        shardSyncTaskManager = new ShardSyncTaskManager(mockKinesisProxy, mockLeaseManager,
                INITIAL_POSITION_IN_STREAM_EXTENDED, CLEANUP_LEASES_SHARD_COMPLETION, IGNORE_UNEXPECTED_CHILD_SHARDS,
                SHARD_SYNC_IDLE_TIME_MILLIS, mockMetricsFactory, Executors.newSingleThreadExecutor(), mockShardSyncer);
    }

    @Test public void testShardSyncIdempotency() throws Exception {
        shardSyncTaskManager.syncShardAndLeaseInfo(new ArrayList<>());
        // Wait for ShardSyncer to be initialized.
        pausableNoOpShardSyncer.waitForShardSyncerInitializationLatch.await();
        verify(mockShardSyncer, times(1))
                .checkAndCreateLeasesForNewShards(Matchers.any(), Matchers.any(), Matchers.any(), anyBoolean(),
                        anyBoolean(), Matchers.any());
        // Invoke a few more times. This would flip shardSyncRequestPending to true in ShardSyncTaskManager.
        int count = 0;
        while (count++ < 5) {
            shardSyncTaskManager.syncShardAndLeaseInfo(new ArrayList<>());
        }
        // Since blockShardSyncLatch is still blocked, previous ShardSyncTask is still running, hence no new invocations.
        verify(mockShardSyncer, times(1))
                .checkAndCreateLeasesForNewShards(Matchers.any(), Matchers.any(), Matchers.any(), anyBoolean(),
                        anyBoolean(), Matchers.any());
        // countdown and exit.
        pausableNoOpShardSyncer.blockShardSyncLatch.countDown();
    }

    @Test public void testShardSyncRerunsForPendingRequests() throws Exception {
        shardSyncTaskManager.syncShardAndLeaseInfo(new ArrayList<>());
        // Wait for ShardSyncer to be initialized.
        pausableNoOpShardSyncer.waitForShardSyncerInitializationLatch.await();
        verify(mockShardSyncer, times(1))
                .checkAndCreateLeasesForNewShards(Matchers.any(), Matchers.any(), Matchers.any(), anyBoolean(),
                        anyBoolean(), Matchers.any());
        // Invoke a few more times. This would flip shardSyncRequestPending to true in ShardSyncTaskManager.
        int count = 0;
        while (count++ < 5) {
            shardSyncTaskManager.syncShardAndLeaseInfo(new ArrayList<>());
        }
        pausableNoOpShardSyncer.waitForShardSyncerInitializationLatch = new CountDownLatch(1);
        // unblock pending shardSync so a new ShardSync is triggered.
        pausableNoOpShardSyncer.blockShardSyncLatch.countDown();
        // Wait for ShardSyncer to be initialized.
        pausableNoOpShardSyncer.waitForShardSyncerInitializationLatch.await();
        // There should be totally 2 invocation of shardSyncer. The first one should be triggered with an empty list the latestShards.
        // The second invocation should be the pending shard sync task, which should have null as the latestShards.
        verify(mockShardSyncer, times(1))
                .checkAndCreateLeasesForNewShards(Matchers.any(), Matchers.any(), Matchers.any(), anyBoolean(),
                        anyBoolean(), Matchers.eq(new ArrayList<>()));
        verify(mockShardSyncer, times(1))
                .checkAndCreateLeasesForNewShards(Matchers.any(), Matchers.any(), Matchers.any(), anyBoolean(),
                        anyBoolean(), Matchers.eq(null));
    }

    private static class PausableNoOpShardSyncer implements ShardSyncer {

        private CountDownLatch blockShardSyncLatch;
        private CountDownLatch waitForShardSyncerInitializationLatch;

        PausableNoOpShardSyncer() {
            this.blockShardSyncLatch = new CountDownLatch(1);
            this.waitForShardSyncerInitializationLatch = new CountDownLatch(1);
        }

        @Override public void checkAndCreateLeasesForNewShards(IKinesisProxy kinesisProxy,
                ILeaseManager<KinesisClientLease> leaseManager, InitialPositionInStreamExtended initialPositionInStream,
                boolean cleanupLeasesOfCompletedShards, boolean ignoreUnexpectedChildShards)
                throws DependencyException, InvalidStateException, ProvisionedThroughputException,
                KinesisClientLibIOException {
            try {
                waitForShardSyncerInitializationLatch.countDown();
                blockShardSyncLatch.await();
            } catch (InterruptedException e) {
                // No-OP
            }
        }

        @Override public void checkAndCreateLeasesForNewShards(IKinesisProxy kinesisProxy,
                ILeaseManager<KinesisClientLease> leaseManager, InitialPositionInStreamExtended initialPositionInStream,
                boolean cleanupLeasesOfCompletedShards, boolean ignoreUnexpectedChildShards, List<Shard> latestShards)
                throws DependencyException, InvalidStateException, ProvisionedThroughputException,
                KinesisClientLibIOException {
            this.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, initialPositionInStream,
                    cleanupLeasesOfCompletedShards, ignoreUnexpectedChildShards);
        }
    }
}
