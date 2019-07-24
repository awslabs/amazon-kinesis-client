package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.Future;

@RunWith(MockitoJUnitRunner.class)
public class ShardEndShardSyncStrategyTest {

    @Mock
    private ShardSyncTaskManager shardSyncTaskManager;
    @Mock
    Future result;
    private ShardEndShardSyncStrategy shardSyncStrategy;

    @Before
    public void setup() {
        shardSyncStrategy = new ShardEndShardSyncStrategy(shardSyncTaskManager);
    }

    @Test
    public void testSyncShards() {
        when(shardSyncTaskManager.syncShardAndLeaseInfo(null)).thenReturn(result);
        shardSyncStrategy.syncShards();
        verify(shardSyncTaskManager, times(1)).syncShardAndLeaseInfo(null);
    }

    @Test
    public void testSyncShardsRetriesIfInflightShardSyncTaskPresent() {
        when(shardSyncTaskManager.syncShardAndLeaseInfo(null)).thenReturn(null).thenReturn(result);
        shardSyncStrategy.syncShards();
        verify(shardSyncTaskManager, times(2)).syncShardAndLeaseInfo(null);
    }

    @Test
    public void testNoOpOnWorkerInitialization() {
        shardSyncStrategy.onWorkerInitialization();
        verifyZeroInteractions(shardSyncTaskManager);
    }

    @Test
    public void testFoundCompletedShard() {
        when(shardSyncTaskManager.syncShardAndLeaseInfo(null)).thenReturn(result);
        shardSyncStrategy.foundCompletedShard();
        verify(shardSyncTaskManager, times(1)).syncShardAndLeaseInfo(null);
    }

    @Test
    public void testOnShutdown() {
        when(shardSyncTaskManager.syncShardAndLeaseInfo(null)).thenReturn(result);
        shardSyncStrategy.onShutDown();
        verify(shardSyncTaskManager, times(1)).syncShardAndLeaseInfo(null);
    }

    @Test
    public void testNoOpOnStop() {
        shardSyncStrategy.onWorkerInitialization();
        verifyZeroInteractions(shardSyncTaskManager);
    }
}
