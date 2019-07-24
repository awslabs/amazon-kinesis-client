package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(MockitoJUnitRunner.class)
public class PeriodicShardSyncStrategyTest {

    @Mock
    private PeriodicShardSyncManager periodicShardSyncManager;
    private PeriodicShardSyncStrategy shardSyncStrategy;

    @Before
    public void setup() {
        shardSyncStrategy = new PeriodicShardSyncStrategy(periodicShardSyncManager);
    }

    @Test
    public void testSyncShards() {
        shardSyncStrategy.syncShards();
        verify(periodicShardSyncManager, times(1)).start();
    }

    @Test
    public void testWorkerInitialization() {
        shardSyncStrategy.onWorkerInitialization();
        verify(periodicShardSyncManager, times(1)).start();
    }

    @Test
    public void testNoOpOnFoundCompletedShard() {
        shardSyncStrategy.foundCompletedShard();
        verifyZeroInteractions(periodicShardSyncManager);
    }

    @Test
    public void testNoOpOnShutdown() {
        shardSyncStrategy.foundCompletedShard();
        verifyZeroInteractions(periodicShardSyncManager);
    }

    @Test
    public void testStop() {
        shardSyncStrategy.stop();
        verify(periodicShardSyncManager, times(1)).stop();
    }



}

