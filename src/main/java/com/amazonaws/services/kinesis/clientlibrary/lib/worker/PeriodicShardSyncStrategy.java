package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.Shard;

import java.util.List;

/**
 * An implementation of ShardSyncStrategy.
 */
class PeriodicShardSyncStrategy implements ShardSyncStrategy {

    private PeriodicShardSyncManager periodicShardSyncManager;

    PeriodicShardSyncStrategy(PeriodicShardSyncManager periodicShardSyncManager) {
        this.periodicShardSyncManager = periodicShardSyncManager;
    }

    @Override
    public ShardSyncStrategyType getStrategyType() {
        return ShardSyncStrategyType.PERIODIC;
    }

    @Override
    public TaskResult syncShards() {
        return periodicShardSyncManager.start();
    }

    @Override
    public TaskResult onWorkerInitialization() {
        return syncShards();
    }

    @Override
    public TaskResult onFoundCompletedShard() {
        return new TaskResult(null);
    }

    @Override
    public TaskResult onShardConsumerShutDown() {
        return new TaskResult(null);
    }

    @Override
    public void onWorkerShutDown() {
        periodicShardSyncManager.stop();
    }
}
