package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

/**
 * An implementation of ShardSyncStrategy.
 */
public class PeriodicShardSyncStrategy implements ShardSyncStrategy {

    public static final String NAME = "PeriodicShardSyncStrategy";
    private PeriodicShardSyncManager periodicShardSyncManager;

    public PeriodicShardSyncStrategy(PeriodicShardSyncManager periodicShardSyncManager) {
        this.periodicShardSyncManager = periodicShardSyncManager;
    }

    @Override public String getName() {
        return NAME;
    }

    @Override public TaskResult syncShards() {
        return periodicShardSyncManager.start();
    }

    @Override public TaskResult onWorkerInitialization() {
        return syncShards();
    }

    @Override public TaskResult onFoundCompletedShard() {
        return new TaskResult(null);
    }

    @Override public TaskResult onShardConsumerShutDown() {
        return new TaskResult(null);
    }

    @Override public void onWorkerShutDown() {
        periodicShardSyncManager.stop();
    }
}
