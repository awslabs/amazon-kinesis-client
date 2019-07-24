package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

public class PeriodicShardSyncStrategy implements ShardSyncStrategy {

    public static final String NAME = "PeriodicShardSyncStrategy";
    private PeriodicShardSyncManager periodicShardSyncManager;

    public PeriodicShardSyncStrategy(PeriodicShardSyncManager periodicShardSyncManager) {
        this.periodicShardSyncManager = periodicShardSyncManager;
    }

    @Override
    public String getName() {
        return NAME;
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
    public TaskResult foundCompletedShard() {
        return new TaskResult(null);
    }

    @Override public TaskResult onShutDown() {
        return new TaskResult(null);
    }

    @Override
    public void stop() {
        periodicShardSyncManager.stop();
    }
}
