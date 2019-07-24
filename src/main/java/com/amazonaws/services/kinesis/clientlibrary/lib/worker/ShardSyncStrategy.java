package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

public interface ShardSyncStrategy {

    String getName();

    TaskResult syncShards();

    TaskResult onWorkerInitialization();

    TaskResult foundCompletedShard();

    TaskResult onShutDown();

    void stop();
}
