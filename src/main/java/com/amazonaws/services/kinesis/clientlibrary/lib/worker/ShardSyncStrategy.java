package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

/**
 * Facade of methods that can be invoked at different points
 * in KCL application execution to perform certain actions related to shard-sync.
 */
public interface ShardSyncStrategy {

    /**
     * Can be used to provide a custom name for the implemented strategy.
     *
     * @return Name of the strategy.
     */
    ShardSyncStrategyType getStrategyType();

    /**
     * Invoked when the KCL application wants to execute shard-sync.
     *
     * @return TaskResult
     */
    TaskResult syncShards();

    /**
     * Invoked at worker initialization
     *
     * @return
     */
    TaskResult onWorkerInitialization();

    /**
     * Invoked when a completed shard is found.
     *
     * @return
     */
    TaskResult onFoundCompletedShard();

    /**
     * Invoked when ShardConsumer is shutdown.
     *
     * @return
     */
    TaskResult onShardConsumerShutDown();

    /**
     * Invoked when worker is shutdown.
     */
    void onWorkerShutDown();
}
