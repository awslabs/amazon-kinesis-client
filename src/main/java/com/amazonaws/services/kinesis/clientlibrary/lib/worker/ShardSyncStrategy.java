package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.Shard;

import java.util.List;

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
     * Invoked when ShardConsumer is shutdown and all shards are provided.
     *
     * @param latestShards - Optional parameter, can be null. Pass in parameter to reuse output from ListShards API.
     * @return
     */
    default TaskResult onShardConsumerShutDown(List<Shard> latestShards) {
        return onShardConsumerShutDown();
    }

    /**
     * Invoked when worker is shutdown.
     */
    void onWorkerShutDown();
}
