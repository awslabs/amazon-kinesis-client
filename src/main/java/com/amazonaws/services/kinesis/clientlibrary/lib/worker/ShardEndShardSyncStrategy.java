package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.amazonaws.services.kinesis.model.Shard;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of ShardSyncStrategy that facilitates shard syncs when a shard completes processing.
 */
class ShardEndShardSyncStrategy implements ShardSyncStrategy {

    private static final Log LOG = LogFactory.getLog(Worker.class);
    private ShardSyncTaskManager shardSyncTaskManager;

    /** Runs periodic shard sync jobs in the background as an auditor process for shard-end syncs. */
    private PeriodicShardSyncManager periodicShardSyncManager;

    ShardEndShardSyncStrategy(ShardSyncTaskManager shardSyncTaskManager,
                              PeriodicShardSyncManager periodicShardSyncManager) {
        this.shardSyncTaskManager = shardSyncTaskManager;
        this.periodicShardSyncManager = periodicShardSyncManager;
    }

    @Override
    public ShardSyncStrategyType getStrategyType() {
        return ShardSyncStrategyType.SHARD_END;
    }

    @Override
    public TaskResult syncShards() {
        Future<TaskResult> taskResultFuture = null;
        TaskResult result = null;
        while (taskResultFuture == null) {
            taskResultFuture = shardSyncTaskManager.syncShardAndLeaseInfo(null);
        }
        try {
            result = taskResultFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("ShardEndShardSyncStrategy syncShards encountered exception.", e);
        }
        return result;
    }

    @Override
    public TaskResult onWorkerInitialization() {
        LOG.info("Starting periodic shard sync background process for SHARD_END shard sync strategy.");
        return periodicShardSyncManager.start();
    }

    @Override
    public TaskResult onFoundCompletedShard() {
        shardSyncTaskManager.syncShardAndLeaseInfo(null);
        return new TaskResult(null);
    }

    @Override
    public TaskResult onShardConsumerShutDown() {
        return onFoundCompletedShard();
    }

    @Override
    public TaskResult onShardConsumerShutDown(List<Shard> latestShards) {
        shardSyncTaskManager.syncShardAndLeaseInfo(latestShards);
        return new TaskResult(null);
    }

    @Override
    public void onWorkerShutDown() {
        LOG.info("Stopping periodic shard sync background process for SHARD_END shard sync strategy.");
        periodicShardSyncManager.stop();
    }
}
