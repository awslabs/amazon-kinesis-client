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

    ShardEndShardSyncStrategy(ShardSyncTaskManager shardSyncTaskManager) {
        this.shardSyncTaskManager = shardSyncTaskManager;
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
        // TODO: Start leaderElectedPeriodicShardSyncManager in background
        LOG.debug(String.format("onWorkerInitialization is NoOp for ShardSyncStrategyType %s", getStrategyType().toString()));
        return new TaskResult(null);
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
        // TODO: Shut down leaderElectedPeriodicShardSyncManager
        LOG.debug(String.format("Stop is NoOp for ShardSyncStrategyType %s", getStrategyType().toString()));
    }
}
