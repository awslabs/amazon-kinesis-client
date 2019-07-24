package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ShardEndShardSyncStrategy implements ShardSyncStrategy {

    private static final Log LOG = LogFactory.getLog(Worker.class);
    private ShardSyncTaskManager shardSyncTaskManager;
    public static final String NAME = "ShardEndShardSyncStrategy";

    public ShardEndShardSyncStrategy(ShardSyncTaskManager shardSyncTaskManager) {
        this.shardSyncTaskManager = shardSyncTaskManager;
    }

    @Override public String getName() {
        return NAME;
    }

    @Override
    public TaskResult syncShards() {
        Future<TaskResult> taskResultFuture = null;
        TaskResult result = null;
        while(taskResultFuture == null) {
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
        LOG.debug(String.format("onWorkerInitialization is NoOp for %s", NAME));
        return new TaskResult(null);
    }

    @Override public TaskResult foundCompletedShard() {
        return syncShards();
    }

    @Override public TaskResult onShutDown() {
        return syncShards();
    }

    @Override public void stop() {
        LOG.debug(String.format("Stop is NoOp for %s", NAME));
    }
}
