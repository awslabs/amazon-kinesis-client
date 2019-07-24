/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This implementation is responsible for scheduling periodic shard syncs on a
 * worker based on the values such as Max concurrency, jitter, frequency etc set
 * by the client in the config
 */
class PeriodicShardSyncScheduler {

    private final IKinesisProxy kinesisProxy;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private InitialPositionInStreamExtended initialPosition;
    private final KinesisClientLibConfiguration config;
    private final ShardSyncer shardSyncer;
    private final ScheduledThreadPoolExecutor scheduledExecutor;
    private ScheduledFuture scheduledFuture;
    private boolean isRunning;
    private ScheduledLeaseLeaderPoller leaderPoller;
    private String workerId;

    private static final Log LOG = LogFactory.getLog(ScheduledLeaseLeaderPoller.class);
    private static final long SHARD_SYNC_TASK_IDLE_TIME_MILLIS = 0;
    private static final long INITIAL_DELAY = 0;
    private static final int AWAIT_TERMINATION_SECS = 5;
    private static final long PERIODIC_SHARD_SYNC_FREQUENCY_MILLIS = 10000;
    private static final int PERIODIC_SHARD_SYNC_MAX_JITTER_MILLIS = 10;

    PeriodicShardSyncScheduler(IKinesisProxy kinesisProxy,
        ILeaseManager<KinesisClientLease> leaseManager,
        InitialPositionInStreamExtended initialPosition,
        KinesisClientLibConfiguration config,
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor,
        ShardSyncer shardSyncer,
        ScheduledLeaseLeaderPoller leaderPoller,
        String workerId) {
        this.initialPosition = initialPosition;
        this.kinesisProxy = kinesisProxy;
        this.leaseManager = leaseManager;
        this.config = config;
        this.scheduledExecutor = scheduledThreadPoolExecutor;
        this.shardSyncer = shardSyncer;
        this.leaderPoller = leaderPoller;
        this.workerId = workerId;
    }

    static class ShardSyncWithLeaderCheck implements Runnable {
        private ShardSyncTask shardSyncTask;
        private ScheduledLeaseLeaderPoller leaderPoller;
        private String workerId;

        private ShardSyncWithLeaderCheck(ShardSyncTask shardSyncTask,
            ScheduledLeaseLeaderPoller leaderPoller,
            String workerId) {
            this.shardSyncTask = shardSyncTask;
            this.leaderPoller = leaderPoller;
            this.workerId = workerId;
        }

        @Override public void run() {
            if (leaderPoller.isLeader(workerId)) {
                LOG.debug(String.format("WorkerId %s is a leader, running the shard sync task", workerId));
                shardSyncTask.run();
            } else {
                LOG.debug(String.format("WorkerId %s is not a leader, not running the shard sync task", workerId));
            }
        }
    }

    void scheduleTask(ShardSyncWithLeaderCheck task) {
        // adding jitter to stagger the syncs across workers
        Random jitterDelayGen = new Random();
        int jitterDelay = jitterDelayGen.nextInt(PERIODIC_SHARD_SYNC_MAX_JITTER_MILLIS);
        scheduledFuture = scheduledExecutor.scheduleAtFixedRate(task, INITIAL_DELAY,
            PERIODIC_SHARD_SYNC_FREQUENCY_MILLIS + jitterDelay,
            TimeUnit.MILLISECONDS);
    }

    public void start() {
        if (!isRunning) {
            ShardSyncWithLeaderCheck shardSyncTaskWithLeaderCheck = new ShardSyncWithLeaderCheck (new ShardSyncTask(kinesisProxy, leaseManager, initialPosition,
                config.shouldCleanupLeasesUponShardCompletion(), config.shouldIgnoreUnexpectedChildShards(),
                SHARD_SYNC_TASK_IDLE_TIME_MILLIS, shardSyncer), leaderPoller, workerId);
            scheduleTask(shardSyncTaskWithLeaderCheck);
            isRunning = true;
        }
    }

    public void shutdown() {
        try {
            scheduledExecutor.shutdown();
            if (scheduledExecutor.awaitTermination(AWAIT_TERMINATION_SECS, TimeUnit.SECONDS)) {
                LOG.info("Successfully stopped leader polling threads on the worker");
            } else {
                scheduledExecutor.shutdownNow();
                LOG.info(String.format("Stopped leader polling threads after awaiting termination for %d seconds",
                    AWAIT_TERMINATION_SECS));
            }
            isRunning = false;
        } catch (InterruptedException e) {
            LOG.debug("Encountered InterruptedException while awaiting leader polling threadpool termination");
        }
    }
}
