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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeaderElectionStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.LeaderPoller;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Leader Poller implementation polling for the leaders as per the config values
 * for frequency and concurrency
 *
 */
class ScheduledLeaseLeaderPoller implements LeaderPoller<KinesisClientLease> {

    private final LeaderElectionStrategy<KinesisClientLease> leaderElectionStrategy;
    private final KinesisClientLibConfiguration config;
    private final ScheduledThreadPoolExecutor scheduledExecutor;

    private static final Log LOG = LogFactory.getLog(ScheduledLeaseLeaderPoller.class);
    // Setting the initial delay to a minute so that the lease taker as part of
    // the initialize step runs and the shards(leases) are assigned owners
    private static final int POLLER_INITIAL_DELAY_MINS = 1;
    private static final int AWAIT_TERMINATION_SECS = 5;
    private static final int PERIODIC_SHARD_SYNC_LEADER_POLLER_FREQUENCY_IN_MINS = 5;

    ScheduledLeaseLeaderPoller(LeaderElectionStrategy<KinesisClientLease> leaderElectionStrategy,
                               KinesisClientLibConfiguration config, ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
        this.leaderElectionStrategy = leaderElectionStrategy;
        this.config = config;
        this.scheduledExecutor = scheduledThreadPoolExecutor;
    }

    @Override
    public void pollForLeaders() {
        scheduledExecutor.scheduleWithFixedDelay(leaderElectionStrategy, POLLER_INITIAL_DELAY_MINS,
                                                 PERIODIC_SHARD_SYNC_LEADER_POLLER_FREQUENCY_IN_MINS, TimeUnit.MINUTES);
    }

    @Override
    public LeaderElectionStrategy<KinesisClientLease> getLeaderElectionStrategy() {
        return leaderElectionStrategy;
    }

    @Override
    public synchronized void stop() {
        try {
            scheduledExecutor.shutdown();
            if (scheduledExecutor.awaitTermination(AWAIT_TERMINATION_SECS, TimeUnit.SECONDS)) {
                LOG.info("Successfully stopped leader polling threads on the worker");
            } else {
                scheduledExecutor.shutdownNow();
                LOG.info(String.format("Stopped leader polling threads after awaiting termination for %d seconds",
                                       AWAIT_TERMINATION_SECS));
            }
        } catch (InterruptedException e) {
            LOG.debug("Encountered InterruptedException while awaiting leader polling threadpool termination");
        }
    }
}
