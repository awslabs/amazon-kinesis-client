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

import com.amazonaws.services.kinesis.model.Shard;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

import java.util.List;

/**
 * This task syncs leases/activies with shards of the stream.
 * It will create new leases/activites when it discovers new shards (e.g. setup/resharding).
 * It will clean up leases/activities for shards that have been completely processed (if
 * cleanupLeasesUponShardCompletion is true).
 */
public class ShardSyncTask implements ITask {

    private static final Log LOG = LogFactory.getLog(ShardSyncTask.class);

    private final IKinesisProxy kinesisProxy;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private InitialPositionInStreamExtended initialPosition;
    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean ignoreUnexpectedChildShards;
    private final long shardSyncTaskIdleTimeMillis;
    private final TaskType taskType = TaskType.SHARDSYNC;
    private final ShardSyncer shardSyncer;
    private final List<Shard> latestShards;

    /**
     * @param kinesisProxy Used to fetch information about the stream (e.g. shard list)
     * @param leaseManager Used to fetch and create leases
     * @param initialPositionInStream One of LATEST, TRIM_HORIZON or AT_TIMESTAMP. Amazon Kinesis Client Library will
     *        start processing records from this point in the stream (when an application starts up for the first time)
     *        except for shards that already have a checkpoint (and their descendant shards).
     * @param cleanupLeasesUponShardCompletion Clean up shards we've finished processing (don't wait for expiration
     *        in Kinesis)
     * @param shardSyncTaskIdleTimeMillis shardSync task idle time in millis
     * @param shardSyncer shardSyncer instance used to check and create new leases
     * @param latestShards latest snapshot of shards to reuse
     */
    public ShardSyncTask(IKinesisProxy kinesisProxy,
                         ILeaseManager<KinesisClientLease> leaseManager,
                         InitialPositionInStreamExtended initialPositionInStream,
                         boolean cleanupLeasesUponShardCompletion,
                         boolean ignoreUnexpectedChildShards,
                         long shardSyncTaskIdleTimeMillis,
                         ShardSyncer shardSyncer, List<Shard> latestShards) {
        this.latestShards = latestShards;
        this.kinesisProxy = kinesisProxy;
        this.leaseManager = leaseManager;
        this.initialPosition = initialPositionInStream;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.shardSyncTaskIdleTimeMillis = shardSyncTaskIdleTimeMillis;
        this.shardSyncer = shardSyncer;
    }

    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        Exception exception = null;

        try {
            shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy,
                    leaseManager,
                    initialPosition,
                    cleanupLeasesUponShardCompletion,
                    ignoreUnexpectedChildShards,
                    latestShards);
            if (shardSyncTaskIdleTimeMillis > 0) {
                Thread.sleep(shardSyncTaskIdleTimeMillis);
            }
        } catch (Exception e) {
            LOG.error("Caught exception while sync'ing Kinesis shards and leases", e);
            exception = e;
        }

        return new TaskResult(exception);
    }


    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#getTaskType()
     */
    @Override
    public TaskType getTaskType() {
        return taskType;
    }

}
