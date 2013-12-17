/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

/**
 * This task syncs leases/activies with shards of the stream.
 * It will create new leases/activites when it discovers new shards (e.g. setup/resharding).
 * It will clean up leases/activities for shards that have been completely processed (if
 * cleanupLeasesUponShardCompletion is true).
 */
class ShardSyncTask implements ITask {

    private static final Log LOG = LogFactory.getLog(ShardSyncTask.class);

    private final IKinesisProxy kinesisProxy;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private InitialPositionInStream initialPosition;
    private final boolean cleanupLeasesUponShardCompletion;
    private final long shardSyncTaskIdleTimeMillis;
    private final TaskType taskType = TaskType.SHARDSYNC;

    /**
     * @param kinesisProxy Used to fetch information about the stream (e.g. shard list)
     * @param leaseManager Used to fetch and create leases
     * @param initialPosition One of LATEST or TRIM_HORIZON. Amazon Kinesis Client Library will start processing records
     *        from this point in the stream (when an application starts up for the first time) except for shards that
     *        already have a checkpoint (and their descendant shards).
     */
    ShardSyncTask(IKinesisProxy kinesisProxy,
            ILeaseManager<KinesisClientLease> leaseManager,
            InitialPositionInStream initialPositionInStream,
            boolean cleanupLeasesUponShardCompletion,
            long shardSyncTaskIdleTimeMillis) {
        this.kinesisProxy = kinesisProxy;
        this.leaseManager = leaseManager;
        this.initialPosition = initialPositionInStream;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.shardSyncTaskIdleTimeMillis = shardSyncTaskIdleTimeMillis;
    }

    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        Exception exception = null;

        try {
            ShardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy,
                    leaseManager,
                    initialPosition,
                    cleanupLeasesUponShardCompletion);
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
