/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.leases;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.lifecycle.ITask;
import software.amazon.kinesis.lifecycle.TaskCompletedListener;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.lifecycle.TaskType;
import software.amazon.kinesis.retrieval.IKinesisProxy;

/**
 * This task syncs leases/activies with shards of the stream.
 * It will create new leases/activites when it discovers new shards (e.g. setup/resharding).
 * It will clean up leases/activities for shards that have been completely processed (if
 * cleanupLeasesUponShardCompletion is true).
 */
@Slf4j
public class ShardSyncTask implements ITask {
    private final IKinesisProxy kinesisProxy;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private InitialPositionInStreamExtended initialPosition;
    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean ignoreUnexpectedChildShards;
    private final long shardSyncTaskIdleTimeMillis;
    private final TaskType taskType = TaskType.SHARDSYNC;

    private TaskCompletedListener listener;

    /**
     * @param kinesisProxy Used to fetch information about the stream (e.g. shard list)
     * @param leaseManager Used to fetch and create leases
     * @param initialPositionInStream One of LATEST, TRIM_HORIZON or AT_TIMESTAMP. Amazon Kinesis Client Library will
     *        start processing records from this point in the stream (when an application starts up for the first time)
     *        except for shards that already have a checkpoint (and their descendant shards).
     */
    public ShardSyncTask(IKinesisProxy kinesisProxy,
                         ILeaseManager<KinesisClientLease> leaseManager,
                         InitialPositionInStreamExtended initialPositionInStream,
                         boolean cleanupLeasesUponShardCompletion,
                         boolean ignoreUnexpectedChildShards,
                         long shardSyncTaskIdleTimeMillis) {
        this.kinesisProxy = kinesisProxy;
        this.leaseManager = leaseManager;
        this.initialPosition = initialPositionInStream;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.shardSyncTaskIdleTimeMillis = shardSyncTaskIdleTimeMillis;
    }

    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        try {
            Exception exception = null;

            try {
                ShardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, initialPosition,
                        cleanupLeasesUponShardCompletion, ignoreUnexpectedChildShards);
                if (shardSyncTaskIdleTimeMillis > 0) {
                    Thread.sleep(shardSyncTaskIdleTimeMillis);
                }
            } catch (Exception e) {
                log.error("Caught exception while sync'ing Kinesis shards and leases", e);
                exception = e;
            }

            return new TaskResult(exception);
        } finally {
            if (listener != null) {
                listener.taskCompleted(this);
            }
        }
    }


    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#getTaskType()
     */
    @Override
    public TaskType getTaskType() {
        return taskType;
    }

    @Override
    public void addTaskCompletedListener(TaskCompletedListener taskCompletedListener) {
        if (listener != null) {
            log.warn("Listener is being reset, this shouldn't happen");
        }
        listener = taskCompletedListener;
    }

}
