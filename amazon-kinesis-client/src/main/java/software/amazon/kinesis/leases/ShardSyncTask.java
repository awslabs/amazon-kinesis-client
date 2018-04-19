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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.lifecycle.ITask;
import software.amazon.kinesis.lifecycle.TaskCompletedListener;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.lifecycle.TaskType;

/**
 * This task syncs leases/activies with shards of the stream.
 * It will create new leases/activites when it discovers new shards (e.g. setup/resharding).
 * It will clean up leases/activities for shards that have been completely processed (if
 * cleanupLeasesUponShardCompletion is true).
 */
@RequiredArgsConstructor
@Slf4j
public class ShardSyncTask implements ITask {
    @NonNull
    private final LeaseManagerProxy leaseManagerProxy;
    @NonNull
    private final LeaseManager<KinesisClientLease> leaseManager;
    @NonNull
    private final InitialPositionInStreamExtended initialPosition;
    private final boolean cleanupLeasesUponShardCompletion;
    private final boolean ignoreUnexpectedChildShards;
    private final long shardSyncTaskIdleTimeMillis;

    private final TaskType taskType = TaskType.SHARDSYNC;

    private TaskCompletedListener listener;

    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        try {
            Exception exception = null;

            try {
                ShardSyncer.checkAndCreateLeasesForNewShards(leaseManagerProxy, leaseManager, initialPosition,
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
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#taskType()
     */
    @Override
    public TaskType taskType() {
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
