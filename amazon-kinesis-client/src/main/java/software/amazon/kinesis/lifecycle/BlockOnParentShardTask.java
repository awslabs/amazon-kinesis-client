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
package software.amazon.kinesis.lifecycle;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.BlockedOnParentShardException;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.leases.LeaseManager;
import software.amazon.kinesis.leases.KinesisClientLease;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Task to block until processing of all data records in the parent shard(s) is completed.
 * We check if we have checkpoint(s) for the parent shard(s).
 * If a checkpoint for a parent shard is found, we poll and wait until the checkpoint value is SHARD_END
 *    (application has checkpointed after processing all records in the shard).
 * If we don't find a checkpoint for the parent shard(s), we assume they have been trimmed and directly
 * proceed with processing data from the shard.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@Slf4j
// TODO: Check for non null values
public class BlockOnParentShardTask implements ITask {
    @NonNull
    private final ShardInfo shardInfo;
    private final LeaseManager<KinesisClientLease> leaseManager;
    // Sleep for this duration if the parent shards have not completed processing, or we encounter an exception.
    private final long parentShardPollIntervalMillis;

    private final TaskType taskType = TaskType.BLOCK_ON_PARENT_SHARDS;

    private TaskCompletedListener listener;

    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        try {
            Exception exception = null;

            try {
                boolean blockedOnParentShard = false;
                for (String shardId : shardInfo.parentShardIds()) {
                    KinesisClientLease lease = leaseManager.getLease(shardId);
                    if (lease != null) {
                        ExtendedSequenceNumber checkpoint = lease.getCheckpoint();
                        if ((checkpoint == null) || (!checkpoint.equals(ExtendedSequenceNumber.SHARD_END))) {
                            log.debug("Shard {} is not yet done. Its current checkpoint is {}", shardId, checkpoint);
                            blockedOnParentShard = true;
                            exception = new BlockedOnParentShardException("Parent shard not yet done");
                            break;
                        } else {
                            log.debug("Shard {} has been completely processed.", shardId);
                        }
                    } else {
                        log.info("No lease found for shard {}. Not blocking on completion of this shard.", shardId);
                    }
                }

                if (!blockedOnParentShard) {
                    log.info("No need to block on parents {} of shard {}", shardInfo.parentShardIds(),
                            shardInfo.shardId());
                    return new TaskResult(null);
                }
            } catch (Exception e) {
                log.error("Caught exception when checking for parent shard checkpoint", e);
                exception = e;
            }
            try {
                Thread.sleep(parentShardPollIntervalMillis);
            } catch (InterruptedException e) {
                log.error("Sleep interrupted when waiting on parent shard(s) of {}", shardInfo.shardId(), e);
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
