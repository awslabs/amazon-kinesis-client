/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.lifecycle;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.exceptions.internal.BlockedOnParentShardException;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Task to block until processing of all data records in the parent shard(s) is completed.
 * We check if we have checkpoint(s) for the parent shard(s).
 * If a checkpoint for a parent shard is found, we poll and wait until the checkpoint value is SHARD_END
 * (application has checkpointed after processing all records in the shard).
 * If we don't find a checkpoint for the parent shard(s), we assume they have been trimmed and directly
 * proceed with processing data from the shard.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@Slf4j
@KinesisClientInternalApi
// TODO: Check for non null values
public class BlockOnParentShardTask implements ConsumerTask {
    @NonNull
    private final ShardInfo shardInfo;
    private final LeaseRefresher leaseRefresher;
    // Sleep for this duration if the parent shards have not completed processing, or we encounter an exception.
    private final long parentShardPollIntervalMillis;

    private final TaskType taskType = TaskType.BLOCK_ON_PARENT_SHARDS;

    /*
     * (non-Javadoc)
     * 
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ConsumerTask#call()
     */
    @Override
    public TaskResult call() {
        Exception exception = null;

        try {
            boolean blockedOnParentShard = false;
            for (String shardId : shardInfo.parentShardIds()) {
                Lease lease = leaseRefresher.getLease(shardId);
                if (lease != null) {
                    ExtendedSequenceNumber checkpoint = lease.checkpoint();
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
                log.info("No need to block on parents {} of shard {}", shardInfo.parentShardIds(), shardInfo.shardId());
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
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ConsumerTask#taskType()
     */
    @Override
    public TaskType taskType() {
        return taskType;
    }

}
