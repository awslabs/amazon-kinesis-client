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

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.BlockedOnParentShardException;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

/**
 * Task to block until processing of all data records in the parent shard(s) is completed.
 * We check if we have checkpoint(s) for the parent shard(s).
 * If a checkpoint for a parent shard is found, we poll and wait until the checkpoint value is SHARD_END
 *    (application has checkpointed after processing all records in the shard).
 * If we don't find a checkpoint for the parent shard(s), we assume they have been trimmed and directly
 * proceed with processing data from the shard.
 */
class BlockOnParentShardTask implements ITask {

    private static final Log LOG = LogFactory.getLog(BlockOnParentShardTask.class);
    private final ShardInfo shardInfo;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    
    private final TaskType taskType = TaskType.BLOCK_ON_PARENT_SHARDS;
    // Sleep for this duration if the parent shards have not completed processing, or we encounter an exception.
    private final long parentShardPollIntervalMillis;
    
    /**
     * @param shardInfo Information about the shard we are working on
     * @param leaseManager Used to fetch the lease and checkpoint info for parent shards
     * @param parentShardPollIntervalMillis Sleep time if the parent shard has not completed processing
     */
    BlockOnParentShardTask(ShardInfo shardInfo,
            ILeaseManager<KinesisClientLease> leaseManager,
            long parentShardPollIntervalMillis) {
        this.shardInfo = shardInfo;
        this.leaseManager = leaseManager;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
    }

    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        Exception exception = null;
        
        try {
            boolean blockedOnParentShard = false;
            for (String shardId : shardInfo.getParentShardIds()) {
                KinesisClientLease lease = leaseManager.getLease(shardId);
                if (lease != null) {
                    ExtendedSequenceNumber checkpoint = lease.getCheckpoint();
                    if ((checkpoint == null) || (!checkpoint.equals(ExtendedSequenceNumber.SHARD_END))) {
                        LOG.debug("Shard " + shardId + " is not yet done. Its current checkpoint is " + checkpoint);
                        blockedOnParentShard = true;
                        exception = new BlockedOnParentShardException("Parent shard not yet done");
                        break;
                    } else {
                        LOG.debug("Shard " + shardId + " has been completely processed.");
                    }
                } else {
                    LOG.info("No lease found for shard " + shardId + ". Not blocking on completion of this shard.");
                }
            }
            
            if (!blockedOnParentShard) {
                LOG.info("No need to block on parents " + shardInfo.getParentShardIds() + " of shard "
                        + shardInfo.getShardId());
                return new TaskResult(null);
            }
        } catch (Exception e) {
            LOG.error("Caught exception when checking for parent shard checkpoint", e);
            exception = e;
        }
        try {
            Thread.sleep(parentShardPollIntervalMillis);
        } catch (InterruptedException e) {
            LOG.error("Sleep interrupted when waiting on parent shard(s) of " + shardInfo.getShardId(), e);
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
