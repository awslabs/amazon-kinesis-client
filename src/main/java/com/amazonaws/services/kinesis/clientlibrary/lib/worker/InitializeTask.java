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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;

/**
 * Task for initializing shard position and invoking the RecordProcessor initialize() API.
 */
class InitializeTask implements ITask {

    private static final Log LOG = LogFactory.getLog(InitializeTask.class);
    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final KinesisDataFetcher dataFetcher;
    private final TaskType taskType = TaskType.INITIALIZE;
    private final ICheckpoint checkpoint;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    // Back off for this interval if we encounter a problem (exception)
    private final long backoffTimeMillis;

    /**
     * Constructor.
     */
    InitializeTask(ShardInfo shardInfo,
            IRecordProcessor recordProcessor,
            ICheckpoint checkpoint,
            RecordProcessorCheckpointer recordProcessorCheckpointer,
            KinesisDataFetcher dataFetcher,
            long backoffTimeMillis) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.checkpoint = checkpoint;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.dataFetcher = dataFetcher;
        this.backoffTimeMillis = backoffTimeMillis;
    }

    /* Initializes the data fetcher (position in shard) and invokes the RecordProcessor initialize() API.
     * (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        boolean applicationException = false;
        Exception exception = null;

        try {
            LOG.debug("Initializing ShardId " + shardInfo.getShardId());
            String initialCheckpoint = checkpoint.getCheckpoint(shardInfo.getShardId());
            dataFetcher.initialize(initialCheckpoint);
            recordProcessorCheckpointer.setSequenceNumber(initialCheckpoint);
            try {
                LOG.debug("Calling the record processor initialize().");
                recordProcessor.initialize(shardInfo.getShardId());
                LOG.debug("Record processor initialize() completed.");
            } catch (Exception e) {
                applicationException = true;
                throw e;
            }

            return new TaskResult(null);
        } catch (Exception e) {
            if (applicationException) {
                LOG.error("Application initialize() threw exception: ", e);
            } else {
                LOG.error("Caught exception: ", e);
            }
            exception = e;
            // backoff if we encounter an exception.
            try {
                Thread.sleep(this.backoffTimeMillis);
            } catch (InterruptedException ie) {
                LOG.debug("Interrupted sleep", ie);
            }
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
