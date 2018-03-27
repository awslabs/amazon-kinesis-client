/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import software.amazon.kinesis.coordinator.RecordProcessorCheckpointer;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.coordinator.StreamConfig;
import software.amazon.kinesis.processor.ICheckpoint;
import software.amazon.kinesis.processor.IRecordProcessor;
import software.amazon.kinesis.checkpoint.Checkpoint;
import software.amazon.kinesis.retrieval.GetRecordsCache;
import software.amazon.kinesis.retrieval.KinesisDataFetcher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.metrics.MetricsHelper;
import software.amazon.kinesis.metrics.MetricsLevel;

import lombok.extern.slf4j.Slf4j;

/**
 * Task for initializing shard position and invoking the RecordProcessor initialize() API.
 */
@Slf4j
public class InitializeTask implements ITask {
    private static final String RECORD_PROCESSOR_INITIALIZE_METRIC = "RecordProcessor.initialize";

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final KinesisDataFetcher dataFetcher;
    private final TaskType taskType = TaskType.INITIALIZE;
    private final ICheckpoint checkpoint;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    // Back off for this interval if we encounter a problem (exception)
    private final long backoffTimeMillis;
    private final StreamConfig streamConfig;
    private final GetRecordsCache getRecordsCache;

    /**
     * Constructor.
     */
    InitializeTask(ShardInfo shardInfo,
                   IRecordProcessor recordProcessor,
                   ICheckpoint checkpoint,
                   RecordProcessorCheckpointer recordProcessorCheckpointer,
                   KinesisDataFetcher dataFetcher,
                   long backoffTimeMillis,
                   StreamConfig streamConfig,
                   GetRecordsCache getRecordsCache) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.checkpoint = checkpoint;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.dataFetcher = dataFetcher;
        this.backoffTimeMillis = backoffTimeMillis;
        this.streamConfig = streamConfig;
        this.getRecordsCache = getRecordsCache;
    }

    /*
     * Initializes the data fetcher (position in shard) and invokes the RecordProcessor initialize() API.
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        boolean applicationException = false;
        Exception exception = null;

        try {
            log.debug("Initializing ShardId {}", shardInfo.getShardId());
            Checkpoint initialCheckpointObject = checkpoint.getCheckpointObject(shardInfo.getShardId());
            ExtendedSequenceNumber initialCheckpoint = initialCheckpointObject.getCheckpoint();

            dataFetcher.initialize(initialCheckpoint.getSequenceNumber(), streamConfig.getInitialPositionInStream());
            getRecordsCache.start();
            recordProcessorCheckpointer.setLargestPermittedCheckpointValue(initialCheckpoint);
            recordProcessorCheckpointer.setInitialCheckpointValue(initialCheckpoint);

            log.debug("Calling the record processor initialize().");
            final InitializationInput initializationInput = new InitializationInput()
                .withShardId(shardInfo.getShardId())
                .withExtendedSequenceNumber(initialCheckpoint)
                .withPendingCheckpointSequenceNumber(initialCheckpointObject.getPendingCheckpoint());
            final long recordProcessorStartTimeMillis = System.currentTimeMillis();
            try {
                recordProcessor.initialize(initializationInput);
                log.debug("Record processor initialize() completed.");
            } catch (Exception e) {
                applicationException = true;
                throw e;
            } finally {
                MetricsHelper.addLatency(RECORD_PROCESSOR_INITIALIZE_METRIC, recordProcessorStartTimeMillis,
                        MetricsLevel.SUMMARY);
            }

            return new TaskResult(null);
        } catch (Exception e) {
            if (applicationException) {
                log.error("Application initialize() threw exception: ", e);
            } else {
                log.error("Caught exception: ", e);
            }
            exception = e;
            // backoff if we encounter an exception.
            try {
                Thread.sleep(this.backoffTimeMillis);
            } catch (InterruptedException ie) {
                log.debug("Interrupted sleep", ie);
            }
        }

        return new TaskResult(exception);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#getTaskType()
     */
    @Override
    public TaskType getTaskType() {
        return taskType;
    }

    @Override
    public void addTaskCompletedListener(TaskCompletedListener taskCompletedListener) {

    }

}
