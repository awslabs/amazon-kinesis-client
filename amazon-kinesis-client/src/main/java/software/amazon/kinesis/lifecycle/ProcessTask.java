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

import java.util.List;
import java.util.ListIterator;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.ThrottlingReporter;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Task for fetching data records and invoking processRecords() on the record processor instance.
 */
@Slf4j
@KinesisClientInternalApi
public class ProcessTask implements ConsumerTask {
    private static final String PROCESS_TASK_OPERATION = "ProcessTask";
    private static final String DATA_BYTES_PROCESSED_METRIC = "DataBytesProcessed";
    private static final String RECORDS_PROCESSED_METRIC = "RecordsProcessed";
    private static final String RECORD_PROCESSOR_PROCESS_RECORDS_METRIC = "RecordProcessor.processRecords";
    private static final String MILLIS_BEHIND_LATEST_METRIC = "MillisBehindLatest";

    private final ShardInfo shardInfo;
    private final ShardRecordProcessor shardRecordProcessor;
    private final ShardRecordProcessorCheckpointer recordProcessorCheckpointer;
    private final TaskType taskType = TaskType.PROCESS;
    private final long backoffTimeMillis;
    private final Shard shard;
    private final ThrottlingReporter throttlingReporter;
    private final boolean shouldCallProcessRecordsEvenForEmptyRecordList;
    private final long idleTimeInMilliseconds;
    private final ProcessRecordsInput processRecordsInput;
    private final MetricsFactory metricsFactory;
    private final AggregatorUtil aggregatorUtil;

    public ProcessTask(@NonNull ShardInfo shardInfo,
                       @NonNull ShardRecordProcessor shardRecordProcessor,
                       @NonNull ShardRecordProcessorCheckpointer recordProcessorCheckpointer,
                       long backoffTimeMillis,
                       boolean skipShardSyncAtWorkerInitializationIfLeasesExist,
                       ShardDetector shardDetector,
                       @NonNull ThrottlingReporter throttlingReporter,
                       ProcessRecordsInput processRecordsInput,
                       boolean shouldCallProcessRecordsEvenForEmptyRecordList,
                       long idleTimeInMilliseconds,
                       @NonNull AggregatorUtil aggregatorUtil,
                       @NonNull MetricsFactory metricsFactory) {
        this.shardInfo = shardInfo;
        this.shardRecordProcessor = shardRecordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.backoffTimeMillis = backoffTimeMillis;
        this.throttlingReporter = throttlingReporter;
        this.processRecordsInput = processRecordsInput;
        this.shouldCallProcessRecordsEvenForEmptyRecordList = shouldCallProcessRecordsEvenForEmptyRecordList;
        this.idleTimeInMilliseconds = idleTimeInMilliseconds;
        this.metricsFactory = metricsFactory;

        if (!skipShardSyncAtWorkerInitializationIfLeasesExist) {
            this.shard = shardDetector.shard(shardInfo.shardId());
        } else {
            this.shard = null;
        }

        if (this.shard == null && !skipShardSyncAtWorkerInitializationIfLeasesExist) {
            log.warn("Cannot get the shard for this ProcessTask, so duplicate KPL user records "
                    + "in the event of resharding will not be dropped during deaggregation of Amazon "
                    + "Kinesis records.");
        }
        this.aggregatorUtil = aggregatorUtil;

        this.recordProcessorCheckpointer.checkpointer().operation(PROCESS_TASK_OPERATION);
    }

    /*
     * (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ConsumerTask#call()
     */
    @Override
    public TaskResult call() {
        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, PROCESS_TASK_OPERATION);
        MetricsUtil.addShardId(scope, shardInfo.shardId());
        long startTimeMillis = System.currentTimeMillis();
        boolean success = false;
        try {
            scope.addData(RECORDS_PROCESSED_METRIC, 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            scope.addData(DATA_BYTES_PROCESSED_METRIC, 0, StandardUnit.BYTES, MetricsLevel.SUMMARY);
            Exception exception = null;

            try {
                if (processRecordsInput.millisBehindLatest() != null) {
                    scope.addData(MILLIS_BEHIND_LATEST_METRIC, processRecordsInput.millisBehindLatest(),
                            StandardUnit.MILLISECONDS, MetricsLevel.SUMMARY);
                }

                if (processRecordsInput.isAtShardEnd() && processRecordsInput.records().isEmpty()) {
                    log.info("Reached end of shard {} and have no records to process", shardInfo.shardId());
                    return new TaskResult(null, true);
                }

                throttlingReporter.success();
                List<KinesisClientRecord> records = deaggregateAnyKplRecords(processRecordsInput.records());


                if (!records.isEmpty()) {
                    scope.addData(RECORDS_PROCESSED_METRIC, records.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);
                }

                recordProcessorCheckpointer.largestPermittedCheckpointValue(filterAndGetMaxExtendedSequenceNumber(
                        scope, records, recordProcessorCheckpointer.lastCheckpointValue(),
                        recordProcessorCheckpointer.largestPermittedCheckpointValue()));

                if (shouldCallProcessRecords(records)) {
                    callProcessRecords(processRecordsInput, records);
                }
                success = true;
            } catch (RuntimeException e) {
                log.error("ShardId {}: Caught exception: ", shardInfo.shardId(), e);
                exception = e;
                backoff();
            }

            if (processRecordsInput.isAtShardEnd()) {
                log.info("Reached end of shard {}, and processed {} records", shardInfo.shardId(), processRecordsInput.records().size());
                return new TaskResult(null, true);
            }
            return new TaskResult(exception);
        } finally {
            MetricsUtil.addSuccessAndLatency(scope, success, startTimeMillis, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(scope);
        }
    }

    private List<KinesisClientRecord> deaggregateAnyKplRecords(List<KinesisClientRecord> records) {
        if (shard == null) {
            return aggregatorUtil.deaggregate(records);
        } else {
            return aggregatorUtil.deaggregate(records, shard.hashKeyRange().startingHashKey(), shard.hashKeyRange().endingHashKey());
        }
    }

    /**
     * Sleeps for the configured backoff period. This is usually only called when an exception occurs.
     */
    private void backoff() {
        // backoff if we encounter an exception.
        try {
            Thread.sleep(this.backoffTimeMillis);
        } catch (InterruptedException ie) {
            log.debug("{}: Sleep was interrupted", shardInfo.shardId(), ie);
        }
    }

    /**
     * Dispatches a batch of records to the record processor, and handles any fallout from that.
     *
     * @param input
     *            the result of the last call to Kinesis
     * @param records
     *            the records to be dispatched. It's possible the records have been adjusted by KPL deaggregation.
     */
    private void callProcessRecords(ProcessRecordsInput input, List<KinesisClientRecord> records) {
        log.debug("Calling application processRecords() with {} records from {}", records.size(),
                shardInfo.shardId());

        final ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder().records(records).cacheExitTime(input.cacheExitTime()).cacheEntryTime(input.cacheEntryTime())
                .checkpointer(recordProcessorCheckpointer).millisBehindLatest(input.millisBehindLatest()).build();

        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, PROCESS_TASK_OPERATION);
        MetricsUtil.addShardId(scope, shardInfo.shardId());
        final long startTime = System.currentTimeMillis();
        try {
            shardRecordProcessor.processRecords(processRecordsInput);
        } catch (Exception e) {
            log.error("ShardId {}: Application processRecords() threw an exception when processing shard ",
                    shardInfo.shardId(), e);
            log.error("ShardId {}: Skipping over the following data records: {}", shardInfo.shardId(), records);
        } finally {
            MetricsUtil.addLatency(scope, RECORD_PROCESSOR_PROCESS_RECORDS_METRIC, startTime, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(scope);
        }
    }

    /**
     * Whether we should call process records or not
     *
     * @param records
     *            the records returned from the call to Kinesis, and/or deaggregation
     * @return true if the set of records should be dispatched to the record process, false if they should not.
     */
    private boolean shouldCallProcessRecords(List<KinesisClientRecord> records) {
        return (!records.isEmpty()) || shouldCallProcessRecordsEvenForEmptyRecordList;
    }

    /**
     * Emits metrics, and sleeps if there are no records available
     *
     * @param startTimeMillis
     *            the time when the task started
     */
    private void handleNoRecords(long startTimeMillis) {
        log.debug("Kinesis didn't return any records for shard {}", shardInfo.shardId());

        long sleepTimeMillis = idleTimeInMilliseconds - (System.currentTimeMillis() - startTimeMillis);
        if (sleepTimeMillis > 0) {
            sleepTimeMillis = Math.max(sleepTimeMillis, idleTimeInMilliseconds);
            try {
                log.debug("Sleeping for {} ms since there were no new records in shard {}", sleepTimeMillis,
                        shardInfo.shardId());
                Thread.sleep(sleepTimeMillis);
            } catch (InterruptedException e) {
                log.debug("ShardId {}: Sleep was interrupted", shardInfo.shardId());
            }
        }
    }

    @Override
    public TaskType taskType() {
        return taskType;
    }

    /**
     * Scans a list of records to filter out records up to and including the most recent checkpoint value and to get the
     * greatest extended sequence number from the retained records. Also emits metrics about the records.
     *
     * @param scope
     *            metrics scope to emit metrics into
     * @param records
     *            list of records to scan and change in-place as needed
     * @param lastCheckpointValue
     *            the most recent checkpoint value
     * @param lastLargestPermittedCheckpointValue
     *            previous largest permitted checkpoint value
     * @return the largest extended sequence number among the retained records
     */
    private ExtendedSequenceNumber filterAndGetMaxExtendedSequenceNumber(final MetricsScope scope,
                                                                         final List<KinesisClientRecord> records,
                                                                         final ExtendedSequenceNumber lastCheckpointValue,
                                                                         final ExtendedSequenceNumber lastLargestPermittedCheckpointValue) {
        ExtendedSequenceNumber largestExtendedSequenceNumber = lastLargestPermittedCheckpointValue;
        ListIterator<KinesisClientRecord> recordIterator = records.listIterator();
        while (recordIterator.hasNext()) {
            KinesisClientRecord record = recordIterator.next();
            ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber(record.sequenceNumber(),
                    record.subSequenceNumber());

            if (extendedSequenceNumber.compareTo(lastCheckpointValue) <= 0) {
                recordIterator.remove();
                log.debug("removing record with ESN {} because the ESN is <= checkpoint ({})", extendedSequenceNumber,
                        lastCheckpointValue);
                continue;
            }

            if (largestExtendedSequenceNumber == null
                    || largestExtendedSequenceNumber.compareTo(extendedSequenceNumber) < 0) {
                largestExtendedSequenceNumber = extendedSequenceNumber;
            }

            scope.addData(DATA_BYTES_PROCESSED_METRIC, record.data().limit(), StandardUnit.BYTES,
                    MetricsLevel.SUMMARY);
        }
        return largestExtendedSequenceNumber;
    }

}
