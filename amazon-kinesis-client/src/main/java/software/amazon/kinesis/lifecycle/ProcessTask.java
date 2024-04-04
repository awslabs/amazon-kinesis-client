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
import software.amazon.kinesis.schemaregistry.SchemaRegistryDecoder;

/**
 * Task for fetching data records and invoking processRecords() on the record processor instance.
 */
@Slf4j
@KinesisClientInternalApi
public class ProcessTask implements ConsumerTask {
    private static final String PROCESS_TASK_OPERATION = "ProcessTask";
    private static final String APPLICATION_TRACKER_OPERATION = "ApplicationTracker";
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
    private final String shardInfoId;
    private final SchemaRegistryDecoder schemaRegistryDecoder;

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
                       @NonNull MetricsFactory metricsFactory,
                       SchemaRegistryDecoder schemaRegistryDecoder) {
        this.shardInfo = shardInfo;
        this.shardInfoId = ShardInfo.getLeaseKey(shardInfo);
        this.shardRecordProcessor = shardRecordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.backoffTimeMillis = backoffTimeMillis;
        this.throttlingReporter = throttlingReporter;
        this.processRecordsInput = processRecordsInput;
        this.shouldCallProcessRecordsEvenForEmptyRecordList = shouldCallProcessRecordsEvenForEmptyRecordList;
        this.idleTimeInMilliseconds = idleTimeInMilliseconds;
        this.metricsFactory = metricsFactory;
        this.schemaRegistryDecoder = schemaRegistryDecoder;

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
        /*
         * NOTE: the difference between appScope and shardScope is, appScope doesn't have shardId as a dimension,
         * therefore all data added to appScope, although from different shard consumer, will be sent to the same metric,
         * which is the app-level MillsBehindLatest metric.
         */
        final MetricsScope appScope = MetricsUtil.createMetricsWithOperation(metricsFactory, APPLICATION_TRACKER_OPERATION);
        final MetricsScope shardScope = MetricsUtil.createMetricsWithOperation(metricsFactory, PROCESS_TASK_OPERATION);
        MetricsUtil.addStreamId(shardScope, shardInfo.streamConfig().streamIdentifier());
        MetricsUtil.addShardId(shardScope, shardInfo.shardId());
        long startTimeMillis = System.currentTimeMillis();
        boolean success = false;
        try {
            shardScope.addData(RECORDS_PROCESSED_METRIC, 0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            shardScope.addData(DATA_BYTES_PROCESSED_METRIC, 0, StandardUnit.BYTES, MetricsLevel.SUMMARY);
            Exception exception = null;

            try {
                if (processRecordsInput.millisBehindLatest() != null) {
                    shardScope.addData(MILLIS_BEHIND_LATEST_METRIC, processRecordsInput.millisBehindLatest(),
                            StandardUnit.MILLISECONDS, MetricsLevel.SUMMARY);
                    appScope.addData(MILLIS_BEHIND_LATEST_METRIC, processRecordsInput.millisBehindLatest(),
                            StandardUnit.MILLISECONDS, MetricsLevel.SUMMARY);
                }

                if (processRecordsInput.isAtShardEnd() && processRecordsInput.records().isEmpty()) {
                    log.info("Reached end of shard {} and have no records to process", shardInfoId);
                    return new TaskResult(null, true);
                }

                throttlingReporter.success();
                List<KinesisClientRecord> records = deaggregateAnyKplRecords(processRecordsInput.records());

                if (schemaRegistryDecoder != null) {
                    records = schemaRegistryDecoder.decode(records);
                }

                if (!records.isEmpty()) {
                    shardScope.addData(RECORDS_PROCESSED_METRIC, records.size(), StandardUnit.COUNT, MetricsLevel.SUMMARY);
                }

                recordProcessorCheckpointer.largestPermittedCheckpointValue(filterAndGetMaxExtendedSequenceNumber(
                        shardScope, records, recordProcessorCheckpointer.lastCheckpointValue(),
                        recordProcessorCheckpointer.largestPermittedCheckpointValue()));

                if (shouldCallProcessRecords(records)) {
                    callProcessRecords(processRecordsInput, records);
                }
                success = true;
            } catch (RuntimeException e) {
                log.error("ShardId {}: Caught exception: ", shardInfoId, e);
                exception = e;
                backoff();
            }

            if (processRecordsInput.isAtShardEnd()) {
                log.info("Reached end of shard {}, and processed {} records", shardInfoId, processRecordsInput.records().size());
                return new TaskResult(null, true);
            }
            return new TaskResult(exception);
        } finally {
            MetricsUtil.addSuccessAndLatency(shardScope, success, startTimeMillis, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(shardScope);
            MetricsUtil.endScope(appScope);
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
            log.debug("{}: Sleep was interrupted", shardInfoId, ie);
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
                shardInfoId);

        final ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder().records(records)
                .cacheExitTime(input.cacheExitTime()).cacheEntryTime(input.cacheEntryTime())
                .isAtShardEnd(input.isAtShardEnd()).checkpointer(recordProcessorCheckpointer)
                .millisBehindLatest(input.millisBehindLatest()).build();

        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, PROCESS_TASK_OPERATION);
        MetricsUtil.addStreamId(scope, shardInfo.streamConfig().streamIdentifier());
        MetricsUtil.addShardId(scope, shardInfo.shardId());
        final long startTime = System.currentTimeMillis();
        try {
            shardRecordProcessor.processRecords(processRecordsInput);
        } catch (Exception e) {
            log.error("ShardId {}: Application processRecords() threw an exception when processing shard ",
                    shardInfoId, e);
            log.error("ShardId {}: Skipping over the following data records: {}", shardInfoId, records);
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
                log.debug("{} : removing record with ESN {} because the ESN is <= checkpoint ({})", shardInfoId,
                        extendedSequenceNumber, lastCheckpointValue);
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
