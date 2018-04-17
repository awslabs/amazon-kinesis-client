/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
 * http://aws.amazon.com/asl/ or in the "license" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package software.amazon.kinesis.lifecycle;

import java.math.BigInteger;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.coordinator.RecordProcessorCheckpointer;
import software.amazon.kinesis.leases.LeaseManagerProxy;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.IMetricsScope;
import software.amazon.kinesis.metrics.MetricsHelper;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.processor.IRecordProcessor;
import software.amazon.kinesis.retrieval.GetRecordsCache;
import software.amazon.kinesis.retrieval.ThrottlingReporter;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.retrieval.kpl.UserRecord;

/**
 * Task for fetching data records and invoking processRecords() on the record processor instance.
 */
@Slf4j
public class ProcessTask implements ITask {
    private static final String DATA_BYTES_PROCESSED_METRIC = "DataBytesProcessed";
    private static final String RECORDS_PROCESSED_METRIC = "RecordsProcessed";
    private static final String MILLIS_BEHIND_LATEST_METRIC = "MillisBehindLatest";
    private static final String RECORD_PROCESSOR_PROCESS_RECORDS_METRIC = "RecordProcessor.processRecords";

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final TaskType taskType = TaskType.PROCESS;
    private final long backoffTimeMillis;
    private final Shard shard;
    private final ThrottlingReporter throttlingReporter;
    private final boolean shouldCallProcessRecordsEvenForEmptyRecordList;
    private final long idleTimeInMilliseconds;

    private final ProcessRecordsInput processRecordsInput;
    private TaskCompletedListener listener;

    @RequiredArgsConstructor
    public static class RecordsFetcher {

        private final GetRecordsCache getRecordsCache;

        public ProcessRecordsInput getRecords() {
            ProcessRecordsInput processRecordsInput = getRecordsCache.getNextResult();

            if (processRecordsInput.getMillisBehindLatest() != null) {
                MetricsHelper.getMetricsScope().addData(MILLIS_BEHIND_LATEST_METRIC,
                        processRecordsInput.getMillisBehindLatest(), StandardUnit.Milliseconds, MetricsLevel.SUMMARY);
            }

            return processRecordsInput;
        }

    }

    public ProcessTask(@NonNull final ShardInfo shardInfo,
                       @NonNull final IRecordProcessor recordProcessor,
                       @NonNull final RecordProcessorCheckpointer recordProcessorCheckpointer,
                       final long backoffTimeMillis,
                       final boolean skipShardSyncAtWorkerInitializationIfLeasesExist,
                       final LeaseManagerProxy leaseManagerProxy,
                       @NonNull final ThrottlingReporter throttlingReporter,
                       final ProcessRecordsInput processRecordsInput,
                       final boolean shouldCallProcessRecordsEvenForEmptyRecordList,
                       final long idleTimeInMilliseconds) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.backoffTimeMillis = backoffTimeMillis;
        this.throttlingReporter = throttlingReporter;
        this.processRecordsInput = processRecordsInput;
        this.shouldCallProcessRecordsEvenForEmptyRecordList = shouldCallProcessRecordsEvenForEmptyRecordList;
        this.idleTimeInMilliseconds = idleTimeInMilliseconds;

        Optional<Shard> currentShard = Optional.empty();
        if (!skipShardSyncAtWorkerInitializationIfLeasesExist) {
            currentShard = leaseManagerProxy.listShards().stream()
                    .filter(shard -> shardInfo.shardId().equals(shard.getShardId()))
                    .findFirst();
        }
        this.shard = currentShard.orElse(null);

        if (this.shard == null && !skipShardSyncAtWorkerInitializationIfLeasesExist) {
            log.warn("Cannot get the shard for this ProcessTask, so duplicate KPL user records "
                    + "in the event of resharding will not be dropped during deaggregation of Amazon "
                    + "Kinesis records.");
        }
    }

    /*
     * (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        try {
            long startTimeMillis = System.currentTimeMillis();
            IMetricsScope scope = MetricsHelper.getMetricsScope();
            scope.addDimension(MetricsHelper.SHARD_ID_DIMENSION_NAME, shardInfo.shardId());
            scope.addData(RECORDS_PROCESSED_METRIC, 0, StandardUnit.Count, MetricsLevel.SUMMARY);
            scope.addData(DATA_BYTES_PROCESSED_METRIC, 0, StandardUnit.Bytes, MetricsLevel.SUMMARY);
            Exception exception = null;

            try {
                if (processRecordsInput.isAtShardEnd()) {
                    log.info("Reached end of shard {}", shardInfo.shardId());
                    return new TaskResult(null, true);
                }

                throttlingReporter.success();
                List<Record> records = processRecordsInput.getRecords();

                if (!records.isEmpty()) {
                    scope.addData(RECORDS_PROCESSED_METRIC, records.size(), StandardUnit.Count, MetricsLevel.SUMMARY);
                } else {
                    handleNoRecords(startTimeMillis);
                }
                records = deaggregateRecords(records);

                recordProcessorCheckpointer.largestPermittedCheckpointValue(filterAndGetMaxExtendedSequenceNumber(
                        scope, records, recordProcessorCheckpointer.lastCheckpointValue(),
                        recordProcessorCheckpointer.largestPermittedCheckpointValue()));

                if (shouldCallProcessRecords(records)) {
                    callProcessRecords(processRecordsInput, records);
                }
            } catch (RuntimeException e) {
                log.error("ShardId {}: Caught exception: ", shardInfo.shardId(), e);
                exception = e;
                backoff();
            }

            return new TaskResult(exception);
        } finally {
            if (listener != null) {
                listener.taskCompleted(this);
            }
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
    private void callProcessRecords(ProcessRecordsInput input, List<Record> records) {
        log.debug("Calling application processRecords() with {} records from {}", records.size(),
                shardInfo.shardId());
        final ProcessRecordsInput processRecordsInput = new ProcessRecordsInput().withRecords(records)
                .withCheckpointer(recordProcessorCheckpointer).withMillisBehindLatest(input.getMillisBehindLatest());

        final long recordProcessorStartTimeMillis = System.currentTimeMillis();
        try {
            recordProcessor.processRecords(processRecordsInput);
        } catch (Exception e) {
            log.error("ShardId {}: Application processRecords() threw an exception when processing shard ",
                    shardInfo.shardId(), e);
            log.error("ShardId {}: Skipping over the following data records: {}", shardInfo.shardId(), records);
        } finally {
            MetricsHelper.addLatencyPerShard(shardInfo.shardId(), RECORD_PROCESSOR_PROCESS_RECORDS_METRIC,
                    recordProcessorStartTimeMillis, MetricsLevel.SUMMARY);
        }
    }

    /**
     * Whether we should call process records or not
     *
     * @param records
     *            the records returned from the call to Kinesis, and/or deaggregation
     * @return true if the set of records should be dispatched to the record process, false if they should not.
     */
    private boolean shouldCallProcessRecords(List<Record> records) {
        return (!records.isEmpty()) || shouldCallProcessRecordsEvenForEmptyRecordList;
    }

    /**
     * Determines whether to deaggregate the given records, and if they are KPL records dispatches them to deaggregation
     *
     * @param records
     *            the records to deaggregate is deaggregation is required.
     * @return returns either the deaggregated records, or the original records
     */
    @SuppressWarnings("unchecked")
    private List<Record> deaggregateRecords(List<Record> records) {
        // We deaggregate if and only if we got actual Kinesis records, i.e.
        // not instances of some subclass thereof.
        if (!records.isEmpty() && records.get(0).getClass().equals(Record.class)) {
            if (this.shard != null) {
                return (List<Record>) (List<?>) UserRecord.deaggregate(records,
                        new BigInteger(this.shard.getHashKeyRange().getStartingHashKey()),
                        new BigInteger(this.shard.getHashKeyRange().getEndingHashKey()));
            } else {
                return (List<Record>) (List<?>) UserRecord.deaggregate(records);
            }
        }
        return records;
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

    @Override
    public void addTaskCompletedListener(TaskCompletedListener taskCompletedListener) {
        if (listener != null) {
            log.warn("Listener is being reset, this shouldn't happen");
        }
        listener = taskCompletedListener;
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
    private ExtendedSequenceNumber filterAndGetMaxExtendedSequenceNumber(IMetricsScope scope, List<Record> records,
            final ExtendedSequenceNumber lastCheckpointValue,
            final ExtendedSequenceNumber lastLargestPermittedCheckpointValue) {
        ExtendedSequenceNumber largestExtendedSequenceNumber = lastLargestPermittedCheckpointValue;
        ListIterator<Record> recordIterator = records.listIterator();
        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();
            ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber(record.getSequenceNumber(),
                    record instanceof UserRecord ? ((UserRecord) record).getSubSequenceNumber() : null);

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

            scope.addData(DATA_BYTES_PROCESSED_METRIC, record.getData().limit(), StandardUnit.Bytes,
                    MetricsLevel.SUMMARY);
        }
        return largestExtendedSequenceNumber;
    }

}