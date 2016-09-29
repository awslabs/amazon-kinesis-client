/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxyExtended;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;

/**
 * Task for fetching data records and invoking processRecords() on the record processor instance.
 */
class ProcessTask implements ITask {

    private static final Log LOG = LogFactory.getLog(ProcessTask.class);

    private static final String EXPIRED_ITERATOR_METRIC = "ExpiredIterator";
    private static final String DATA_BYTES_PROCESSED_METRIC = "DataBytesProcessed";
    private static final String RECORDS_PROCESSED_METRIC = "RecordsProcessed";
    private static final String MILLIS_BEHIND_LATEST_METRIC = "MillisBehindLatest";
    private static final String RECORD_PROCESSOR_PROCESS_RECORDS_METRIC = "RecordProcessor.processRecords";

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final KinesisDataFetcher dataFetcher;
    private final TaskType taskType = TaskType.PROCESS;
    private final StreamConfig streamConfig;
    private final long backoffTimeMillis;
    private final Shard shard;

    /**
     * @param shardInfo contains information about the shard
     * @param streamConfig Stream configuration
     * @param recordProcessor Record processor used to process the data records for the shard
     * @param recordProcessorCheckpointer Passed to the RecordProcessor so it can checkpoint
     *        progress
     * @param dataFetcher Kinesis data fetcher (used to fetch records from Kinesis)
     * @param backoffTimeMillis backoff time when catching exceptions
     */
    public ProcessTask(ShardInfo shardInfo,
            StreamConfig streamConfig,
            IRecordProcessor recordProcessor,
            RecordProcessorCheckpointer recordProcessorCheckpointer,
            KinesisDataFetcher dataFetcher,
            long backoffTimeMillis) {
        super();
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.dataFetcher = dataFetcher;
        this.streamConfig = streamConfig;
        this.backoffTimeMillis = backoffTimeMillis;
        IKinesisProxy kinesisProxy = this.streamConfig.getStreamProxy();
        if (kinesisProxy instanceof IKinesisProxyExtended) {
            this.shard = ((IKinesisProxyExtended) kinesisProxy).getShard(this.shardInfo.getShardId());
        } else {
            LOG.warn("Cannot get the shard for this ProcessTask, so duplicate KPL user records "
                    + "in the event of resharding will not be dropped during deaggregation of Amazon "
                    + "Kinesis records.");
            this.shard = null;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    
    // CHECKSTYLE:OFF CyclomaticComplexity
    @SuppressWarnings("unchecked")
    @Override
    public TaskResult call() {
        long startTimeMillis = System.currentTimeMillis();
        IMetricsScope scope = MetricsHelper.getMetricsScope();
        scope.addDimension(MetricsHelper.SHARD_ID_DIMENSION_NAME, shardInfo.getShardId());
        scope.addData(RECORDS_PROCESSED_METRIC, 0, StandardUnit.Count, MetricsLevel.SUMMARY);
        scope.addData(DATA_BYTES_PROCESSED_METRIC, 0, StandardUnit.Bytes, MetricsLevel.SUMMARY);

        Exception exception = null;

        try {
            if (dataFetcher.isShardEndReached()) {
                LOG.info("Reached end of shard " + shardInfo.getShardId());
                boolean shardEndReached = true;
                return new TaskResult(null, shardEndReached);
            }

            final GetRecordsResult getRecordsResult = getRecordsResult();
            List<Record> records = getRecordsResult.getRecords();
            
            if (!records.isEmpty()) {
                scope.addData(RECORDS_PROCESSED_METRIC, records.size(), StandardUnit.Count, MetricsLevel.SUMMARY);
            } else {
                LOG.debug("Kinesis didn't return any records for shard " + shardInfo.getShardId());

                long sleepTimeMillis =
                        streamConfig.getIdleTimeInMilliseconds() - (System.currentTimeMillis() - startTimeMillis);
                if (sleepTimeMillis > 0) {
                    sleepTimeMillis = Math.max(sleepTimeMillis, streamConfig.getIdleTimeInMilliseconds());
                    try {
                        LOG.debug("Sleeping for " + sleepTimeMillis + " ms since there were no new records in shard "
                                + shardInfo.getShardId());
                        Thread.sleep(sleepTimeMillis);
                    } catch (InterruptedException e) {
                        LOG.debug("ShardId " + shardInfo.getShardId() + ": Sleep was interrupted");
                    }
                }
            }
            
            // We deaggregate if and only if we got actual Kinesis records, i.e.
            // not instances of some subclass thereof.
            if (!records.isEmpty() && records.get(0).getClass().equals(Record.class)) {
                if (this.shard != null) {
                    records = (List<Record>) (List<?>) UserRecord.deaggregate(records, 
                              new BigInteger(this.shard.getHashKeyRange().getStartingHashKey()),
                              new BigInteger(this.shard.getHashKeyRange().getEndingHashKey()));
                } else {
                    records = (List<Record>) (List<?>) UserRecord.deaggregate(records);
                }
            }
            
            recordProcessorCheckpointer.setLargestPermittedCheckpointValue(
                    filterAndGetMaxExtendedSequenceNumber(scope, records,
                            recordProcessorCheckpointer.getLastCheckpointValue(),
                            recordProcessorCheckpointer.getLargestPermittedCheckpointValue()));

            if ((!records.isEmpty()) || streamConfig.shouldCallProcessRecordsEvenForEmptyRecordList()) {
                LOG.debug("Calling application processRecords() with " + records.size()
                        + " records from " + shardInfo.getShardId());
                final ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                        .withRecords(records)
                        .withCheckpointer(recordProcessorCheckpointer)
                        .withMillisBehindLatest(getRecordsResult.getMillisBehindLatest());

                final long recordProcessorStartTimeMillis = System.currentTimeMillis();
                try {
                    recordProcessor.processRecords(processRecordsInput);
                } catch (Exception e) {
                    LOG.error("ShardId " + shardInfo.getShardId()
                            + ": Application processRecords() threw an exception when processing shard ", e);
                    LOG.error("ShardId " + shardInfo.getShardId() + ": Skipping over the following data records: "
                            + records);
                } finally {
                    MetricsHelper.addLatencyPerShard(shardInfo.getShardId(), RECORD_PROCESSOR_PROCESS_RECORDS_METRIC,
                            recordProcessorStartTimeMillis, MetricsLevel.SUMMARY);
                }
            }
        } catch (RuntimeException e) {
            LOG.error("ShardId " + shardInfo.getShardId() + ": Caught exception: ", e);
            exception = e;

            // backoff if we encounter an exception.
            try {
                Thread.sleep(this.backoffTimeMillis);
            } catch (InterruptedException ie) {
                LOG.debug(shardInfo.getShardId() + ": Sleep was interrupted", ie);
            }
        }

        return new TaskResult(exception);
    }
    // CHECKSTYLE:ON CyclomaticComplexity

    @Override
    public TaskType getTaskType() {
        return taskType;
    }

    /**
     * Scans a list of records to filter out records up to and including the most recent checkpoint value and to get
     * the greatest extended sequence number from the retained records. Also emits metrics about the records.
     *
     * @param scope metrics scope to emit metrics into
     * @param records list of records to scan and change in-place as needed
     * @param lastCheckpointValue the most recent checkpoint value
     * @param lastLargestPermittedCheckpointValue previous largest permitted checkpoint value
     * @return the largest extended sequence number among the retained records
     */
    private ExtendedSequenceNumber filterAndGetMaxExtendedSequenceNumber(IMetricsScope scope, List<Record> records,
            final ExtendedSequenceNumber lastCheckpointValue,
            final ExtendedSequenceNumber lastLargestPermittedCheckpointValue) {
        ExtendedSequenceNumber largestExtendedSequenceNumber = lastLargestPermittedCheckpointValue;
        ListIterator<Record> recordIterator = records.listIterator();
        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();
            ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber(
                    record.getSequenceNumber(),
                    record instanceof UserRecord
                            ? ((UserRecord) record).getSubSequenceNumber()
                            : null);

            if (extendedSequenceNumber.compareTo(lastCheckpointValue) <= 0) {
                recordIterator.remove();
                LOG.debug("removing record with ESN " + extendedSequenceNumber
                        + " because the ESN is <= checkpoint (" + lastCheckpointValue + ")");
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

    /**
     * Gets records from Kinesis and retries once in the event of an ExpiredIteratorException.
     *
     * @return list of data records from Kinesis
     */
    private GetRecordsResult getRecordsResult() {
        try {
            return getRecordsResultAndRecordMillisBehindLatest();
        } catch (ExpiredIteratorException e) {
            // If we see a ExpiredIteratorException, try once to restart from the greatest remembered sequence number
            LOG.info("ShardId " + shardInfo.getShardId()
                    + ": getRecords threw ExpiredIteratorException - restarting after greatest seqNum "
                    + "passed to customer", e);
            MetricsHelper.getMetricsScope().addData(EXPIRED_ITERATOR_METRIC, 1, StandardUnit.Count,
                    MetricsLevel.SUMMARY);

            /*
             * Advance the iterator to after the greatest processed sequence number (remembered by
             * recordProcessorCheckpointer).
             */
            dataFetcher.advanceIteratorTo(recordProcessorCheckpointer.getLargestPermittedCheckpointValue()
                    .getSequenceNumber(), streamConfig.getInitialPositionInStream());

            // Try a second time - if we fail this time, expose the failure.
            try {
                return getRecordsResultAndRecordMillisBehindLatest();
            } catch (ExpiredIteratorException ex) {
                String msg =
                        "Shard " + shardInfo.getShardId()
                                + ": getRecords threw ExpiredIteratorException with a fresh iterator.";
                LOG.error(msg, ex);
                throw ex;
            }
        }
    }

    /**
     * Gets records from Kinesis and records the MillisBehindLatest metric if present.
     *
     * @return list of data records from Kinesis
     */
    private GetRecordsResult getRecordsResultAndRecordMillisBehindLatest() {
        final GetRecordsResult getRecordsResult = dataFetcher.getRecords(streamConfig.getMaxRecords());

        if (getRecordsResult == null) {
            // Stream no longer exists
            return new GetRecordsResult().withRecords(Collections.<Record>emptyList());
        }

        if (getRecordsResult.getMillisBehindLatest() != null) {
            MetricsHelper.getMetricsScope().addData(MILLIS_BEHIND_LATEST_METRIC,
                    getRecordsResult.getMillisBehindLatest(),
                    StandardUnit.Milliseconds,
                    MetricsLevel.SUMMARY);
        }

        return getRecordsResult;
    }

}
