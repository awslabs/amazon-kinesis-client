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

import java.math.BigInteger;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;

/**
 * Task for fetching data records and invoking processRecords() on the record processor instance.
 */
class ProcessTask implements ITask {

    private static final String EXPIRED_ITERATOR_METRIC = "ExpiredIterator";
    private static final String DATA_BYTES_PROCESSED_METRIC = "DataBytesProcessed";
    private static final String RECORDS_PROCESSED_METRIC = "RecordsProcessed";
    private static final Log LOG = LogFactory.getLog(ProcessTask.class);

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final KinesisDataFetcher dataFetcher;
    private final TaskType taskType = TaskType.PROCESS;
    private final StreamConfig streamConfig;
    private final long backoffTimeMillis;

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
    }

    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    @Override
    public TaskResult call() {
        long startTimeMillis = System.currentTimeMillis();
        IMetricsScope scope = MetricsHelper.getMetricsScope();
        scope.addDimension("ShardId", shardInfo.getShardId());
        scope.addData(RECORDS_PROCESSED_METRIC, 0, StandardUnit.Count);
        scope.addData(DATA_BYTES_PROCESSED_METRIC, 0, StandardUnit.Bytes);

        Exception exception = null;

        try {
            if (dataFetcher.isShardEndReached()) {
                LOG.info("Reached end of shard " + shardInfo.getShardId());
                boolean shardEndReached = true;
                return new TaskResult(null, shardEndReached);
            }
            List<Record> records = getRecords();
            
            if (records.isEmpty()) {
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

            if ((!records.isEmpty()) || streamConfig.shouldCallProcessRecordsEvenForEmptyRecordList()) {
                
                // If we got more records, record the max sequence number. Sleep if there are no records.
                if (!records.isEmpty()) {
                    String maxSequenceNumber = getMaxSequenceNumber(scope, records);
                    recordProcessorCheckpointer.setSequenceNumber(maxSequenceNumber);                
                }                
                try {
                    LOG.debug("Calling application processRecords() with " + records.size() + " records from "
                            + shardInfo.getShardId());
                    recordProcessor.processRecords(records, recordProcessorCheckpointer);
                } catch (Exception e) {
                    LOG.error("ShardId " + shardInfo.getShardId()
                            + ": Application processRecords() threw an exception when processing shard ", e);
                    LOG.error("ShardId " + shardInfo.getShardId() + ": Skipping over the following data records: "
                            + records);
                }                
            }
        } catch (RuntimeException | KinesisClientLibException e) {
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

    /**
     * Scans a list of records and returns the greatest sequence number from the records. Also emits metrics about the
     * records.
     * 
     * @param scope metrics scope to emit metrics into
     * @param records list of records to scan
     * @return greatest sequence number out of all the records.
     */
    private String getMaxSequenceNumber(IMetricsScope scope, List<Record> records) {
        scope.addData(RECORDS_PROCESSED_METRIC, records.size(), StandardUnit.Count);
        ListIterator<Record> recordIterator = records.listIterator();
        BigInteger maxSequenceNumber = BigInteger.ZERO;

        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();
            BigInteger sequenceNumber = new BigInteger(record.getSequenceNumber());
            if (maxSequenceNumber.compareTo(sequenceNumber) < 0) {
                maxSequenceNumber = sequenceNumber;
            }

            scope.addData(DATA_BYTES_PROCESSED_METRIC, record.getData().limit(), StandardUnit.Bytes);
        }

        return maxSequenceNumber.toString();
    }

    /**
     * Gets records from Kinesis and retries once in the event of an ExpiredIteratorException.
     * 
     * @return list of data records from Kinesis
     * @throws KinesisClientLibException if reading checkpoints fails in the edge case where we haven't passed any
     *         records to the client code yet
     */
    private List<Record> getRecords() throws KinesisClientLibException {
        int maxRecords = streamConfig.getMaxRecords();
        try {
            return dataFetcher.getRecords(maxRecords);
        } catch (ExpiredIteratorException e) {
            // If we see a ExpiredIteratorException, try once to restart from the greatest remembered sequence number
            LOG.info("ShardId " + shardInfo.getShardId()
                    + ": getRecords threw ExpiredIteratorException - restarting after greatest seqNum "
                    + "passed to customer", e);
            MetricsHelper.getMetricsScope().addData(EXPIRED_ITERATOR_METRIC, 1, StandardUnit.Count);

            /*
             * Advance the iterator to after the greatest processed sequence number (remembered by
             * recordProcessorCheckpointer).
             */
            dataFetcher.advanceIteratorAfter(recordProcessorCheckpointer.getSequenceNumber());

            // Try a second time - if we fail this time, expose the failure.
            try {
                return dataFetcher.getRecords(maxRecords);
            } catch (ExpiredIteratorException ex) {
                String msg =
                        "Shard " + shardInfo.getShardId()
                                + ": getRecords threw ExpiredIteratorException with a fresh iterator.";
                LOG.error(msg, ex);
                throw ex;
            }
        }
    }

    @Override
    public TaskType getTaskType() {
        return taskType;
    }

}
