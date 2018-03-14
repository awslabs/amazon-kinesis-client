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
package software.amazon.kinesis.retrieval;

import java.util.Collections;
import java.util.Date;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import org.apache.commons.lang.StringUtils;

import software.amazon.kinesis.checkpoint.SentinelCheckpoint;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.IKinesisProxy;
import software.amazon.kinesis.retrieval.MetricsCollectingKinesisProxyDecorator;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.util.CollectionUtils;
import com.google.common.collect.Iterables;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Used to get data from Amazon Kinesis. Tracks iterator state internally.
 */
@Slf4j
class KinesisDataFetcher {
    private String nextIterator;
    private IKinesisProxy kinesisProxy;
    private final String shardId;
    private boolean isShardEndReached;
    private boolean isInitialized;
    private String lastKnownSequenceNumber;
    private InitialPositionInStreamExtended initialPositionInStream;

    /**
     *
     * @param kinesisProxy Kinesis proxy
     * @param shardInfo The shardInfo object.
     */
    public KinesisDataFetcher(IKinesisProxy kinesisProxy, ShardInfo shardInfo) {
        this.shardId = shardInfo.getShardId();
        this.kinesisProxy = new MetricsCollectingKinesisProxyDecorator("KinesisDataFetcher", kinesisProxy, this.shardId);
    }

    /**
     * Get records from the current position in the stream (up to maxRecords).
     *
     * @param maxRecords Max records to fetch
     * @return list of records of up to maxRecords size
     */
    public DataFetcherResult getRecords(int maxRecords) {
        if (!isInitialized) {
            throw new IllegalArgumentException("KinesisDataFetcher.getRecords called before initialization.");
        }
        
        if (nextIterator != null) {
            try {
                return new AdvancingResult(kinesisProxy.get(nextIterator, maxRecords));
            } catch (ResourceNotFoundException e) {
                log.info("Caught ResourceNotFoundException when fetching records for shard {}", shardId);
                return TERMINAL_RESULT;
            }
        } else {
            return TERMINAL_RESULT;
        }
    }

    final DataFetcherResult TERMINAL_RESULT = new DataFetcherResult() {
        @Override
        public GetRecordsResult getResult() {
            return new GetRecordsResult().withMillisBehindLatest(null).withRecords(Collections.emptyList())
                    .withNextShardIterator(null);
        }

        @Override
        public GetRecordsResult accept() {
            isShardEndReached = true;
            return getResult();
        }

        @Override
        public boolean isShardEnd() {
            return isShardEndReached;
        }
    };

    @Data
    class AdvancingResult implements DataFetcherResult {

        final GetRecordsResult result;

        @Override
        public GetRecordsResult getResult() {
            return result;
        }

        @Override
        public GetRecordsResult accept() {
            nextIterator = result.getNextShardIterator();
            if (!CollectionUtils.isNullOrEmpty(result.getRecords())) {
                lastKnownSequenceNumber = Iterables.getLast(result.getRecords()).getSequenceNumber();
            }
            if (nextIterator == null) {
                isShardEndReached = true;
            }
            return getResult();
        }

        @Override
        public boolean isShardEnd() {
            return isShardEndReached;
        }
    }

    /**
     * Initializes this KinesisDataFetcher's iterator based on the checkpointed sequence number.
     * @param initialCheckpoint Current checkpoint sequence number for this shard.
     * @param initialPositionInStream The initialPositionInStream.
     */
    public void initialize(String initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream) {
        log.info("Initializing shard {} with {}", shardId, initialCheckpoint);
        advanceIteratorTo(initialCheckpoint, initialPositionInStream);
        isInitialized = true;
    }

    public void initialize(ExtendedSequenceNumber initialCheckpoint,
            InitialPositionInStreamExtended initialPositionInStream) {
        log.info("Initializing shard {} with {}", shardId, initialCheckpoint.getSequenceNumber());
        advanceIteratorTo(initialCheckpoint.getSequenceNumber(), initialPositionInStream);
        isInitialized = true;
    }

    /**
     * Advances this KinesisDataFetcher's internal iterator to be at the passed-in sequence number.
     *
     * @param sequenceNumber advance the iterator to the record at this sequence number.
     * @param initialPositionInStream The initialPositionInStream.
     */
    void advanceIteratorTo(String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream) {
        if (sequenceNumber == null) {
            throw new IllegalArgumentException("SequenceNumber should not be null: shardId " + shardId);
        } else if (sequenceNumber.equals(SentinelCheckpoint.LATEST.toString())) {
            nextIterator = getIterator(ShardIteratorType.LATEST.toString());
        } else if (sequenceNumber.equals(SentinelCheckpoint.TRIM_HORIZON.toString())) {
            nextIterator = getIterator(ShardIteratorType.TRIM_HORIZON.toString());
        } else if (sequenceNumber.equals(SentinelCheckpoint.AT_TIMESTAMP.toString())) {
            nextIterator = getIterator(initialPositionInStream.getTimestamp());
        } else if (sequenceNumber.equals(SentinelCheckpoint.SHARD_END.toString())) {
            nextIterator = null;
        } else {
            nextIterator = getIterator(ShardIteratorType.AT_SEQUENCE_NUMBER.toString(), sequenceNumber);
        }
        if (nextIterator == null) {
            isShardEndReached = true;
        }
        this.lastKnownSequenceNumber = sequenceNumber;
        this.initialPositionInStream = initialPositionInStream;
    }

    /**
     * @param iteratorType The iteratorType - either AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER.
     * @param sequenceNumber The sequenceNumber.
     *
     * @return iterator or null if we catch a ResourceNotFound exception
     */
    private String getIterator(String iteratorType, String sequenceNumber) {
        String iterator = null;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Calling getIterator for {}, iterator type {} and sequence number {}", shardId, iteratorType,
                        sequenceNumber);
            }
            iterator = kinesisProxy.getIterator(shardId, iteratorType, sequenceNumber);
        } catch (ResourceNotFoundException e) {
            log.info("Caught ResourceNotFoundException when getting an iterator for shard {}", shardId, e);
        }
        return iterator;
    }

    /**
     * @param iteratorType The iteratorType - either TRIM_HORIZON or LATEST.
     * @return iterator or null if we catch a ResourceNotFound exception
     */
    private String getIterator(String iteratorType) {
        String iterator = null;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Calling getIterator for {} and iterator type {}", shardId, iteratorType);
            }
            iterator = kinesisProxy.getIterator(shardId, iteratorType);
        } catch (ResourceNotFoundException e) {
            log.info("Caught ResourceNotFoundException when getting an iterator for shard {}", shardId, e);
        }
        return iterator;
    }

    /**
     * @param timestamp The timestamp.
     * @return iterator or null if we catch a ResourceNotFound exception
     */
    private String getIterator(Date timestamp) {
        String iterator = null;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Calling getIterator for {} and timestamp {}", shardId, timestamp);
            }
            iterator = kinesisProxy.getIterator(shardId, timestamp);
        } catch (ResourceNotFoundException e) {
            log.info("Caught ResourceNotFoundException when getting an iterator for shard {}", shardId, e);
        }
        return iterator;
    }

    /**
     * Gets a new iterator from the last known sequence number i.e. the sequence number of the last record from the last
     * getRecords call.
     */
    public void restartIterator() {
        if (StringUtils.isEmpty(lastKnownSequenceNumber) || initialPositionInStream == null) {
            throw new IllegalStateException("Make sure to initialize the KinesisDataFetcher before restarting the iterator.");
        }
        advanceIteratorTo(lastKnownSequenceNumber, initialPositionInStream);
    }

    /**
     * @return the shardEndReached
     */
    protected boolean isShardEndReached() {
        return isShardEndReached;
    }

    /** Note: This method has package level access for testing purposes.
     * @return nextIterator
     */
    String getNextIterator() {
        return nextIterator;
    }

}
