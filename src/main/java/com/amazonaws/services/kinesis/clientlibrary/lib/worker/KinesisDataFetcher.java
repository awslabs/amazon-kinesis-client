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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.Collections;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.MetricsCollectingKinesisProxyDecorator;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

import lombok.Data;

/**
 * Used to get data from Amazon Kinesis. Tracks iterator state internally.
 */
class KinesisDataFetcher {

    private static final Log LOG = LogFactory.getLog(KinesisDataFetcher.class);

    private String nextIterator;
    private IKinesisProxy kinesisProxy;
    private final String shardId;
    private boolean isShardEndReached;
    private boolean isInitialized;

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
                LOG.info("Caught ResourceNotFoundException when fetching records for shard " + shardId);
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
        LOG.info("Initializing shard " + shardId + " with " + initialCheckpoint);
        advanceIteratorTo(initialCheckpoint, initialPositionInStream);
        isInitialized = true;
    }

    public void initialize(ExtendedSequenceNumber initialCheckpoint,
            InitialPositionInStreamExtended initialPositionInStream) {
        LOG.info("Initializing shard " + shardId + " with " + initialCheckpoint.getSequenceNumber());
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Calling getIterator for " + shardId + ", iterator type " + iteratorType
                    + " and sequence number " + sequenceNumber);
            }
            iterator = kinesisProxy.getIterator(shardId, iteratorType, sequenceNumber);
        } catch (ResourceNotFoundException e) {
            LOG.info("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Calling getIterator for " + shardId + " and iterator type " + iteratorType);
            }
            iterator = kinesisProxy.getIterator(shardId, iteratorType);
        } catch (ResourceNotFoundException e) {
            LOG.info("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Calling getIterator for " + shardId + " and timestamp " + timestamp);
            }
            iterator = kinesisProxy.getIterator(shardId, timestamp);
        } catch (ResourceNotFoundException e) {
            LOG.info("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
        }
        return iterator;
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
