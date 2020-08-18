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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.kinesis.model.ChildShard;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.MetricsCollectingKinesisProxyDecorator;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.util.CollectionUtils;
import com.google.common.collect.Iterables;

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
    private String lastKnownSequenceNumber;
    private InitialPositionInStreamExtended initialPositionInStream;
    private List<ChildShard> childShards = Collections.emptyList();

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
            LOG.info("Skipping fetching records from Kinesis for shard " + shardId + ": nextIterator is null.");
            return TERMINAL_RESULT;
        }
    }

    final DataFetcherResult TERMINAL_RESULT = new DataFetcherResult() {
        @Override
        public GetRecordsResult getResult() {
            return new GetRecordsResult()
                    .withMillisBehindLatest(null)
                    .withRecords(Collections.emptyList())
                    .withNextShardIterator(null)
                    .withChildShards(Collections.emptyList());
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
            if (!isValidResult(result)) {
                // Throwing SDK exception when the GetRecords result is not valid. This will allow PrefetchGetRecordsCache to retry the GetRecords call.
                throw new SdkClientException("Shard " + shardId +": GetRecordsResult is not valid. NextShardIterator: " + result.getNextShardIterator()
                                                     + ". ChildShards: " + result.getChildShards());
            }
            nextIterator = result.getNextShardIterator();
            if (!CollectionUtils.isNullOrEmpty(result.getRecords())) {
                lastKnownSequenceNumber = Iterables.getLast(result.getRecords()).getSequenceNumber();
            }
            if (nextIterator == null) {
                LOG.info("Reached shard end: nextIterator is null in AdvancingResult.accept for shard " + shardId + ". childShards: " + result.getChildShards());
                if (!CollectionUtils.isNullOrEmpty(result.getChildShards())) {
                    childShards = result.getChildShards();
                }
                isShardEndReached = true;
            }
            return getResult();
        }

        @Override
        public boolean isShardEnd() {
            return isShardEndReached;
        }
    }

    private boolean isValidResult(GetRecordsResult getRecordsResult) {
        // GetRecords result should contain childShard information. There are two valid combination for the nextShardIterator and childShards
        // If the GetRecords call does not reach the shard end, getRecords result should contain a non-null nextShardIterator and an empty list of childShards.
        // If the GetRecords call reaches the shard end, getRecords result should contain a null nextShardIterator and a non-empty list of childShards.
        // All other combinations are invalid and indicating an issue with GetRecords result from Kinesis service.
        if (getRecordsResult.getNextShardIterator() == null && CollectionUtils.isNullOrEmpty(getRecordsResult.getChildShards()) ||
                getRecordsResult.getNextShardIterator() != null && !CollectionUtils.isNullOrEmpty(getRecordsResult.getChildShards())) {
            return false;
        }
        for (ChildShard childShard : getRecordsResult.getChildShards()) {
            if (CollectionUtils.isNullOrEmpty(childShard.getParentShards())) {
                return false;
            }
        }
        return true;
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

    public void initialize(ExtendedSequenceNumber initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream) {
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
            LOG.info("Reached shard end: cannot advance iterator for shard " + shardId);
            isShardEndReached = true;
            // TODO: transition to ShuttingDown state on shardend instead to shutdown state for enqueueing this for cleanup
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

    protected List<ChildShard> getChildShards() {
        return childShards;
    }

    /** Note: This method has package level access for testing purposes.
     * @return nextIterator
     */
    String getNextIterator() {
        return nextIterator;
    }

}
