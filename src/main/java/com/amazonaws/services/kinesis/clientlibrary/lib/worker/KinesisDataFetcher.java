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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.MetricsCollectingKinesisProxyDecorator;
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
     * @param shardId shardId (we'll fetch data for this shard)
     * @param checkpoint used to get current checkpoint from which to start fetching records
     */
    public KinesisDataFetcher(IKinesisProxy kinesisProxy, ShardInfo shardInfo) {
        this.shardId = shardInfo.getShardId();
        this.kinesisProxy =
                new MetricsCollectingKinesisProxyDecorator("KinesisDataFetcher", kinesisProxy, this.shardId);
    }

    /**
     * Get records from the current position in the stream (up to maxRecords).
     * 
     * @param maxRecords Max records to fetch
     * @return list of records of up to maxRecords size
     */
    public List<Record> getRecords(int maxRecords) {
        if (!isInitialized) {
            throw new IllegalArgumentException("KinesisDataFetcher.getRecords called before initialization.");
        }

        List<Record> records = null;
        GetRecordsResult response = null;
        if (nextIterator != null) {
            try {
                response = kinesisProxy.get(nextIterator, maxRecords);
                records = response.getRecords();
                nextIterator = response.getNextShardIterator();
            } catch (ResourceNotFoundException e) {
                LOG.info("Caught ResourceNotFoundException when fetching records for shard " + shardId);
                nextIterator = null;
            }
            if (nextIterator == null) {
                isShardEndReached = true;
            }
        } else {
            isShardEndReached = true;
        }

        return records;
    }

    /**
     * Initializes this KinesisDataFetcher's iterator based on the checkpoint.
     * @param initialCheckpoint Current checkpoint for this shard.
     * 
     */
    public void initialize(String initialCheckpoint) {

        LOG.info("Initializing shard " + shardId + " with " + initialCheckpoint);
        advanceIteratorAfter(initialCheckpoint);
        isInitialized = true;
    }
    
    /**
     * Advances this KinesisDataFetcher's internal iterator to be after the passed-in sequence number.
     * 
     * @param sequenceNumber advance the iterator to the first record after this sequence number.
     */
    private void advanceIteratorAfterSequenceNumber(String sequenceNumber) {
        nextIterator = getIterator(ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), sequenceNumber);
    }
    
    /**
     * Advances this KinesisDataFetcher's internal iterator to be after the passed-in sequence number.
     * 
     * @param sequenceNumber advance the iterator to the first record after this sequence number.
     */
    void advanceIteratorAfter(String sequenceNumber) {
        if (sequenceNumber == null) {
            throw new IllegalArgumentException("SequenceNumber should not be null: shardId " + shardId);
        } else if (sequenceNumber.equals(SentinelCheckpoint.LATEST.toString())) {
            nextIterator = getIterator(ShardIteratorType.LATEST.toString(), null);
        } else if (sequenceNumber.equals(SentinelCheckpoint.TRIM_HORIZON.toString())) {
            nextIterator = getIterator(ShardIteratorType.TRIM_HORIZON.toString(), null);
        } else if (sequenceNumber.equals(SentinelCheckpoint.SHARD_END.toString())) {
            nextIterator = null;
        } else {
            advanceIteratorAfterSequenceNumber(sequenceNumber);
        }
        if (nextIterator == null) {
            isShardEndReached = true;
        }
    }
    
    /**
     * @param iteratorType
     * @param sequenceNumber
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
