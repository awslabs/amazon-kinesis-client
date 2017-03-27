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
package com.amazonaws.services.kinesis.clientlibrary.proxies;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamStatus;

/**
 * Kinesis proxy - used to make calls to Amazon Kinesis (e.g. fetch data records and list of shards).
 */
public class KinesisProxy implements IKinesisProxyExtended {

    private static final Log LOG = LogFactory.getLog(KinesisProxy.class);

    private static final EnumSet<ShardIteratorType> EXPECTED_ITERATOR_TYPES = EnumSet
            .of(ShardIteratorType.AT_SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER);

    private static String defaultServiceName = "kinesis";
    private static String defaultRegionId = "us-east-1";;

    private AmazonKinesis client;
    private AWSCredentialsProvider credentialsProvider;
    private AtomicReference<List<Shard>> listOfShardsSinceLastGet = new AtomicReference<>();
    private ShardIterationState shardIterationState = null;

    private final String streamName;

    private static final long DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS = 1000L;
    private static final int DEFAULT_DESCRIBE_STREAM_RETRY_TIMES = 50;
    private final long describeStreamBackoffTimeInMillis;
    private final int maxDescribeStreamRetryAttempts;

    /**
     * Public constructor.
     * 
     * @param streamName Data records will be fetched from this stream
     * @param credentialProvider Provides credentials for signing Kinesis requests
     * @param endpoint Kinesis endpoint
     */

    public KinesisProxy(final String streamName, AWSCredentialsProvider credentialProvider, String endpoint) {
        this(streamName, credentialProvider, endpoint, defaultServiceName, defaultRegionId,
                DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS, DEFAULT_DESCRIBE_STREAM_RETRY_TIMES);
    }

    /**
     * Public constructor.
     * 
     * @param streamName Data records will be fetched from this stream
     * @param credentialProvider Provides credentials for signing Kinesis requests
     * @param endpoint Kinesis endpoint
     * @param serviceName service name
     * @param regionId region id
     * @param describeStreamBackoffTimeInMillis Backoff time for DescribeStream calls in milliseconds
     * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls
     */
    public KinesisProxy(final String streamName,
            AWSCredentialsProvider credentialProvider,
            String endpoint,
            String serviceName,
            String regionId,
            long describeStreamBackoffTimeInMillis,
            int maxDescribeStreamRetryAttempts) {
        this(streamName, credentialProvider, buildClientSettingEndpoint(credentialProvider,
                endpoint,
                serviceName,
                regionId), describeStreamBackoffTimeInMillis, maxDescribeStreamRetryAttempts);
        

        LOG.debug("KinesisProxy has created a kinesisClient");
    }
    
    private static AmazonKinesisClient buildClientSettingEndpoint(AWSCredentialsProvider credentialProvider,
            String endpoint,
            String serviceName,
            String regionId) {
        AmazonKinesisClient client = new AmazonKinesisClient(credentialProvider);
        client.setEndpoint(endpoint);
        client.setSignerRegionOverride(regionId);
        return client;
    }

    /**
     * Public constructor.
     * 
     * @param streamName Data records will be fetched from this stream
     * @param credentialProvider Provides credentials for signing Kinesis requests
     * @param kinesisClient Kinesis client (used to fetch data from Kinesis)
     * @param describeStreamBackoffTimeInMillis Backoff time for DescribeStream calls in milliseconds
     * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls
     */
    public KinesisProxy(final String streamName,
            AWSCredentialsProvider credentialProvider,
            AmazonKinesis kinesisClient,
            long describeStreamBackoffTimeInMillis,
            int maxDescribeStreamRetryAttempts) {
        this.streamName = streamName;
        this.credentialsProvider = credentialProvider;
        this.describeStreamBackoffTimeInMillis = describeStreamBackoffTimeInMillis;
        this.maxDescribeStreamRetryAttempts = maxDescribeStreamRetryAttempts;
        this.client = kinesisClient;

        LOG.debug("KinesisProxy( " + streamName + ")");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GetRecordsResult get(String shardIterator, int maxRecords)
        throws ResourceNotFoundException, InvalidArgumentException, ExpiredIteratorException {

        final GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setRequestCredentials(credentialsProvider.getCredentials());
        getRecordsRequest.setShardIterator(shardIterator);
        getRecordsRequest.setLimit(maxRecords);
        final GetRecordsResult response = client.getRecords(getRecordsRequest);
        return response;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DescribeStreamResult getStreamInfo(String startShardId)
            throws ResourceNotFoundException, LimitExceededException {
        final DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setRequestCredentials(credentialsProvider.getCredentials());
        describeStreamRequest.setStreamName(streamName);
        describeStreamRequest.setExclusiveStartShardId(startShardId);
        DescribeStreamResult response = null;

        LimitExceededException lastException = null;

        int remainingRetryTimes = this.maxDescribeStreamRetryAttempts;
        // Call DescribeStream, with backoff and retries (if we get LimitExceededException).
        while (response == null) {
            try {
                response = client.describeStream(describeStreamRequest);
            } catch (LimitExceededException le) {
                LOG.info("Got LimitExceededException when describing stream " + streamName + ". Backing off for "
                        + this.describeStreamBackoffTimeInMillis + " millis.");
                try {
                    Thread.sleep(this.describeStreamBackoffTimeInMillis);
                } catch (InterruptedException ie) {
                    LOG.debug("Stream " + streamName + " : Sleep  was interrupted ", ie);
                }
                lastException = le;
            }
            remainingRetryTimes--;
            if (remainingRetryTimes <= 0 && response == null) {
                if (lastException != null) {
                    throw lastException;
                }
                throw new IllegalStateException("Received null from DescribeStream call.");
            }
        }

        if (StreamStatus.ACTIVE.toString().equals(response.getStreamDescription().getStreamStatus())
                || StreamStatus.UPDATING.toString().equals(response.getStreamDescription().getStreamStatus())) {
            return response;
        } else {
            LOG.info("Stream is in status " + response.getStreamDescription().getStreamStatus()
                    + ", KinesisProxy.DescribeStream returning null (wait until stream is Active or Updating");
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Shard getShard(String shardId) {
        if (this.listOfShardsSinceLastGet.get() == null) {
            //Update this.listOfShardsSinceLastGet as needed.
            this.getShardList();
        }

        for (Shard shard : listOfShardsSinceLastGet.get()) {
            if (shard.getShardId().equals(shardId))  {
                return shard;
            }
        }
        
        LOG.warn("Cannot find the shard given the shardId " + shardId);
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized List<Shard> getShardList() {

        DescribeStreamResult response;
        if (shardIterationState == null) {
            shardIterationState = new ShardIterationState();
        }

        do {
            response = getStreamInfo(shardIterationState.getLastShardId());

            if (response == null) {
                /*
                 * If getStreamInfo ever returns null, we should bail and return null. This indicates the stream is not
                 * in ACTIVE or UPDATING state and we may not have accurate/consistent information about the stream.
                 */
                return null;
            } else {
                shardIterationState.update(response.getStreamDescription().getShards());
            }
        } while (response.getStreamDescription().isHasMoreShards());
        this.listOfShardsSinceLastGet.set(shardIterationState.getShards());

        shardIterationState = new ShardIterationState();
        return listOfShardsSinceLastGet.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllShardIds() throws ResourceNotFoundException {
        List<Shard> shards = getShardList();
        if (shards == null) {
            return null;
        } else {
            Set<String> shardIds = new HashSet<String>();

            for (Shard shard : getShardList()) {
                shardIds.add(shard.getShardId());
            }

            return shardIds;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIterator(String shardId, String iteratorType, String sequenceNumber) {
        ShardIteratorType shardIteratorType;
        try {
            shardIteratorType = ShardIteratorType.fromValue(iteratorType);
        } catch (IllegalArgumentException iae) {
            LOG.error("Caught illegal argument exception while parsing iteratorType: " + iteratorType, iae);
            shardIteratorType = null;
        }

        if (!EXPECTED_ITERATOR_TYPES.contains(shardIteratorType)) {
            LOG.info("This method should only be used for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER "
                    + "ShardIteratorTypes. For methods to use with other ShardIteratorTypes, see IKinesisProxy.java");
        }
        final GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setRequestCredentials(credentialsProvider.getCredentials());
        getShardIteratorRequest.setStreamName(streamName);
        getShardIteratorRequest.setShardId(shardId);
        getShardIteratorRequest.setShardIteratorType(iteratorType);
        getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber);
        getShardIteratorRequest.setTimestamp(null);
        final GetShardIteratorResult response = client.getShardIterator(getShardIteratorRequest);
        return response.getShardIterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIterator(String shardId, String iteratorType) {
        final GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setRequestCredentials(credentialsProvider.getCredentials());
        getShardIteratorRequest.setStreamName(streamName);
        getShardIteratorRequest.setShardId(shardId);
        getShardIteratorRequest.setShardIteratorType(iteratorType);
        getShardIteratorRequest.setStartingSequenceNumber(null);
        getShardIteratorRequest.setTimestamp(null);
        final GetShardIteratorResult response = client.getShardIterator(getShardIteratorRequest);
        return response.getShardIterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIterator(String shardId, Date timestamp) {
        final GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setRequestCredentials(credentialsProvider.getCredentials());
        getShardIteratorRequest.setStreamName(streamName);
        getShardIteratorRequest.setShardId(shardId);
        getShardIteratorRequest.setShardIteratorType(ShardIteratorType.AT_TIMESTAMP);
        getShardIteratorRequest.setStartingSequenceNumber(null);
        getShardIteratorRequest.setTimestamp(timestamp);
        final GetShardIteratorResult response = client.getShardIterator(getShardIteratorRequest);
        return response.getShardIterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PutRecordResult put(String exclusiveMinimumSequenceNumber,
            String explicitHashKey,
            String partitionKey,
            ByteBuffer data) throws ResourceNotFoundException, InvalidArgumentException {
        final PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setRequestCredentials(credentialsProvider.getCredentials());
        putRecordRequest.setStreamName(streamName);
        putRecordRequest.setSequenceNumberForOrdering(exclusiveMinimumSequenceNumber);
        putRecordRequest.setExplicitHashKey(explicitHashKey);
        putRecordRequest.setPartitionKey(partitionKey);
        putRecordRequest.setData(data);

        final PutRecordResult response = client.putRecord(putRecordRequest);
        return response;
    }

    @Data
    static class ShardIterationState {

        private List<Shard> shards;
        private String lastShardId;

        public ShardIterationState() {
            shards = new ArrayList<>();
        }

        public void update(List<Shard> shards) {
            if (shards == null || shards.isEmpty()) {
                return;
            }
            this.shards.addAll(shards);
            Shard lastShard = shards.get(shards.size() - 1);
            if (lastShardId == null || lastShardId.compareTo(lastShard.getShardId()) < 0) {
                lastShardId = lastShard.getShardId();
            }
        }
    }

}
