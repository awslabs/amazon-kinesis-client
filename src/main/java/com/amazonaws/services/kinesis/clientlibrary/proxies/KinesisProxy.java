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
package com.amazonaws.services.kinesis.clientlibrary.proxies;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
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
import com.amazonaws.services.kinesis.model.StreamStatus;

/**
 * Kinesis proxy - used to make calls to Amazon Kinesis (e.g. fetch data records and list of shards).
 */
public class KinesisProxy implements IKinesisProxy {

    private static final Log LOG = LogFactory.getLog(KinesisProxy.class);

    private static String defaultServiceName = "kinesis";
    private static String defaultRegionId = "us-east-1";;

    private AmazonKinesisClient client;
    private AWSCredentialsProvider credentialsProvider;

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
        this(streamName, credentialProvider, new AmazonKinesisClient(credentialProvider),
                describeStreamBackoffTimeInMillis, maxDescribeStreamRetryAttempts);
        client.setEndpoint(endpoint, serviceName, regionId);

        LOG.debug("KinesisProxy has created a kinesisClient");
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
            AmazonKinesisClient kinesisClient,
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
        int remainingRetryTimes = this.maxDescribeStreamRetryAttempts;
        // Call DescribeStream, with backoff and retries (if we get LimitExceededException).
        while ((remainingRetryTimes >= 0) && (response == null)) {
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
            }
            remainingRetryTimes--;
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
    public List<Shard> getShardList() {
        List<Shard> result = new ArrayList<Shard>();

        DescribeStreamResult response = null;
        String lastShardId = null;

        do {
            response = getStreamInfo(lastShardId);

            if (response == null) {
                /*
                 * If getStreamInfo ever returns null, we should bail and return null. This indicates the stream is not
                 * in ACTIVE or UPDATING state and we may not have accurate/consistent information about the stream.
                 */
                return null;
            } else {
                List<Shard> shards = response.getStreamDescription().getShards();
                result.addAll(shards);
                lastShardId = shards.get(shards.size() - 1).getShardId();
            }
        } while (response.getStreamDescription().isHasMoreShards());

        return result;
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
        final GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setRequestCredentials(credentialsProvider.getCredentials());
        getShardIteratorRequest.setStreamName(streamName);
        getShardIteratorRequest.setShardId(shardId);
        getShardIteratorRequest.setShardIteratorType(iteratorType);
        getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber);
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

}
