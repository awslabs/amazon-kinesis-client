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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamStatus;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Kinesis proxy - used to make calls to Amazon Kinesis (e.g. fetch data records and list of shards).
 */
@Slf4j
public class KinesisProxy implements IKinesisProxyExtended {
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
    private final long listShardsBackoffTimeInMillis;
    private final int maxListShardsRetryAttempts;
    private boolean isKinesisClient = true;

    /**
     * @deprecated We expect the client to be passed to the proxy, and the proxy will not require to create it.
     * 
     * @param credentialProvider
     * @param endpoint
     * @param serviceName
     * @param regionId
     * @return
     */
    @Deprecated
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
     * @deprecated Deprecating constructor, this constructor doesn't use AWS best practices, moving forward please use
     * {@link #KinesisProxy(KinesisClientLibConfiguration, AmazonKinesis)} or
     * {@link #KinesisProxy(String, AmazonKinesis, long, int, long, int)} to create the object. Will be removed in the
     * next major/minor release.
     * 
     * @param streamName Data records will be fetched from this stream
     * @param credentialProvider Provides credentials for signing Kinesis requests
     * @param endpoint Kinesis endpoint
     */
    @Deprecated
    public KinesisProxy(final String streamName, AWSCredentialsProvider credentialProvider, String endpoint) {
        this(streamName, credentialProvider, endpoint, defaultServiceName, defaultRegionId,
                DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS, DEFAULT_DESCRIBE_STREAM_RETRY_TIMES,
                KinesisClientLibConfiguration.DEFAULT_LIST_SHARDS_BACKOFF_TIME_IN_MILLIS,
                KinesisClientLibConfiguration.DEFAULT_MAX_LIST_SHARDS_RETRY_ATTEMPTS);
    }

    /**
     * Public constructor.
     * 
     * @deprecated Deprecating constructor, this constructor doesn't use AWS best practices, moving forward please use
     * {@link #KinesisProxy(KinesisClientLibConfiguration, AmazonKinesis)} or
     * {@link #KinesisProxy(String, AmazonKinesis, long, int, long, int)} to create the object. Will be removed in the
     * next major/minor release.
     * 
     * @param streamName Data records will be fetched from this stream
     * @param credentialProvider Provides credentials for signing Kinesis requests
     * @param endpoint Kinesis endpoint
     * @param serviceName service name
     * @param regionId region id
     * @param describeStreamBackoffTimeInMillis Backoff time for DescribeStream calls in milliseconds
     * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls
     */
    @Deprecated
    public KinesisProxy(final String streamName,
            AWSCredentialsProvider credentialProvider,
            String endpoint,
            String serviceName,
            String regionId,
            long describeStreamBackoffTimeInMillis,
            int maxDescribeStreamRetryAttempts,
            long listShardsBackoffTimeInMillis,
            int maxListShardsRetryAttempts) {
        this(streamName,
                credentialProvider,
                buildClientSettingEndpoint(credentialProvider, endpoint, serviceName, regionId),
                describeStreamBackoffTimeInMillis,
                maxDescribeStreamRetryAttempts,
                listShardsBackoffTimeInMillis,
                maxListShardsRetryAttempts);
        log.debug("KinesisProxy has created a kinesisClient");
    }

    /**
     * Public constructor.
     * 
     * @deprecated Deprecating constructor, this constructor doesn't use AWS best practices, moving forward please use
     * {@link #KinesisProxy(KinesisClientLibConfiguration, AmazonKinesis)} or
     * {@link #KinesisProxy(String, AmazonKinesis, long, int, long, int)} to create the object. Will be removed in the
     * next major/minor release.
     * 
     * @param streamName Data records will be fetched from this stream
     * @param credentialProvider Provides credentials for signing Kinesis requests
     * @param kinesisClient Kinesis client (used to fetch data from Kinesis)
     * @param describeStreamBackoffTimeInMillis Backoff time for DescribeStream calls in milliseconds
     * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls
     */
    @Deprecated
    public KinesisProxy(final String streamName,
            AWSCredentialsProvider credentialProvider,
            AmazonKinesis kinesisClient,
            long describeStreamBackoffTimeInMillis,
            int maxDescribeStreamRetryAttempts,
            long listShardsBackoffTimeInMillis,
            int maxListShardsRetryAttempts) {
        this(streamName, kinesisClient, describeStreamBackoffTimeInMillis, maxDescribeStreamRetryAttempts,
                listShardsBackoffTimeInMillis, maxListShardsRetryAttempts);
        this.credentialsProvider = credentialProvider;
        log.debug("KinesisProxy( " + streamName + ")");
    }

    /**
     * Public constructor.
     * @param config
     */
    public KinesisProxy(final KinesisClientLibConfiguration config, final AmazonKinesis client) {
        this(config.getStreamName(),
                client,
                DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS,
                DEFAULT_DESCRIBE_STREAM_RETRY_TIMES,
                config.getListShardsBackoffTimeInMillis(),
                config.getMaxListShardsRetryAttempts());
        this.credentialsProvider = config.getKinesisCredentialsProvider();
    }

    public KinesisProxy(final String streamName,
            final AmazonKinesis client,
            final long describeStreamBackoffTimeInMillis,
            final int maxDescribeStreamRetryAttempts,
            final long listShardsBackoffTimeInMillis,
            final int maxListShardsRetryAttempts) {
        this.streamName = streamName;
        this.client = client;
        this.describeStreamBackoffTimeInMillis = describeStreamBackoffTimeInMillis;
        this.maxDescribeStreamRetryAttempts = maxDescribeStreamRetryAttempts;
        this.listShardsBackoffTimeInMillis = listShardsBackoffTimeInMillis;
        this.maxListShardsRetryAttempts = maxListShardsRetryAttempts;

        try {
            if (Class.forName("com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient")
                    .isAssignableFrom(client.getClass())) {
                isKinesisClient = false;
                log.debug("Client is DynamoDb client, will use DescribeStream.");
            }
        } catch (ClassNotFoundException e) {
            log.debug("Client is Kinesis Client, using ListShards instead of DescribeStream.");
        }
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
    @Deprecated
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
                log.info("Got LimitExceededException when describing stream {}. Backing off for {} millis.", streamName,
                        this.describeStreamBackoffTimeInMillis);
                try {
                    Thread.sleep(this.describeStreamBackoffTimeInMillis);
                } catch (InterruptedException ie) {
                    log.debug("Stream {} : Sleep  was interrupted ", streamName, ie);
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
            log.info("Stream is in status {}, KinesisProxy.DescribeStream returning null (wait until stream is Active "
                    + "or Updating", response.getStreamDescription().getStreamStatus());
            return null;
        }
    }
    
    private ListShardsResult listShards(final String nextToken) {
        final ListShardsRequest request = new ListShardsRequest();
        request.setRequestCredentials(credentialsProvider.getCredentials());
        if (StringUtils.isEmpty(nextToken)) {
            request.setStreamName(streamName);
        } else {
            request.setNextToken(nextToken);
        }
        ListShardsResult result = null;
        LimitExceededException lastException = null;
        int remainingRetries = this.maxListShardsRetryAttempts;
        
        while (result == null) {
            try {
                result = client.listShards(request);
            } catch (LimitExceededException e) {
                log.info("Got LimitExceededException when listing shards {}. Backing off for {} millis.", streamName,
                        this.listShardsBackoffTimeInMillis);
                try {
                    Thread.sleep(this.listShardsBackoffTimeInMillis);
                } catch (InterruptedException ie) {
                    log.debug("Stream {} : Sleep  was interrupted ", streamName, ie);
                }
                lastException = e;
            } catch (ResourceInUseException e) {
                log.info("Stream is not in Active/Updating status, returning null (wait until stream is in Active or"
                        + " Updating)");
                return null;
            }
            remainingRetries--;
            if (remainingRetries <= 0 && result == null) {
                if (lastException != null) {
                    throw lastException;
                }
                throw new IllegalStateException("Received null from ListShards call.");
            }
        }
        
        return result;
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
        
        log.warn("Cannot find the shard given the shardId {}", shardId);
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized List<Shard> getShardList() {
        if (shardIterationState == null) {
            shardIterationState = new ShardIterationState();
        }
        
        if (isKinesisClient) {
            ListShardsResult result;
            String nextToken = null;
            
            do {
                result = listShards(nextToken);
                
                if (result == null) {
                    /*
                    * If listShards ever returns null, we should bail and return null. This indicates the stream is not
                    * in ACTIVE or UPDATING state and we may not have accurate/consistent information about the stream.
                    */
                    return null;
                } else {
                    shardIterationState.update(result.getShards());
                    nextToken = result.getNextToken();
                }
            } while (StringUtils.isNotEmpty(result.getNextToken()));
            
        } else {
            DescribeStreamResult response;

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
        }
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
            log.error("Caught illegal argument exception while parsing iteratorType: {}", iteratorType, iae);
            shardIteratorType = null;
        }

        if (!EXPECTED_ITERATOR_TYPES.contains(shardIteratorType)) {
            log.info("This method should only be used for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER "
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