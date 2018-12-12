/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.proxies;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
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

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * Kinesis proxy - used to make calls to Amazon Kinesis (e.g. fetch data records and list of shards).
 */
public class KinesisProxy implements IKinesisProxyExtended {

    private static final Log LOG = LogFactory.getLog(KinesisProxy.class);

    private static final EnumSet<ShardIteratorType> EXPECTED_ITERATOR_TYPES = EnumSet
            .of(ShardIteratorType.AT_SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER);
    public static final int MAX_CACHE_MISSES_BEFORE_RELOAD = 1000;
    public static final Duration CACHE_MAX_ALLOWED_AGE = Duration.of(30, ChronoUnit.SECONDS);
    public static final int CACHE_MISS_WARNING_MODULUS = 250;

    private static String defaultServiceName = "kinesis";
    private static String defaultRegionId = "us-east-1";;

    private AmazonKinesis client;
    private AWSCredentialsProvider credentialsProvider;

    private ShardIterationState shardIterationState = null;

    @Setter(AccessLevel.PACKAGE)
    private volatile Map<String, Shard> cachedShardMap = null;
    @Setter(AccessLevel.PACKAGE)
    @Getter(AccessLevel.PACKAGE)
    private volatile Instant lastCacheUpdateTime = null;
    @Setter(AccessLevel.PACKAGE)
    @Getter(AccessLevel.PACKAGE)
    private AtomicInteger cacheMisses = new AtomicInteger(0);

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
        LOG.debug("KinesisProxy has created a kinesisClient");
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
        LOG.debug("KinesisProxy( " + streamName + ")");
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
                LOG.debug("Client is DynamoDb client, will use DescribeStream.");
            }
        } catch (ClassNotFoundException e) {
            LOG.debug("Client is Kinesis Client, using ListShards instead of DescribeStream.");
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
                LOG.info("Got LimitExceededException when listing shards " + streamName + ". Backing off for "
                        + this.listShardsBackoffTimeInMillis + " millis.");
                try {
                    Thread.sleep(this.listShardsBackoffTimeInMillis);
                } catch (InterruptedException ie) {
                    LOG.debug("Stream " + streamName + " : Sleep  was interrupted ", ie);
                }
                lastException = e;
            } catch (ResourceInUseException e) {
                LOG.info("Stream is not in Active/Updating status, returning null (wait until stream is in Active or"
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
        if (this.cachedShardMap == null) {
            synchronized (this) {
                if (this.cachedShardMap == null) {
                    this.getShardList();
                }
            }
        }

        Shard shard = cachedShardMap.get(shardId);
        if (shard == null) {
            if (cacheMisses.incrementAndGet() > MAX_CACHE_MISSES_BEFORE_RELOAD || cacheNeedsTimeUpdate()) {
                synchronized (this) {
                    shard = cachedShardMap.get(shardId);

                    //
                    // If after synchronizing we resolve the shard, it means someone else already got it for us.
                    //
                    if (shard == null) {
                        LOG.info("To many shard map cache misses or cache is out of date -- forcing a refresh");
                        this.getShardList();
                        shard = verifyAndLogShardAfterCacheUpdate(shardId);
                        cacheMisses.set(0);
                    } else {
                        //
                        // If someone else got us the shard go ahead and zero cache misses
                        //
                        cacheMisses.set(0);
                    }

                }
            }
        }

        if (shard == null) {
            String message = "Cannot find the shard given the shardId " + shardId + ".  Cache misses: " + cacheMisses;
            if (cacheMisses.get() % CACHE_MISS_WARNING_MODULUS == 0) {
                LOG.warn(message);
            } else {
                LOG.debug(message);
            }
        }
        return shard;
    }

    private Shard verifyAndLogShardAfterCacheUpdate(String shardId) {
        Shard shard = cachedShardMap.get(shardId);
        if (shard == null) {
            LOG.warn("Even after cache refresh shard '" + shardId + "' wasn't found.  "
                    + "This could indicate a bigger problem");
        }
        return shard;
    }

    private boolean cacheNeedsTimeUpdate() {
        if (lastCacheUpdateTime == null) {
            return true;
        }
        Instant now = Instant.now();
        Duration cacheAge = Duration.between(lastCacheUpdateTime, now);

        String baseMessage = "Shard map cache is " + cacheAge + " > " + CACHE_MAX_ALLOWED_AGE + ". ";
        if (cacheAge.compareTo(CACHE_MAX_ALLOWED_AGE) > 0) {
            LOG.info(baseMessage + "Age exceeds limit -- Refreshing.");
            return true;
        }
        LOG.debug(baseMessage + "Age doesn't exceed limit.");
        return false;
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
        List<Shard> shards = shardIterationState.getShards();
        this.cachedShardMap = shards.stream().collect(Collectors.toMap(Shard::getShardId, Function.identity()));
        this.lastCacheUpdateTime = Instant.now();

        shardIterationState = new ShardIterationState();
        return shards;
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
