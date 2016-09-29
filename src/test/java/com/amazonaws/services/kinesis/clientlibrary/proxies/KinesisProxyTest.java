package com.amazonaws.services.kinesis.clientlibrary.proxies;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.AmazonServiceException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;

import junit.framework.Assert;

@RunWith(MockitoJUnitRunner.class)
public class KinesisProxyTest {
    private static final String TEST_STRING = "TestString";
    private static final long BACKOFF_TIME = 10L;
    private static final int RETRY_TIMES = 50;

    @Mock
    private AmazonKinesisClient mockClient;
    @Mock
    private AWSCredentialsProvider mockCredentialsProvider;
    @Mock
    private GetShardIteratorResult shardIteratorResult;
    private KinesisProxy proxy;

    // Test shards for verifying.
    private Set<String> shardIdSet;
    private List<Shard> shards;

    @Before
    public void setUpTest() {
        // Set up kinesis proxy
        proxy = new KinesisProxy(TEST_STRING, mockCredentialsProvider, mockClient, BACKOFF_TIME, RETRY_TIMES);
        when(mockCredentialsProvider.getCredentials()).thenReturn(null);
        // Set up test shards
        shardIdSet = new HashSet<>();
        shards = new ArrayList<>();
        String[] shardIds = new String[] { "shard-1", "shard-2", "shard-3", "shard-4" };
        for (String shardId : shardIds) {
            Shard shard = new Shard();
            shard.setShardId(shardId);
            shards.add(shard);
            shardIdSet.add(shardId);
        }
    }

    @Test
    public void testGetShardListWithMoreDataAvailable() {
        // Set up mock :
        // First call describeStream returning response with first two shards in the list;
        // Second call describeStream returning response with rest shards.
        DescribeStreamResult responseWithMoreData = createGetStreamInfoResponse(shards.subList(0, 2), true);
        DescribeStreamResult responseFinal = createGetStreamInfoResponse(shards.subList(2, shards.size()), false);
        doReturn(responseWithMoreData).when(mockClient).describeStream(argThat(new IsRequestWithStartShardId(null)));
        doReturn(responseFinal).when(mockClient)
                .describeStream(argThat(new IsRequestWithStartShardId(shards.get(1).getShardId())));

        Set<String> resultShardIdSets = proxy.getAllShardIds();
        Assert.assertTrue("Result set should equal to Test set", shardIdSet.equals(resultShardIdSets));
    }

    @Test
    public void testGetShardListWithLimitExceededException() {
        // Set up mock :
        // First call describeStream throwing LimitExceededException;
        // Second call describeStream returning shards list.
        DescribeStreamResult response = createGetStreamInfoResponse(shards, false);
        doThrow(new LimitExceededException("Test Exception")).doReturn(response).when(mockClient)
                .describeStream(argThat(new IsRequestWithStartShardId(null)));

        Set<String> resultShardIdSet = proxy.getAllShardIds();
        Assert.assertTrue("Result set should equal to Test set", shardIdSet.equals(resultShardIdSet));
    }

    @Test
    public void testValidShardIteratorType() {
        when(mockClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(shardIteratorResult);
        String expectedShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString();
        proxy.getIterator("Shard-001", expectedShardIteratorType, "1234");

        verify(mockClient).getShardIterator(argThat(both(isA(GetShardIteratorRequest.class))
                .and(hasProperty("shardIteratorType", equalTo(expectedShardIteratorType)))));
    }

    @Test
    public void testInvalidShardIteratorIsntChanged() {
        when(mockClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(shardIteratorResult);
        String expectedShardIteratorType = ShardIteratorType.AT_TIMESTAMP.toString();
        proxy.getIterator("Shard-001", expectedShardIteratorType, "1234");

        verify(mockClient).getShardIterator(argThat(both(isA(GetShardIteratorRequest.class))
                .and(hasProperty("shardIteratorType", equalTo(expectedShardIteratorType)))));
    }

    @Test(expected = AmazonServiceException.class)
    public void testNullShardIteratorType() {
        when(mockClient.getShardIterator(any(GetShardIteratorRequest.class))).thenThrow(new AmazonServiceException("expected null"));
        String expectedShardIteratorType = null;
        proxy.getIterator("Shard-001", expectedShardIteratorType, "1234");

        verify(mockClient).getShardIterator(argThat(both(isA(GetShardIteratorRequest.class))
                .and(hasProperty("shardIteratorType", nullValue(String.class)))));
    }

    private DescribeStreamResult createGetStreamInfoResponse(List<Shard> shards1, boolean isHasMoreShards) {
        // Create stream description
        StreamDescription description = new StreamDescription();
        description.setHasMoreShards(isHasMoreShards);
        description.setShards(shards1);
        description.setStreamStatus(StreamStatus.ACTIVE);

        // Create Describe Stream Result
        DescribeStreamResult response = new DescribeStreamResult();
        response.setStreamDescription(description);
        return response;
    }

    // Matcher for testing describe stream request with specific start shard ID.
    private static class IsRequestWithStartShardId extends ArgumentMatcher<DescribeStreamRequest> {
        private final String shardId;

        public IsRequestWithStartShardId(String shardId) {
            this.shardId = shardId;
        }

        @Override
        public boolean matches(Object request) {
            String startShardId = ((DescribeStreamRequest) request).getExclusiveStartShardId();
            // If startShardId equals to null, shardId should also be null.
            if (startShardId == null) {
                return shardId == null;
            }
            return startShardId.equals(shardId);
        }
    }

}
