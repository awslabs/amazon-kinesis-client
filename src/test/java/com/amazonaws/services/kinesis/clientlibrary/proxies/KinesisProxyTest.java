package com.amazonaws.services.kinesis.clientlibrary.proxies;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.AmazonServiceException;
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

@RunWith(MockitoJUnitRunner.class)
public class KinesisProxyTest {
    private static final String TEST_STRING = "TestString";
    private static final long BACKOFF_TIME = 10L;
    private static final int RETRY_TIMES = 3;

    @Mock
    private AmazonKinesisClient mockClient;
    @Mock
    private AWSCredentialsProvider mockCredentialsProvider;
    @Mock
    private GetShardIteratorResult shardIteratorResult;
    @Mock
    private DescribeStreamResult describeStreamResult;
    @Mock
    private StreamDescription streamDescription;
    @Mock
    private Shard shard;

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
                .describeStream(argThat(new OldIsRequestWithStartShardId(shards.get(1).getShardId())));

        Set<String> resultShardIdSets = proxy.getAllShardIds();
        assertThat("Result set should equal to Test set", shardIdSet, equalTo(resultShardIdSets));
    }

    @Test
    public void testGetShardListWithLimitExceededException() {
        // Set up mock :
        // First call describeStream throwing LimitExceededException;
        // Second call describeStream returning shards list.
        DescribeStreamResult response = createGetStreamInfoResponse(shards, false);
        doThrow(new LimitExceededException("Test Exception")).doReturn(response).when(mockClient)
                .describeStream(argThat(new OldIsRequestWithStartShardId(null)));

         Set<String> resultShardIdSet = proxy.getAllShardIds();
         assertThat("Result set should equal to Test set", shardIdSet, equalTo(resultShardIdSet));
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

    @Test(expected = AmazonServiceException.class)
    public void testGetStreamInfoFails() throws Exception {
        when(mockClient.describeStream(any(DescribeStreamRequest.class))).thenThrow(new AmazonServiceException("Test"));
        proxy.getShardList();
        verify(mockClient).describeStream(any(DescribeStreamRequest.class));
    }

    @Test
    public void testGetStreamInfoThrottledOnce() throws Exception {
        when(mockClient.describeStream(any(DescribeStreamRequest.class))).thenThrow(new LimitExceededException("Test"))
                .thenReturn(describeStreamResult);
        when(describeStreamResult.getStreamDescription()).thenReturn(streamDescription);
        when(streamDescription.getHasMoreShards()).thenReturn(false);
        when(streamDescription.getStreamStatus()).thenReturn(StreamStatus.ACTIVE.name());
        List<Shard> expectedShards = Collections.singletonList(shard);
        when(streamDescription.getShards()).thenReturn(expectedShards);

        List<Shard> actualShards = proxy.getShardList();

        assertThat(actualShards, equalTo(expectedShards));

        verify(mockClient, times(2)).describeStream(any(DescribeStreamRequest.class));
        verify(describeStreamResult, times(3)).getStreamDescription();
        verify(streamDescription).getStreamStatus();
        verify(streamDescription).isHasMoreShards();
    }

    @Test(expected = LimitExceededException.class)
    public void testGetStreamInfoThrottledAll() throws Exception {
        when(mockClient.describeStream(any(DescribeStreamRequest.class))).thenThrow(new LimitExceededException("Test"));

        proxy.getShardList();
    }

    @Test
    public void testGetStreamInfoStoresOffset() throws Exception {
        when(describeStreamResult.getStreamDescription()).thenReturn(streamDescription);
        when(streamDescription.getStreamStatus()).thenReturn(StreamStatus.ACTIVE.name());
        Shard shard1 = mock(Shard.class);
        Shard shard2 = mock(Shard.class);
        Shard shard3 = mock(Shard.class);
        List<Shard> shardList1 = Collections.singletonList(shard1);
        List<Shard> shardList2 = Collections.singletonList(shard2);
        List<Shard> shardList3 = Collections.singletonList(shard3);

        String shardId1 = "ShardId-0001";
        String shardId2 = "ShardId-0002";
        String shardId3 = "ShardId-0003";

        when(shard1.getShardId()).thenReturn(shardId1);
        when(shard2.getShardId()).thenReturn(shardId2);
        when(shard3.getShardId()).thenReturn(shardId3);

        when(streamDescription.getShards()).thenReturn(shardList1).thenReturn(shardList2).thenReturn(shardList3);
        when(streamDescription.isHasMoreShards()).thenReturn(true, true, false);
        when(mockClient.describeStream(argThat(describeWithoutShardId()))).thenReturn(describeStreamResult);

        when(mockClient.describeStream(argThat(describeWithShardId(shardId1))))
                .thenThrow(new LimitExceededException("1"), new LimitExceededException("2"),
                        new LimitExceededException("3"))
                .thenReturn(describeStreamResult);

        when(mockClient.describeStream(argThat(describeWithShardId(shardId2)))).thenReturn(describeStreamResult);

        boolean limitExceeded = false;
        try {
            proxy.getShardList();
        } catch (LimitExceededException le) {
            limitExceeded = true;
        }
        assertThat(limitExceeded, equalTo(true));
        List<Shard> actualShards = proxy.getShardList();
        List<Shard> expectedShards = Arrays.asList(shard1, shard2, shard3);

        assertThat(actualShards, equalTo(expectedShards));

        verify(mockClient).describeStream(argThat(describeWithoutShardId()));
        verify(mockClient, times(4)).describeStream(argThat(describeWithShardId(shardId1)));
        verify(mockClient).describeStream(argThat(describeWithShardId(shardId2)));

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

    private IsRequestWithStartShardId describeWithoutShardId() {
        return describeWithShardId(null);
    }

    private IsRequestWithStartShardId describeWithShardId(String shardId) {
        return new IsRequestWithStartShardId(shardId);
    }

    // Matcher for testing describe stream request with specific start shard ID.
    private static class IsRequestWithStartShardId extends TypeSafeDiagnosingMatcher<DescribeStreamRequest> {
        private final String shardId;

        public IsRequestWithStartShardId(String shardId) {
            this.shardId = shardId;
        }

        @Override
        protected boolean matchesSafely(DescribeStreamRequest item, Description mismatchDescription) {
            if (shardId == null) {
                if (item.getExclusiveStartShardId() != null) {
                    mismatchDescription.appendText("Expected starting shard id of null, but was ")
                            .appendValue(item.getExclusiveStartShardId());
                    return false;
                }
            } else if (!shardId.equals(item.getExclusiveStartShardId())) {
                mismatchDescription.appendValue(shardId).appendText(" doesn't match expected ")
                        .appendValue(item.getExclusiveStartShardId());
                return false;
            }

            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("A DescribeStreamRequest with a starting shard if of ").appendValue(shardId);
        }
    }

    private static class OldIsRequestWithStartShardId extends ArgumentMatcher<DescribeStreamRequest> {
        private final String shardId;

        public OldIsRequestWithStartShardId(String shardId) {
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
