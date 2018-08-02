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
package com.amazonaws.services.kinesis.clientlibrary.proxies;

public class KinesisProxyTest {
    /*private static final String TEST_STRING = "TestString";
    private static final long DESCRIBE_STREAM_BACKOFF_TIME = 10L;
    private static final long LIST_SHARDS_BACKOFF_TIME = 10L;
    private static final int DESCRIBE_STREAM_RETRY_TIMES = 3;
    private static final int LIST_SHARDS_RETRY_TIMES = 3;
    private static final String NEXT_TOKEN = "NextToken";

    @Mock
    private AmazonKinesis mockClient;
    @Mock
    private AmazonDynamoDBStreamsAdapterClient mockDDBStreamClient;
    @Mock
    private AmazonDynamoDBStreamsAdapterClientChild mockDDBChildClient;
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
    @Mock
    private KinesisClientLibConfiguration config;

    private KinesisProxy proxy;
    private KinesisProxy ddbProxy;
    private KinesisProxy ddbChildProxy;

    // Test shards for verifying.
    private Set<String> shardIdSet;
    private List<Shard> shards;

    @Before
    public void setUpTest() {
        // Set up kinesis ddbProxy
        when(config.getStreamName()).thenReturn(TEST_STRING);
        when(config.getListShardsBackoffTimeInMillis()).thenReturn(LIST_SHARDS_BACKOFF_TIME);
        when(config.getMaxListShardsRetryAttempts()).thenReturn(LIST_SHARDS_RETRY_TIMES);
        when(config.getKinesisCredentialsProvider()).thenReturn(mockCredentialsProvider);

        proxy = new KinesisProxy(config, mockClient);
        ddbProxy = new KinesisProxy(TEST_STRING, mockCredentialsProvider, mockDDBStreamClient,
                DESCRIBE_STREAM_BACKOFF_TIME, DESCRIBE_STREAM_RETRY_TIMES, LIST_SHARDS_BACKOFF_TIME,
                LIST_SHARDS_RETRY_TIMES);
        ddbChildProxy = new KinesisProxy(TEST_STRING, mockCredentialsProvider, mockDDBChildClient,
                DESCRIBE_STREAM_BACKOFF_TIME, DESCRIBE_STREAM_RETRY_TIMES, LIST_SHARDS_BACKOFF_TIME,
                LIST_SHARDS_RETRY_TIMES);
        
        // Set up test shards
        List<String> shardIds = Arrays.asList("shard-1", "shard-2", "shard-3", "shard-4");
        shardIdSet = new HashSet<>(shardIds);
        shards = shardIds.stream().map(shardId -> new Shard().shardId(shardId)).collect(Collectors.toList());
    }

    @Test
    public void testGetShardListWithMoreDataAvailable() {
        // Set up mock :
        // First call describeStream returning response with first two shards in the list;
        // Second call describeStream returning response with rest shards.
        DescribeStreamResult responseWithMoreData = createGetStreamInfoResponse(shards.subList(0, 2), true);
        DescribeStreamResult responseFinal = createGetStreamInfoResponse(shards.subList(2, shards.size()), false);
        doReturn(responseWithMoreData).when(mockDDBStreamClient).describeStream(argThat(new IsRequestWithStartShardId(null)));
        doReturn(responseFinal).when(mockDDBStreamClient)
                .describeStream(argThat(new OldIsRequestWithStartShardId(shards.get(1).shardId())));

        Set<String> resultShardIdSets = ddbProxy.getAllShardIds();
        assertThat("Result set should equal to Test set", shardIdSet, equalTo(resultShardIdSets));
    }

    @Test
    public void testGetShardListWithLimitExceededException() {
        // Set up mock :
        // First call describeStream throwing LimitExceededException;
        // Second call describeStream returning shards list.
        DescribeStreamResult response = createGetStreamInfoResponse(shards, false);
        doThrow(new LimitExceededException("Test Exception")).doReturn(response).when(mockDDBStreamClient)
                .describeStream(argThat(new OldIsRequestWithStartShardId(null)));

         Set<String> resultShardIdSet = ddbProxy.getAllShardIds();
         assertThat("Result set should equal to Test set", shardIdSet, equalTo(resultShardIdSet));
    }

    @Test
    public void testValidShardIteratorType() {
        when(mockDDBStreamClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(shardIteratorResult);
        String expectedShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString();
        ddbProxy.getIterator("Shard-001", expectedShardIteratorType, "1234");

        verify(mockDDBStreamClient).getShardIterator(argThat(both(isA(GetShardIteratorRequest.class))
                .and(hasProperty("shardIteratorType", equalTo(expectedShardIteratorType)))));
    }

    @Test
    public void testInvalidShardIteratorIsntChanged() {
        when(mockDDBStreamClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(shardIteratorResult);
        String expectedShardIteratorType = ShardIteratorType.AT_TIMESTAMP.toString();
        ddbProxy.getIterator("Shard-001", expectedShardIteratorType, "1234");

        verify(mockDDBStreamClient).getShardIterator(argThat(both(isA(GetShardIteratorRequest.class))
                .and(hasProperty("shardIteratorType", equalTo(expectedShardIteratorType)))));
    }

    @Test(expected = AmazonServiceException.class)
    public void testNullShardIteratorType() throws Exception {
        when(mockDDBStreamClient.getShardIterator(any(GetShardIteratorRequest.class))).thenThrow(new AmazonServiceException("expected null"));
        String expectedShardIteratorType = null;
        ddbProxy.getIterator("Shard-001", expectedShardIteratorType, "1234");

        verify(mockDDBStreamClient).getShardIterator(argThat(both(isA(GetShardIteratorRequest.class))
                .and(hasProperty("shardIteratorType", nullValue(String.class)))));
    }

    @Test(expected = AmazonServiceException.class)
    public void testGetStreamInfoFails() {
        when(mockDDBStreamClient.describeStream(any(DescribeStreamRequest.class))).thenThrow(new AmazonServiceException("Test"));
        try {
            ddbProxy.getShardList();
        } finally {
            verify(mockDDBStreamClient).describeStream(any(DescribeStreamRequest.class));
        }
    }

    @Test
    public void testGetStreamInfoThrottledOnce() throws Exception {
        when(mockDDBStreamClient.describeStream(any(DescribeStreamRequest.class))).thenThrow(new LimitExceededException("Test"))
                .thenReturn(describeStreamResult);
        when(describeStreamResult.getStreamDescription()).thenReturn(streamDescription);
        when(streamDescription.getHasMoreShards()).thenReturn(false);
        when(streamDescription.getStreamStatus()).thenReturn(StreamStatus.ACTIVE.name());
        List<Shard> expectedShards = Collections.singletonList(shard);
        when(streamDescription.getShards()).thenReturn(expectedShards);

        List<Shard> actualShards = ddbProxy.getShardList();

        assertThat(actualShards, equalTo(expectedShards));

        verify(mockDDBStreamClient, times(2)).describeStream(any(DescribeStreamRequest.class));
        verify(describeStreamResult, times(3)).getStreamDescription();
        verify(streamDescription).getStreamStatus();
        verify(streamDescription).isHasMoreShards();
    }

    @Test(expected = LimitExceededException.class)
    public void testGetStreamInfoThrottledAll() throws Exception {
        when(mockDDBStreamClient.describeStream(any(DescribeStreamRequest.class))).thenThrow(new LimitExceededException("Test"));

        ddbProxy.getShardList();
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

        when(shard1.shardId()).thenReturn(shardId1);
        when(shard2.shardId()).thenReturn(shardId2);
        when(shard3.shardId()).thenReturn(shardId3);

        when(streamDescription.getShards()).thenReturn(shardList1).thenReturn(shardList2).thenReturn(shardList3);
        when(streamDescription.isHasMoreShards()).thenReturn(true, true, false);
        when(mockDDBStreamClient.describeStream(argThat(describeWithoutShardId()))).thenReturn(describeStreamResult);

        when(mockDDBStreamClient.describeStream(argThat(describeWithShardId(shardId1))))
                .thenThrow(new LimitExceededException("1"), new LimitExceededException("2"),
                        new LimitExceededException("3"))
                .thenReturn(describeStreamResult);

        when(mockDDBStreamClient.describeStream(argThat(describeWithShardId(shardId2)))).thenReturn(describeStreamResult);

        boolean limitExceeded = false;
        try {
            ddbProxy.getShardList();
        } catch (LimitExceededException le) {
            limitExceeded = true;
        }
        assertThat(limitExceeded, equalTo(true));
        List<Shard> actualShards = ddbProxy.getShardList();
        List<Shard> expectedShards = Arrays.asList(shard1, shard2, shard3);

        assertThat(actualShards, equalTo(expectedShards));

        verify(mockDDBStreamClient).describeStream(argThat(describeWithoutShardId()));
        verify(mockDDBStreamClient, times(4)).describeStream(argThat(describeWithShardId(shardId1)));
        verify(mockDDBStreamClient).describeStream(argThat(describeWithShardId(shardId2)));

    }
    
    @Test
    public void testListShardsWithMoreDataAvailable() {
        ListShardsResult responseWithMoreData = new ListShardsResult().withShards(shards.subList(0, 2)).withNextToken(NEXT_TOKEN);
        ListShardsResult responseFinal = new ListShardsResult().withShards(shards.subList(2, shards.size())).withNextToken(null);
        when(mockClient.listShards(argThat(initialListShardsRequestMatcher()))).thenReturn(responseWithMoreData);
        when(mockClient.listShards(argThat(listShardsNextToken(NEXT_TOKEN)))).thenReturn(responseFinal);

        Set<String> resultShardIdSets = proxy.getAllShardIds();
        assertEquals(shardIdSet, resultShardIdSets);
    }

    @Test
    public void testListShardsWithLimiteExceededException() {
        ListShardsResult result = new ListShardsResult().withShards(shards);
        when(mockClient.listShards(argThat(initialListShardsRequestMatcher()))).thenThrow(LimitExceededException.class).thenReturn(result);

        Set <String> resultShardIdSet = proxy.getAllShardIds();
        assertEquals(shardIdSet, resultShardIdSet);
    }

    @Test(expected = AmazonServiceException.class)
    public void testListShardsFails() {
        when(mockClient.listShards(any(ListShardsRequest.class))).thenThrow(AmazonServiceException.class);
        try {
            proxy.getShardList();
        } finally {
            verify(mockClient).listShards(any(ListShardsRequest.class));
        }
    }

    @Test
    public void testListShardsThrottledOnce() {
        List<Shard> expected = Collections.singletonList(shard);
        ListShardsResult result = new ListShardsResult().withShards(expected);
        when(mockClient.listShards(argThat(initialListShardsRequestMatcher()))).thenThrow(LimitExceededException.class)
                .thenReturn(result);

        List<Shard> actualShards = proxy.getShardList();

        assertEquals(expected, actualShards);
        verify(mockClient, times(2)).listShards(argThat(initialListShardsRequestMatcher()));
    }

    @Test(expected = LimitExceededException.class)
    public void testListShardsThrottledAll() {
        when(mockClient.listShards(argThat(initialListShardsRequestMatcher()))).thenThrow(LimitExceededException.class);
        proxy.getShardList();
    }
    
    @Test
    public void testStreamNotInCorrectStatus() {
        when(mockClient.listShards(argThat(initialListShardsRequestMatcher()))).thenThrow(ResourceInUseException.class);
        assertNull(proxy.getShardList());
    }

    @Test
    public void testGetShardListWithDDBChildClient() {
        DescribeStreamResult responseWithMoreData = createGetStreamInfoResponse(shards.subList(0, 2), true);
        DescribeStreamResult responseFinal = createGetStreamInfoResponse(shards.subList(2, shards.size()), false);
        doReturn(responseWithMoreData).when(mockDDBChildClient).describeStream(argThat(new IsRequestWithStartShardId(null)));
        doReturn(responseFinal).when(mockDDBChildClient)
                .describeStream(argThat(new OldIsRequestWithStartShardId(shards.get(1).shardId())));

        Set<String> resultShardIdSets = ddbChildProxy.getAllShardIds();
        assertThat("Result set should equal to Test set", shardIdSet, equalTo(resultShardIdSets));
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
    // Matcher for testing describe stream request with specific start shard ID.

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
    
    private static ListShardsRequestMatcher initialListShardsRequestMatcher() {
        return new ListShardsRequestMatcher(null, null);
    }
    
    private static ListShardsRequestMatcher listShardsNextToken(final String nextToken) {
        return new ListShardsRequestMatcher(null, nextToken);
    }

    @AllArgsConstructor
    private static class ListShardsRequestMatcher extends TypeSafeDiagnosingMatcher<ListShardsRequest> {
        private final String shardId;
        private final String nextToken;

        @Override
        protected boolean matchesSafely(final ListShardsRequest listShardsRequest, final Description description) {
            if (shardId == null) {
                if (StringUtils.isNotEmpty(listShardsRequest.getExclusiveStartShardId())) {
                    description.appendText("Expected ExclusiveStartShardId to be null, but was ")
                            .appendValue(listShardsRequest.getExclusiveStartShardId());
                    return false;
                }
            } else {
                if (!shardId.equals(listShardsRequest.getExclusiveStartShardId())) {
                    description.appendText("Expected shardId: ").appendValue(shardId)
                            .appendText(" doesn't match actual shardId: ")
                            .appendValue(listShardsRequest.getExclusiveStartShardId());
                    return false;
                }
            }

            if (StringUtils.isNotEmpty(listShardsRequest.getNextToken())) {
                if (StringUtils.isNotEmpty(listShardsRequest.getStreamName()) || StringUtils.isNotEmpty(listShardsRequest.getExclusiveStartShardId())) {
                    return false;
                }

                if (!listShardsRequest.getNextToken().equals(nextToken)) {
                    description.appendText("Found nextToken: ").appendValue(listShardsRequest.getNextToken())
                            .appendText(" when it was supposed to be null.");
                    return false;
                }
            } else {
                return nextToken == null;
            }
            return true;
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("A ListShardsRequest with a shardId: ").appendValue(shardId)
                    .appendText(" and empty nextToken");
        }
    }*/

}
