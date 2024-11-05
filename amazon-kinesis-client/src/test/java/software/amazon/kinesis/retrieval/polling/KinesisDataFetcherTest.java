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
package software.amazon.kinesis.retrieval.polling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.checkpoint.SentinelCheckpoint;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.exceptions.KinesisClientLibException;
import software.amazon.kinesis.leases.ShardObjectHelper;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for KinesisDataFetcher.
 */
@RunWith(MockitoJUnitRunner.class)
public class KinesisDataFetcherTest {
    private static final int MAX_RECORDS = 1;
    private static final String STREAM_NAME = "streamName";
    private static final String SHARD_ID = "shardId-1";
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_AT_TIMESTAMP =
            InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(1000));
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    private KinesisDataFetcher kinesisDataFetcher;

    @Mock
    private KinesisAsyncClient kinesisClient;

    @Mock
    private CompletableFuture<GetRecordsResponse> getRecordsResponseFuture;

    @Mock
    private CompletableFuture<GetShardIteratorResponse> getShardIteratorResponseFuture;

    @Rule
    public ExpectedException expectedExceptionRule = ExpectedException.none();

    @Before
    public void setup() {
        kinesisDataFetcher =
                new KinesisDataFetcher(kinesisClient, STREAM_NAME, SHARD_ID, MAX_RECORDS, NULL_METRICS_FACTORY);
    }

    /**
     * Test initialize() with the LATEST iterator instruction
     */
    @Test
    public final void testInitializeLatest() throws Exception {
        testInitializeAndFetch(
                ShardIteratorType.LATEST.toString(), ShardIteratorType.LATEST.toString(), INITIAL_POSITION_LATEST);
    }

    /**
     * Test initialize() with the TIME_ZERO iterator instruction
     */
    @Test
    public final void testInitializeTimeZero() throws Exception {
        testInitializeAndFetch(
                ShardIteratorType.TRIM_HORIZON.toString(),
                ShardIteratorType.TRIM_HORIZON.toString(),
                INITIAL_POSITION_TRIM_HORIZON);
    }

    /**
     * Test initialize() with the AT_TIMESTAMP iterator instruction
     */
    @Test
    public final void testInitializeAtTimestamp() throws Exception {
        testInitializeAndFetch(
                ShardIteratorType.AT_TIMESTAMP.toString(),
                ShardIteratorType.AT_TIMESTAMP.toString(),
                INITIAL_POSITION_AT_TIMESTAMP);
    }

    /**
     * Test initialize() when a flushpoint exists.
     */
    @Ignore
    @Test
    public final void testInitializeFlushpoint() throws Exception {
        testInitializeAndFetch("foo", "123", INITIAL_POSITION_LATEST);
    }

    /**
     * Test initialize() with an invalid iterator instruction
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testInitializeInvalid() throws Exception {
        testInitializeAndFetch("foo", null, INITIAL_POSITION_LATEST);
    }

    private CompletableFuture<GetShardIteratorResponse> makeGetShardIteratorResponse(String shardIterator) {
        return CompletableFuture.completedFuture(
                GetShardIteratorResponse.builder().shardIterator(shardIterator).build());
    }

    @Test
    public void testAdvanceIteratorTo() throws KinesisClientLibException {
        final Checkpointer checkpoint = mock(Checkpointer.class);
        final String iteratorA = "foo";
        final String iteratorB = "bar";
        final String seqA = "123";
        final String seqB = "456";

        ArgumentCaptor<GetShardIteratorRequest> shardIteratorRequestCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);

        when(kinesisClient.getShardIterator(shardIteratorRequestCaptor.capture()))
                .thenReturn(makeGetShardIteratorResponse(iteratorA))
                .thenReturn(makeGetShardIteratorResponse(iteratorA))
                .thenReturn(makeGetShardIteratorResponse(iteratorB));

        kinesisDataFetcher.initialize(seqA, null);
        kinesisDataFetcher.advanceIteratorTo(seqA, null);
        kinesisDataFetcher.advanceIteratorTo(seqB, null);

        final List<GetShardIteratorRequest> shardIteratorRequests = shardIteratorRequestCaptor.getAllValues();
        assertEquals(3, shardIteratorRequests.size());

        int count = 0;
        for (GetShardIteratorRequest request : shardIteratorRequests) {
            assertEquals(STREAM_NAME, request.streamName());
            assertEquals(SHARD_ID, request.shardId());
            assertEquals(ShardIteratorType.AT_SEQUENCE_NUMBER.toString(), request.shardIteratorTypeAsString());
            if (count == 2) {
                assertEquals(seqB, request.startingSequenceNumber());
            } else {
                assertEquals(seqA, request.startingSequenceNumber());
            }
            count++;
        }
    }

    private GetShardIteratorRequest makeGetShardIteratorRequest(String shardIteratorType) {
        return GetShardIteratorRequest.builder()
                .shardIteratorType(shardIteratorType)
                .streamName(STREAM_NAME)
                .shardId(SHARD_ID)
                .build();
    }

    @Test
    public void testAdvanceIteratorToTrimHorizonLatestAndAtTimestamp() {
        final ArgumentCaptor<GetShardIteratorRequest> requestCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        final String iteratorHorizon = "TRIM_HORIZON";
        final String iteratorLatest = "LATEST";
        final String iteratorAtTimestamp = "AT_TIMESTAMP";
        final Map<ShardIteratorType, GetShardIteratorRequest> requestsMap = Arrays.stream(
                        new String[] {iteratorHorizon, iteratorLatest, iteratorAtTimestamp})
                .map(this::makeGetShardIteratorRequest)
                .collect(Collectors.toMap(r -> ShardIteratorType.valueOf(r.shardIteratorTypeAsString()), r -> r));
        GetShardIteratorRequest tsReq = requestsMap.get(ShardIteratorType.AT_TIMESTAMP);
        requestsMap.put(
                ShardIteratorType.AT_TIMESTAMP,
                tsReq.toBuilder()
                        .timestamp(INITIAL_POSITION_AT_TIMESTAMP.getTimestamp().toInstant())
                        .build());

        when(kinesisClient.getShardIterator(requestCaptor.capture()))
                .thenReturn(makeGetShardIteratorResponse(iteratorHorizon))
                .thenReturn(makeGetShardIteratorResponse(iteratorLatest))
                .thenReturn(makeGetShardIteratorResponse(iteratorAtTimestamp));

        kinesisDataFetcher.advanceIteratorTo(ShardIteratorType.TRIM_HORIZON.toString(), INITIAL_POSITION_TRIM_HORIZON);
        assertEquals(iteratorHorizon, kinesisDataFetcher.getNextIterator());

        kinesisDataFetcher.advanceIteratorTo(ShardIteratorType.LATEST.toString(), INITIAL_POSITION_LATEST);
        assertEquals(iteratorLatest, kinesisDataFetcher.getNextIterator());

        kinesisDataFetcher.advanceIteratorTo(ShardIteratorType.AT_TIMESTAMP.toString(), INITIAL_POSITION_AT_TIMESTAMP);
        assertEquals(iteratorAtTimestamp, kinesisDataFetcher.getNextIterator());

        final List<GetShardIteratorRequest> requests = requestCaptor.getAllValues();
        assertEquals(3, requests.size());
        requests.forEach(request -> {
            final ShardIteratorType type = ShardIteratorType.fromValue(request.shardIteratorTypeAsString());
            assertEquals(requestsMap.get(type).startingSequenceNumber(), request.startingSequenceNumber());
            requestsMap.remove(type);
        });
        assertEquals(0, requestsMap.size());
    }

    private GetRecordsRequest makeGetRecordsRequest(String shardIterator) {
        return GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .limit(MAX_RECORDS)
                .build();
    }

    @Test
    public void testGetRecordsWithResourceNotFoundException() throws Exception {
        final ArgumentCaptor<GetShardIteratorRequest> iteratorCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        final ArgumentCaptor<GetRecordsRequest> recordsCaptor = ArgumentCaptor.forClass(GetRecordsRequest.class);
        // Set up arguments used by proxy
        final String nextIterator = "TestShardIterator";

        final GetShardIteratorRequest expectedIteratorRequest =
                makeGetShardIteratorRequest(ShardIteratorType.LATEST.name());
        final GetRecordsRequest expectedRecordsRequest = makeGetRecordsRequest(nextIterator);

        final CompletableFuture<GetRecordsResponse> future = mock(CompletableFuture.class);

        // Set up proxy mock methods
        when(kinesisClient.getShardIterator(iteratorCaptor.capture()))
                .thenReturn(makeGetShardIteratorResponse(nextIterator));
        when(kinesisClient.getRecords(recordsCaptor.capture())).thenReturn(future);
        when(future.get(anyLong(), any(TimeUnit.class)))
                .thenThrow(new ExecutionException(ResourceNotFoundException.builder()
                        .message("Test Exception")
                        .build()));

        // Create data fectcher and initialize it with latest type checkpoint
        kinesisDataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);
        final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy =
                new SynchronousGetRecordsRetrievalStrategy(kinesisDataFetcher);
        try {
            // Call records of dataFetcher which will throw an exception
            getRecordsRetrievalStrategy.getRecords(MAX_RECORDS);
        } finally {
            // Test shard has reached the end
            assertTrue("Shard should reach the end", kinesisDataFetcher.isShardEndReached());
            assertEquals(
                    expectedIteratorRequest.startingSequenceNumber(),
                    iteratorCaptor.getValue().startingSequenceNumber());
            assertEquals(
                    expectedRecordsRequest.shardIterator(),
                    recordsCaptor.getValue().shardIterator());
        }
    }

    @Test
    public void testGetRecordsThrowsSdkException() throws Exception {
        expectedExceptionRule.expect(SdkException.class);
        expectedExceptionRule.expectMessage("Test Exception");

        CompletableFuture<GetShardIteratorResponse> getShardIteratorFuture = CompletableFuture.completedFuture(
                GetShardIteratorResponse.builder().shardIterator("test").build());

        // Set up proxy mock methods
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(getShardIteratorFuture);
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(getRecordsResponseFuture);
        when(getRecordsResponseFuture.get(anyLong(), any(TimeUnit.class)))
                .thenThrow(new ExecutionException(
                        SdkException.builder().message("Test Exception").build()));

        // Create data fectcher and initialize it with latest type checkpoint
        kinesisDataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);
        final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy =
                new SynchronousGetRecordsRetrievalStrategy(kinesisDataFetcher);

        // Call records of dataFetcher which will throw an exception
        getRecordsRetrievalStrategy.getRecords(MAX_RECORDS);
    }

    @Test
    public void testNonNullGetRecords() throws Exception {
        final String nextIterator = "TestIterator";
        final ArgumentCaptor<GetShardIteratorRequest> iteratorCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        final ArgumentCaptor<GetRecordsRequest> recordsCaptor = ArgumentCaptor.forClass(GetRecordsRequest.class);
        final GetShardIteratorRequest expectedIteratorRequest =
                makeGetShardIteratorRequest(ShardIteratorType.LATEST.name());
        final GetRecordsRequest expectedRecordsRequest = makeGetRecordsRequest(nextIterator);

        final CompletableFuture<GetRecordsResponse> future = mock(CompletableFuture.class);

        when(kinesisClient.getShardIterator(iteratorCaptor.capture()))
                .thenReturn(makeGetShardIteratorResponse(nextIterator));
        when(kinesisClient.getRecords(recordsCaptor.capture())).thenReturn(future);
        when(future.get(anyLong(), any(TimeUnit.class)))
                .thenThrow(new ExecutionException(ResourceNotFoundException.builder()
                        .message("Test Exception")
                        .build()));

        kinesisDataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);
        DataFetcherResult dataFetcherResult = kinesisDataFetcher.getRecords();

        assertNotNull(dataFetcherResult);
        assertEquals(
                expectedIteratorRequest.startingSequenceNumber(),
                iteratorCaptor.getValue().startingSequenceNumber());
        assertEquals(
                expectedRecordsRequest.shardIterator(), recordsCaptor.getValue().shardIterator());
    }

    private CompletableFuture<GetRecordsResponse> makeGetRecordsResponse(String nextIterator, List<Record> records) {
        List<ChildShard> childShards = new ArrayList<>();
        if (nextIterator == null) {
            childShards = createChildShards();
        }
        return CompletableFuture.completedFuture(GetRecordsResponse.builder()
                .nextShardIterator(nextIterator)
                .records(CollectionUtils.isNullOrEmpty(records) ? Collections.emptyList() : records)
                .childShards(childShards)
                .build());
    }

    private List<ChildShard> createChildShards() {
        List<ChildShard> childShards = new ArrayList<>();
        List<String> parentShards = new ArrayList<>();
        parentShards.add(SHARD_ID);
        ChildShard leftChild = ChildShard.builder()
                .shardId("Shard-2")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"))
                .build();
        ChildShard rightChild = ChildShard.builder()
                .shardId("Shard-3")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("50", "99"))
                .build();
        childShards.add(leftChild);
        childShards.add(rightChild);
        return childShards;
    }

    @Test
    public void testFetcherDoesNotAdvanceWithoutAccept() throws InterruptedException, ExecutionException {
        final ArgumentCaptor<GetShardIteratorRequest> iteratorCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        final ArgumentCaptor<GetRecordsRequest> recordsCaptor = ArgumentCaptor.forClass(GetRecordsRequest.class);
        final String initialIterator = "InitialIterator";
        final String nextIterator1 = "NextIteratorOne";
        final String nextIterator2 = "NextIteratorTwo";
        final CompletableFuture<GetRecordsResponse> nonAdvancingResult1 = makeGetRecordsResponse(initialIterator, null);
        final CompletableFuture<GetRecordsResponse> nonAdvancingResult2 = makeGetRecordsResponse(nextIterator1, null);
        final CompletableFuture<GetRecordsResponse> finalNonAdvancingResult =
                makeGetRecordsResponse(nextIterator2, null);
        final CompletableFuture<GetRecordsResponse> advancingResult1 = makeGetRecordsResponse(nextIterator1, null);
        final CompletableFuture<GetRecordsResponse> advancingResult2 = makeGetRecordsResponse(nextIterator2, null);
        final CompletableFuture<GetRecordsResponse> finalAdvancingResult = makeGetRecordsResponse(null, null);

        when(kinesisClient.getShardIterator(iteratorCaptor.capture()))
                .thenReturn(makeGetShardIteratorResponse(initialIterator));
        when(kinesisClient.getRecords(recordsCaptor.capture()))
                .thenReturn(
                        nonAdvancingResult1,
                        advancingResult1,
                        nonAdvancingResult2,
                        advancingResult2,
                        finalNonAdvancingResult,
                        finalAdvancingResult);

        kinesisDataFetcher.initialize(
                "TRIM_HORIZON",
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));

        assertNoAdvance(nonAdvancingResult1.get(), initialIterator);
        assertAdvanced(advancingResult1.get(), initialIterator, nextIterator1);
        verify(kinesisClient, times(2)).getRecords(any(GetRecordsRequest.class));

        assertNoAdvance(nonAdvancingResult2.get(), nextIterator1);
        assertAdvanced(advancingResult2.get(), nextIterator1, nextIterator2);
        verify(kinesisClient, times(4)).getRecords(any(GetRecordsRequest.class));

        assertNoAdvance(finalNonAdvancingResult.get(), nextIterator2);
        assertAdvanced(finalAdvancingResult.get(), nextIterator2, null);
        verify(kinesisClient, times(6)).getRecords(any(GetRecordsRequest.class));

        reset(kinesisClient);

        DataFetcherResult terminal = kinesisDataFetcher.getRecords();
        assertTrue(terminal.isShardEnd());
        assertNotNull(terminal.getResult());

        final GetRecordsResponse terminalResult = terminal.getResult();
        assertNotNull(terminalResult.records());
        assertEquals(0, terminalResult.records().size());
        assertNull(terminalResult.nextShardIterator());
        assertEquals(kinesisDataFetcher.TERMINAL_RESULT, terminal);

        verify(kinesisClient, never()).getRecords(any(GetRecordsRequest.class));
    }

    @Test
    @Ignore
    public void testRestartIterator() throws Exception {
        GetRecordsResponse getRecordsResult = mock(GetRecordsResponse.class);
        GetRecordsResponse restartGetRecordsResponse =
                makeGetRecordsResponse(null, null).get(anyLong(), any(TimeUnit.class));
        Record record = mock(Record.class);
        final String nextShardIterator = "NextShardIterator";
        final String sequenceNumber = "SequenceNumber";

        when(getRecordsResult.records()).thenReturn(Collections.singletonList(record));
        when(getRecordsResult.nextShardIterator()).thenReturn(nextShardIterator);
        when(record.sequenceNumber()).thenReturn(sequenceNumber);

        kinesisDataFetcher.initialize(InitialPositionInStream.LATEST.toString(), INITIAL_POSITION_LATEST);
        assertEquals(getRecordsResult, kinesisDataFetcher.getRecords().accept());

        kinesisDataFetcher.restartIterator();
        assertEquals(restartGetRecordsResponse, kinesisDataFetcher.getRecords().accept());
    }

    @Test
    public void testRestartIteratorUsesAfterSequenceNumberIteratorType() throws Exception {
        final String iterator = "iterator";
        final String sequenceNumber = "123";

        final ArgumentCaptor<GetShardIteratorRequest> shardIteratorRequestCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);

        when(kinesisClient.getShardIterator(shardIteratorRequestCaptor.capture()))
                .thenReturn(makeGetShardIteratorResponse(iterator));

        kinesisDataFetcher.initialize(sequenceNumber, INITIAL_POSITION_LATEST);
        kinesisDataFetcher.restartIterator();
        // The advanceIteratorTo call should not use AFTER_SEQUENCE_NUMBER iterator
        // type unless called by restartIterator
        kinesisDataFetcher.advanceIteratorTo(sequenceNumber, INITIAL_POSITION_LATEST);

        final List<GetShardIteratorRequest> shardIteratorRequests = shardIteratorRequestCaptor.getAllValues();
        assertEquals(3, shardIteratorRequests.size());
        assertEquals(
                ShardIteratorType.AT_SEQUENCE_NUMBER.toString(),
                shardIteratorRequests.get(0).shardIteratorTypeAsString());
        assertEquals(
                ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(),
                shardIteratorRequests.get(1).shardIteratorTypeAsString());
        assertEquals(
                ShardIteratorType.AT_SEQUENCE_NUMBER.toString(),
                shardIteratorRequests.get(2).shardIteratorTypeAsString());
    }

    @Test(expected = IllegalStateException.class)
    public void testRestartIteratorNotInitialized() {
        kinesisDataFetcher.restartIterator();
    }

    @Test
    public void testTimeoutExceptionIsRetryableForGetShardIterator() throws Exception {
        expectedExceptionRule.expect(RetryableRetrievalException.class);
        expectedExceptionRule.expectCause(isA(TimeoutException.class));
        expectedExceptionRule.expectMessage("Timeout");

        // Set up proxy mock methods
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenReturn(getShardIteratorResponseFuture);
        when(getShardIteratorResponseFuture.get(anyLong(), any(TimeUnit.class)))
                .thenThrow(new TimeoutException("Timeout"));

        // Create data fectcher and initialize it with latest type checkpoint
        kinesisDataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);
    }

    @Test
    public void testTimeoutExceptionIsRetryableForGetRecords() throws Exception {
        expectedExceptionRule.expect(RetryableRetrievalException.class);
        expectedExceptionRule.expectCause(isA(TimeoutException.class));
        expectedExceptionRule.expectMessage("Timeout");

        CompletableFuture<GetShardIteratorResponse> getShardIteratorFuture = CompletableFuture.completedFuture(
                GetShardIteratorResponse.builder().shardIterator("test").build());

        // Set up proxy mock methods
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(getShardIteratorFuture);
        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(getRecordsResponseFuture);
        when(getRecordsResponseFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException("Timeout"));

        // Create data fectcher and initialize it with latest type checkpoint
        kinesisDataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);
        final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy =
                new SynchronousGetRecordsRetrievalStrategy(kinesisDataFetcher);

        // Call records of dataFetcher which will throw an exception
        getRecordsRetrievalStrategy.getRecords(MAX_RECORDS);
    }

    private DataFetcherResult assertAdvanced(
            GetRecordsResponse expectedResult, String previousValue, String nextValue) {
        DataFetcherResult acceptResult = kinesisDataFetcher.getRecords();
        assertEquals(expectedResult, acceptResult.getResult());

        assertEquals(previousValue, kinesisDataFetcher.getNextIterator());
        assertFalse(kinesisDataFetcher.isShardEndReached());

        assertEquals(expectedResult, acceptResult.accept());
        assertEquals(nextValue, kinesisDataFetcher.getNextIterator());
        if (nextValue == null) {
            assertTrue(kinesisDataFetcher.isShardEndReached());
        }

        return acceptResult;
    }

    private DataFetcherResult assertNoAdvance(final GetRecordsResponse expectedResult, final String previousValue) {
        assertEquals(previousValue, kinesisDataFetcher.getNextIterator());
        DataFetcherResult noAcceptResult = kinesisDataFetcher.getRecords();
        assertEquals(expectedResult, noAcceptResult.getResult());

        assertEquals(previousValue, kinesisDataFetcher.getNextIterator());

        return noAcceptResult;
    }

    private void testInitializeAndFetch(
            final String iteratorType,
            final String seqNo,
            final InitialPositionInStreamExtended initialPositionInStream)
            throws Exception {
        final ArgumentCaptor<GetShardIteratorRequest> iteratorCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        final ArgumentCaptor<GetRecordsRequest> recordsCaptor = ArgumentCaptor.forClass(GetRecordsRequest.class);
        final String iterator = "foo";
        final List<Record> expectedRecords = Collections.emptyList();
        GetShardIteratorRequest expectedIteratorRequest = makeGetShardIteratorRequest(iteratorType);
        if (iteratorType.equals(ShardIteratorType.AT_TIMESTAMP.toString())) {
            expectedIteratorRequest = expectedIteratorRequest.toBuilder()
                    .timestamp(initialPositionInStream.getTimestamp().toInstant())
                    .build();
        } else if (iteratorType.equals(ShardIteratorType.AT_SEQUENCE_NUMBER.toString())) {
            expectedIteratorRequest = expectedIteratorRequest.toBuilder()
                    .startingSequenceNumber(seqNo)
                    .build();
        }

        when(kinesisClient.getShardIterator(iteratorCaptor.capture()))
                .thenReturn(makeGetShardIteratorResponse(iterator));

        when(kinesisClient.getRecords(recordsCaptor.capture()))
                .thenReturn(makeGetRecordsResponse(null, expectedRecords));

        Checkpointer checkpoint = mock(Checkpointer.class);

        final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy =
                new SynchronousGetRecordsRetrievalStrategy(kinesisDataFetcher);
        kinesisDataFetcher.initialize(seqNo, initialPositionInStream);

        assertEquals(
                expectedRecords,
                getRecordsRetrievalStrategy.getRecords(MAX_RECORDS).records());
        verify(kinesisClient, times(1)).getShardIterator(any(GetShardIteratorRequest.class));
        verify(kinesisClient, times(1)).getRecords(any(GetRecordsRequest.class));
    }
}
