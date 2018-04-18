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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

import software.amazon.kinesis.checkpoint.SentinelCheckpoint;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

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

    private KinesisDataFetcher kinesisDataFetcher;

    @Mock
    private AmazonKinesis amazonKinesis;

    @Before
    public void setup() {
        kinesisDataFetcher = new KinesisDataFetcher(amazonKinesis, STREAM_NAME, SHARD_ID, MAX_RECORDS);
    }

    /**
     * Test initialize() with the LATEST iterator instruction
     */
    @Test
    public final void testInitializeLatest() throws Exception {
        testInitializeAndFetch(ShardIteratorType.LATEST.toString(),
                ShardIteratorType.LATEST.toString(),
                INITIAL_POSITION_LATEST);
    }

    /**
     * Test initialize() with the TIME_ZERO iterator instruction
     */
    @Test
    public final void testInitializeTimeZero() throws Exception {
        testInitializeAndFetch(ShardIteratorType.TRIM_HORIZON.toString(),
                ShardIteratorType.TRIM_HORIZON.toString(),
                INITIAL_POSITION_TRIM_HORIZON);
    }

    /**
     * Test initialize() with the AT_TIMESTAMP iterator instruction
     */
    @Test
    public final void testInitializeAtTimestamp() throws Exception {
        testInitializeAndFetch(ShardIteratorType.AT_TIMESTAMP.toString(),
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

    @Test
    public void testadvanceIteratorTo() throws KinesisClientLibException {
        final Checkpointer checkpoint = mock(Checkpointer.class);
        final String iteratorA = "foo";
        final String iteratorB = "bar";
        final String seqA = "123";
        final String seqB = "456";

        ArgumentCaptor<GetShardIteratorRequest> shardIteratorRequestCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);

        when(amazonKinesis.getShardIterator(shardIteratorRequestCaptor.capture()))
                .thenReturn(new GetShardIteratorResult().withShardIterator(iteratorA))
                .thenReturn(new GetShardIteratorResult().withShardIterator(iteratorA))
                .thenReturn(new GetShardIteratorResult().withShardIterator(iteratorB));
        when(checkpoint.getCheckpoint(SHARD_ID)).thenReturn(new ExtendedSequenceNumber(seqA));

        kinesisDataFetcher.initialize(seqA, null);
        kinesisDataFetcher.advanceIteratorTo(seqA, null);
        kinesisDataFetcher.advanceIteratorTo(seqB, null);

        final List<GetShardIteratorRequest> shardIteratorRequests = shardIteratorRequestCaptor.getAllValues();
        assertEquals(3, shardIteratorRequests.size());

        int count = 0;
        for (GetShardIteratorRequest request : shardIteratorRequests) {
            assertEquals(STREAM_NAME, request.getStreamName());
            assertEquals(SHARD_ID, request.getShardId());
            assertEquals(ShardIteratorType.AT_SEQUENCE_NUMBER.toString(), request.getShardIteratorType());
            if (count == 2) {
                assertEquals(seqB, request.getStartingSequenceNumber());
            } else {
                assertEquals(seqA, request.getStartingSequenceNumber());
            }
            count++;
        }
    }

    @Test
    public void testadvanceIteratorToTrimHorizonLatestAndAtTimestamp() {
        final ArgumentCaptor<GetShardIteratorRequest> requestCaptor = ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        final String iteratorHorizon = "TRIM_HORIZON";
        final String iteratorLatest = "LATEST";
        final String iteratorAtTimestamp = "AT_TIMESTAMP";
        final Map<ShardIteratorType, GetShardIteratorRequest> requestsMap = Arrays.stream(
                new String[] {iteratorHorizon, iteratorLatest, iteratorAtTimestamp})
                .map(iterator -> new GetShardIteratorRequest().withStreamName(STREAM_NAME).withShardId(SHARD_ID)
                        .withShardIteratorType(iterator))
                .collect(Collectors.toMap(r -> ShardIteratorType.valueOf(r.getShardIteratorType()), r -> r));
        requestsMap.get(ShardIteratorType.AT_TIMESTAMP).withTimestamp(INITIAL_POSITION_AT_TIMESTAMP.getTimestamp());

        when(amazonKinesis.getShardIterator(requestCaptor.capture()))
                .thenReturn(new GetShardIteratorResult().withShardIterator(iteratorHorizon))
                .thenReturn(new GetShardIteratorResult().withShardIterator(iteratorLatest))
                .thenReturn(new GetShardIteratorResult().withShardIterator(iteratorAtTimestamp));

        kinesisDataFetcher.advanceIteratorTo(ShardIteratorType.TRIM_HORIZON.toString(), INITIAL_POSITION_TRIM_HORIZON);
        assertEquals(iteratorHorizon, kinesisDataFetcher.getNextIterator());

        kinesisDataFetcher.advanceIteratorTo(ShardIteratorType.LATEST.toString(), INITIAL_POSITION_LATEST);
        assertEquals(iteratorLatest, kinesisDataFetcher.getNextIterator());

        kinesisDataFetcher.advanceIteratorTo(ShardIteratorType.AT_TIMESTAMP.toString(), INITIAL_POSITION_AT_TIMESTAMP);
        assertEquals(iteratorAtTimestamp, kinesisDataFetcher.getNextIterator());

        final List<GetShardIteratorRequest> requests = requestCaptor.getAllValues();
        assertEquals(3, requests.size());
        requests.forEach(request -> {
            final ShardIteratorType type = ShardIteratorType.fromValue(request.getShardIteratorType());
            assertEquals(requestsMap.get(type), request);
            requestsMap.remove(type);
        });
        assertEquals(0, requestsMap.size());
    }

    @Test
    public void testGetRecordsWithResourceNotFoundException() {
        final ArgumentCaptor<GetShardIteratorRequest> iteratorCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        final ArgumentCaptor<GetRecordsRequest> recordsCaptor = ArgumentCaptor.forClass(GetRecordsRequest.class);
        // Set up arguments used by proxy
        final String nextIterator = "TestShardIterator";

        final GetShardIteratorRequest expectedIteratorRequest = new GetShardIteratorRequest()
                .withStreamName(STREAM_NAME).withShardId(SHARD_ID).withShardIteratorType(ShardIteratorType.LATEST);
        final GetRecordsRequest expectedRecordsRequest = new GetRecordsRequest().withShardIterator(nextIterator)
                .withLimit(MAX_RECORDS);

        // Set up proxy mock methods
        when(amazonKinesis.getShardIterator(iteratorCaptor.capture()))
                .thenReturn(new GetShardIteratorResult().withShardIterator(nextIterator));
        when(amazonKinesis.getRecords(recordsCaptor.capture()))
                .thenThrow(new ResourceNotFoundException("Test Exception"));

        // Create data fectcher and initialize it with latest type checkpoint
        kinesisDataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);
        final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy =
                new SynchronousGetRecordsRetrievalStrategy(kinesisDataFetcher);
        // Call getRecords of dataFetcher which will throw an exception
        getRecordsRetrievalStrategy.getRecords(MAX_RECORDS);

        // Test shard has reached the end
        assertTrue("Shard should reach the end", kinesisDataFetcher.isShardEndReached());
        assertEquals(expectedIteratorRequest, iteratorCaptor.getValue());
        assertEquals(expectedRecordsRequest, recordsCaptor.getValue());
    }
    
    @Test
    public void testNonNullGetRecords() {
        final String nextIterator = "TestIterator";
        final ArgumentCaptor<GetShardIteratorRequest> iteratorCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        final ArgumentCaptor<GetRecordsRequest> recordsCaptor = ArgumentCaptor.forClass(GetRecordsRequest.class);
        final GetShardIteratorRequest expectedIteratorRequest = new GetShardIteratorRequest()
                .withStreamName(STREAM_NAME).withShardId(SHARD_ID).withShardIteratorType(ShardIteratorType.LATEST);
        final GetRecordsRequest expectedRecordsRequest = new GetRecordsRequest().withShardIterator(nextIterator)
                .withLimit(MAX_RECORDS);

        when(amazonKinesis.getShardIterator(iteratorCaptor.capture()))
                .thenReturn(new GetShardIteratorResult().withShardIterator(nextIterator));
        when(amazonKinesis.getRecords(recordsCaptor.capture()))
                .thenThrow(new ResourceNotFoundException("Test Exception"));

        kinesisDataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);
        DataFetcherResult dataFetcherResult = kinesisDataFetcher.getRecords();

        assertNotNull(dataFetcherResult);
        assertEquals(expectedIteratorRequest, iteratorCaptor.getValue());
        assertEquals(expectedRecordsRequest, recordsCaptor.getValue());
    }

    @Test
    public void testFetcherDoesNotAdvanceWithoutAccept() {
        final ArgumentCaptor<GetShardIteratorRequest> iteratorCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        final ArgumentCaptor <GetRecordsRequest> recordsCaptor = ArgumentCaptor.forClass(GetRecordsRequest.class);
        final String initialIterator = "InitialIterator";
        final String nextIterator1 = "NextIteratorOne";
        final String nextIterator2 = "NextIteratorTwo";
        final GetRecordsResult nonAdvancingResult1 = new GetRecordsResult().withNextShardIterator(initialIterator);
        final GetRecordsResult nonAdvancingResult2 = new GetRecordsResult().withNextShardIterator(nextIterator1);
        final GetRecordsResult finalNonAdvancingResult = new GetRecordsResult().withNextShardIterator(nextIterator2);
        final GetRecordsResult advancingResult1 = new GetRecordsResult().withNextShardIterator(nextIterator1);
        final GetRecordsResult advancingResult2 = new GetRecordsResult().withNextShardIterator(nextIterator2);
        final GetRecordsResult finalAdvancingResult = new GetRecordsResult();

        when(amazonKinesis.getShardIterator(iteratorCaptor.capture()))
                .thenReturn(new GetShardIteratorResult().withShardIterator(initialIterator));
        when(amazonKinesis.getRecords(recordsCaptor.capture())).thenReturn(nonAdvancingResult1, advancingResult1,
                nonAdvancingResult2, advancingResult2, finalNonAdvancingResult, finalAdvancingResult);

        kinesisDataFetcher.initialize("TRIM_HORIZON",
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));

        assertNoAdvance(nonAdvancingResult1, initialIterator);
        assertAdvanced(advancingResult1, initialIterator, nextIterator1);

        assertNoAdvance(nonAdvancingResult2, nextIterator1);
        assertAdvanced(advancingResult2, nextIterator1, nextIterator2);

        assertNoAdvance(finalNonAdvancingResult, nextIterator2);
        assertAdvanced(finalAdvancingResult, nextIterator2, null);

        verify(amazonKinesis, times(2)).getRecords(eq(new GetRecordsRequest().withShardIterator(initialIterator)
                .withLimit(MAX_RECORDS)));
        verify(amazonKinesis, times(2)).getRecords(eq(new GetRecordsRequest().withShardIterator(nextIterator1)
                .withLimit(MAX_RECORDS)));
        verify(amazonKinesis, times(2)).getRecords(eq(new GetRecordsRequest().withShardIterator(nextIterator2)
                .withLimit(MAX_RECORDS)));

        reset(amazonKinesis);

        DataFetcherResult terminal = kinesisDataFetcher.getRecords();
        assertTrue(terminal.isShardEnd());
        assertNotNull(terminal.getResult());

        final GetRecordsResult terminalResult = terminal.getResult();
        assertNotNull(terminalResult.getRecords());
        assertEquals(0, terminalResult.getRecords().size());
        assertNull(terminalResult.getNextShardIterator());
        assertEquals(kinesisDataFetcher.TERMINAL_RESULT, terminal);

        verify(amazonKinesis, never()).getRecords(any(GetRecordsRequest.class));
    }
    
    @Test
    @Ignore
    public void testRestartIterator() {
        GetRecordsResult getRecordsResult = mock(GetRecordsResult.class);
        GetRecordsResult restartGetRecordsResult = new GetRecordsResult();
        Record record = mock(Record.class);
        final String nextShardIterator = "NextShardIterator";
        final String sequenceNumber = "SequenceNumber";

        when(getRecordsResult.getRecords()).thenReturn(Collections.singletonList(record));
        when(getRecordsResult.getNextShardIterator()).thenReturn(nextShardIterator);
        when(record.getSequenceNumber()).thenReturn(sequenceNumber);

        kinesisDataFetcher.initialize(InitialPositionInStream.LATEST.toString(), INITIAL_POSITION_LATEST);
        assertEquals(getRecordsResult, kinesisDataFetcher.getRecords().accept());

        kinesisDataFetcher.restartIterator();
        assertEquals(restartGetRecordsResult, kinesisDataFetcher.getRecords().accept());
    }
    
    @Test (expected = IllegalStateException.class)
    public void testRestartIteratorNotInitialized() {
        kinesisDataFetcher.restartIterator();
    }

    private DataFetcherResult assertAdvanced(GetRecordsResult expectedResult, String previousValue, String nextValue) {
        DataFetcherResult acceptResult = kinesisDataFetcher.getRecords();
        assertEquals(expectedResult, acceptResult.getResult());

        assertEquals(previousValue, kinesisDataFetcher.getNextIterator());
        assertFalse(kinesisDataFetcher.isShardEndReached());

        assertEquals(expectedResult, acceptResult.accept());
        assertEquals(nextValue, kinesisDataFetcher.getNextIterator());
        if (nextValue == null) {
            assertTrue(kinesisDataFetcher.isShardEndReached());
        }

        verify(amazonKinesis, times(2)).getRecords(eq(new GetRecordsRequest().withShardIterator(previousValue)
                .withLimit(MAX_RECORDS)));

        return acceptResult;
    }

    private DataFetcherResult assertNoAdvance(final GetRecordsResult expectedResult, final String previousValue) {
        assertEquals(previousValue, kinesisDataFetcher.getNextIterator());
        DataFetcherResult noAcceptResult = kinesisDataFetcher.getRecords();
        assertEquals(expectedResult, noAcceptResult.getResult());

        assertEquals(previousValue, kinesisDataFetcher.getNextIterator());

        verify(amazonKinesis).getRecords(eq(new GetRecordsRequest().withShardIterator(previousValue)
                .withLimit(MAX_RECORDS)));

        return noAcceptResult;
    }

    private void testInitializeAndFetch(final String iteratorType,
                                        final String seqNo,
                                        final InitialPositionInStreamExtended initialPositionInStream) throws Exception {
        final ArgumentCaptor<GetShardIteratorRequest> iteratorCaptor =
                ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        final ArgumentCaptor<GetRecordsRequest> recordsCaptor = ArgumentCaptor.forClass(GetRecordsRequest.class);
        final String iterator = "foo";
        final List<Record> expectedRecords = Collections.emptyList();
        final GetShardIteratorRequest expectedIteratorRequest =
                new GetShardIteratorRequest().withStreamName(STREAM_NAME).withShardId(SHARD_ID)
                        .withShardIteratorType(iteratorType);
        if (iteratorType.equals(ShardIteratorType.AT_TIMESTAMP.toString())) {
            expectedIteratorRequest.withTimestamp(initialPositionInStream.getTimestamp());
        } else if (iteratorType.equals(ShardIteratorType.AT_SEQUENCE_NUMBER.toString())) {
            expectedIteratorRequest.withStartingSequenceNumber(seqNo);
        }
        final GetRecordsRequest expectedRecordsRequest = new GetRecordsRequest().withShardIterator(iterator)
                .withLimit(MAX_RECORDS);

        when(amazonKinesis.getShardIterator(iteratorCaptor.capture()))
                .thenReturn(new GetShardIteratorResult().withShardIterator(iterator));
        when(amazonKinesis.getRecords(recordsCaptor.capture()))
                .thenReturn(new GetRecordsResult().withRecords(expectedRecords));

        Checkpointer checkpoint = mock(Checkpointer.class);
        when(checkpoint.getCheckpoint(SHARD_ID)).thenReturn(new ExtendedSequenceNumber(seqNo));

        final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy =
                new SynchronousGetRecordsRetrievalStrategy(kinesisDataFetcher);
        kinesisDataFetcher.initialize(seqNo, initialPositionInStream);

        assertEquals(expectedRecords, getRecordsRetrievalStrategy.getRecords(MAX_RECORDS).getRecords());
        verify(amazonKinesis, times(1)).getShardIterator(eq(expectedIteratorRequest));
        verify(amazonKinesis, times(1)).getRecords(eq(expectedRecordsRequest));
    }

}
