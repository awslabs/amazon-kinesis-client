/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.kinesis.retrieval.polling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * These are the integration tests for the PrefetchRecordsPublisher class.
 */
@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class PrefetchRecordsPublisherIntegrationTest {
    private static final int MAX_SIZE = 3;
    private static final int MAX_BYTE_SIZE = 5 * 1024 * 1024;
    private static final int MAX_RECORDS_COUNT = 30_000;
    private static final int MAX_RECORDS_PER_CALL = 10_000;
    private static final long IDLE_MILLIS_BETWEEN_CALLS = 500L;
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    private PrefetchRecordsPublisher getRecordsCache;
    private GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    private KinesisDataFetcher dataFetcher;
    private ExecutorService executorService;
    private List<Record> records;
    private String operation = "ProcessTask";
    private String streamName = "streamName";
    private String shardId = "shardId-000000000000";

    @Mock
    private KinesisAsyncClient kinesisClient;
    @Mock
    private ExtendedSequenceNumber extendedSequenceNumber;
    @Mock
    private InitialPositionInStreamExtended initialPosition;

    @Before
    public void setup() throws InterruptedException, ExecutionException {
        records = new ArrayList<>();
        dataFetcher = spy(new KinesisDataFetcherForTest(kinesisClient, streamName, shardId, MAX_RECORDS_PER_CALL));
        getRecordsRetrievalStrategy = Mockito.spy(new SynchronousGetRecordsRetrievalStrategy(dataFetcher));
        executorService = spy(Executors.newFixedThreadPool(1));
        CompletableFuture<GetShardIteratorResponse> future = mock(CompletableFuture.class);

        when(extendedSequenceNumber.sequenceNumber()).thenReturn("LATEST");
        when(future.get()).thenReturn(GetShardIteratorResponse.builder().shardIterator("TestIterator").build());
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(future);

        getRecordsCache = new PrefetchRecordsPublisher(MAX_SIZE,
                MAX_BYTE_SIZE,
                MAX_RECORDS_COUNT,
                MAX_RECORDS_PER_CALL,
                getRecordsRetrievalStrategy,
                executorService,
                IDLE_MILLIS_BETWEEN_CALLS,
                new NullMetricsFactory(),
                operation,
                "test-shard");
    }

    @Test
    public void testRollingCache() {
        getRecordsCache.start(extendedSequenceNumber, initialPosition);
        sleep(IDLE_MILLIS_BETWEEN_CALLS);

        ProcessRecordsInput processRecordsInput1 = getRecordsCache.getNextResult();

        assertTrue(processRecordsInput1.records().isEmpty());
        assertEquals(processRecordsInput1.millisBehindLatest(), new Long(1000));
        assertNotNull(processRecordsInput1.cacheEntryTime());

        ProcessRecordsInput processRecordsInput2 = getRecordsCache.getNextResult();

        assertNotEquals(processRecordsInput1, processRecordsInput2);
    }

    @Test
    public void testFullCache() {
        getRecordsCache.start(extendedSequenceNumber, initialPosition);
        sleep(MAX_SIZE * IDLE_MILLIS_BETWEEN_CALLS);

        assertEquals(getRecordsCache.getRecordsResultQueue.size(), MAX_SIZE);

        ProcessRecordsInput processRecordsInput1 = getRecordsCache.getNextResult();
        ProcessRecordsInput processRecordsInput2 = getRecordsCache.getNextResult();

        assertNotEquals(processRecordsInput1, processRecordsInput2);
    }

    @Ignore
    @Test
    public void testDifferentShardCaches() {
        final ExecutorService executorService2 = spy(Executors.newFixedThreadPool(1));
        final KinesisDataFetcher kinesisDataFetcher = spy(new KinesisDataFetcher(kinesisClient, streamName, shardId, MAX_RECORDS_PER_CALL, NULL_METRICS_FACTORY));
        final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy2 =
                spy(new AsynchronousGetRecordsRetrievalStrategy(kinesisDataFetcher, 5 , 5, shardId));
        final PrefetchRecordsPublisher recordsPublisher2 = new PrefetchRecordsPublisher(
                MAX_SIZE,
                MAX_BYTE_SIZE,
                MAX_RECORDS_COUNT,
                MAX_RECORDS_PER_CALL,
                getRecordsRetrievalStrategy2,
                executorService2,
                IDLE_MILLIS_BETWEEN_CALLS,
                new NullMetricsFactory(),
                operation,
                "test-shard-2");

        getRecordsCache.start(extendedSequenceNumber, initialPosition);
        sleep(IDLE_MILLIS_BETWEEN_CALLS);

        final Record record = mock(Record.class);
        final SdkBytes byteBuffer = SdkBytes.fromByteArray(new byte[512 * 1024]);
        when(record.data()).thenReturn(byteBuffer);

        records.add(record);
        records.add(record);
        records.add(record);
        records.add(record);
        recordsPublisher2.start(extendedSequenceNumber, initialPosition);

        sleep(IDLE_MILLIS_BETWEEN_CALLS);

        ProcessRecordsInput p1 = getRecordsCache.getNextResult();

        ProcessRecordsInput p2 = recordsPublisher2.getNextResult();

        assertNotEquals(p1, p2);
        assertTrue(p1.records().isEmpty());
        assertFalse(p2.records().isEmpty());
        assertEquals(p2.records().size(), records.size());

        recordsPublisher2.shutdown();
        sleep(100L);
        verify(executorService2).shutdownNow();
//        verify(getRecordsRetrievalStrategy2).shutdown();
    }

    @Test
    public void testExpiredIteratorException() {
        when(dataFetcher.getRecords()).thenAnswer(new Answer<DataFetcherResult>() {
            @Override
            public DataFetcherResult answer(final InvocationOnMock invocationOnMock) throws Throwable {
                throw ExpiredIteratorException.builder().message("ExpiredIterator").build();
            }
        }).thenCallRealMethod();
        doNothing().when(dataFetcher).restartIterator();

        getRecordsCache.start(extendedSequenceNumber, initialPosition);
        sleep(IDLE_MILLIS_BETWEEN_CALLS);

        ProcessRecordsInput processRecordsInput = getRecordsCache.getNextResult();

        assertNotNull(processRecordsInput);
        assertTrue(processRecordsInput.records().isEmpty());
        verify(dataFetcher).restartIterator();
    }

    @After
    public void shutdown() {
        getRecordsCache.shutdown();
        sleep(100L);
        verify(executorService).shutdownNow();
//        verify(getRecordsRetrievalStrategy).shutdown();
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {}
    }

    private class KinesisDataFetcherForTest extends KinesisDataFetcher {
        public KinesisDataFetcherForTest(final KinesisAsyncClient kinesisClient,
                                         final String streamName,
                                         final String shardId,
                                         final int maxRecords) {
            super(kinesisClient, streamName, shardId, maxRecords, NULL_METRICS_FACTORY);
        }

        @Override
        public DataFetcherResult getRecords() {
            GetRecordsResponse getRecordsResult = GetRecordsResponse.builder().records(new ArrayList<>(records)).millisBehindLatest(1000L).build();

            return new AdvancingResult(getRecordsResult);
        }
    }
}
