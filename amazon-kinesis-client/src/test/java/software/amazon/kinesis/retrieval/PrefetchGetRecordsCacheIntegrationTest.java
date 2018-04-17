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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.lifecycle.ProcessRecordsInput;
import software.amazon.kinesis.metrics.NullMetricsFactory;

/**
 * These are the integration tests for the PrefetchGetRecordsCache class. 
 */
@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class PrefetchGetRecordsCacheIntegrationTest {
    private static final int MAX_SIZE = 3;
    private static final int MAX_BYTE_SIZE = 5 * 1024 * 1024;
    private static final int MAX_RECORDS_COUNT = 30_000;
    private static final int MAX_RECORDS_PER_CALL = 10_000;
    private static final long IDLE_MILLIS_BETWEEN_CALLS = 500L;

    private PrefetchGetRecordsCache getRecordsCache;
    private GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    private KinesisDataFetcher dataFetcher;
    private ExecutorService executorService;
    private List<Record> records;
    private String operation = "ProcessTask";
    private String streamName = "streamName";
    private String shardId = "shardId-000000000000";

    @Mock
    private AmazonKinesis amazonKinesis;

    @Before
    public void setup() {
        records = new ArrayList<>();
        dataFetcher = spy(new KinesisDataFetcherForTest(amazonKinesis, streamName, shardId, MAX_RECORDS_PER_CALL));
        getRecordsRetrievalStrategy = spy(new SynchronousGetRecordsRetrievalStrategy(dataFetcher));
        executorService = spy(Executors.newFixedThreadPool(1));
        getRecordsCache = new PrefetchGetRecordsCache(MAX_SIZE,
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
        getRecordsCache.start();
        sleep(IDLE_MILLIS_BETWEEN_CALLS);

        ProcessRecordsInput processRecordsInput1 = getRecordsCache.getNextResult();

        assertTrue(processRecordsInput1.getRecords().isEmpty());
        assertEquals(processRecordsInput1.getMillisBehindLatest(), new Long(1000));
        assertNotNull(processRecordsInput1.getCacheEntryTime());

        ProcessRecordsInput processRecordsInput2 = getRecordsCache.getNextResult();

        assertNotEquals(processRecordsInput1, processRecordsInput2);
    }

    @Test
    public void testFullCache() {
        getRecordsCache.start();
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
        final KinesisDataFetcher kinesisDataFetcher = spy(new KinesisDataFetcher(amazonKinesis, streamName, shardId, MAX_RECORDS_PER_CALL));
        final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy2 =
                spy(new AsynchronousGetRecordsRetrievalStrategy(kinesisDataFetcher, 5 , 5, shardId));
        final GetRecordsCache getRecordsCache2 = new PrefetchGetRecordsCache(
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

        getRecordsCache.start();
        sleep(IDLE_MILLIS_BETWEEN_CALLS);

        final Record record = mock(Record.class);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(512 * 1024);
        when(record.getData()).thenReturn(byteBuffer);

        records.add(record);
        records.add(record);
        records.add(record);
        records.add(record);
        getRecordsCache2.start();

        sleep(IDLE_MILLIS_BETWEEN_CALLS);

        ProcessRecordsInput p1 = getRecordsCache.getNextResult();

        ProcessRecordsInput p2 = getRecordsCache2.getNextResult();

        assertNotEquals(p1, p2);
        assertTrue(p1.getRecords().isEmpty());
        assertFalse(p2.getRecords().isEmpty());
        assertEquals(p2.getRecords().size(), records.size());

        getRecordsCache2.shutdown();
        sleep(100L);
        verify(executorService2).shutdownNow();
//        verify(getRecordsRetrievalStrategy2).shutdown();
    }

    @Test
    public void testExpiredIteratorException() {
        when(dataFetcher.getRecords()).thenAnswer(new Answer<DataFetcherResult>() {
            @Override
            public DataFetcherResult answer(final InvocationOnMock invocationOnMock) throws Throwable {
                throw new ExpiredIteratorException("ExpiredIterator");
            }
        }).thenCallRealMethod();
        doNothing().when(dataFetcher).restartIterator();

        getRecordsCache.start();
        sleep(IDLE_MILLIS_BETWEEN_CALLS);

        ProcessRecordsInput processRecordsInput = getRecordsCache.getNextResult();

        assertNotNull(processRecordsInput);
        assertTrue(processRecordsInput.getRecords().isEmpty());
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
        public KinesisDataFetcherForTest(final AmazonKinesis amazonKinesis,
                                         final String streamName,
                                         final String shardId,
                                         final int maxRecords) {
            super(amazonKinesis, streamName, shardId, maxRecords);
        }

        @Override
        public DataFetcherResult getRecords() {
            GetRecordsResult getRecordsResult = new GetRecordsResult();
            getRecordsResult.setRecords(new ArrayList<>(records));
            getRecordsResult.setMillisBehindLatest(1000L);

            return new AdvancingResult(getRecordsResult);
        }
    }
}
