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

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.DataFetcherResult;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AsynchronousGetRecordsRetrievalStrategyIntegrationTest {
    private static final int CORE_POOL_SIZE = 1;
    private static final int MAX_POOL_SIZE = 2;
    private static final int TIME_TO_LIVE = 5;
    private static final int RETRY_GET_RECORDS_IN_SECONDS = 2;
    private static final int SLEEP_GET_RECORDS_IN_SECONDS = 10;
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    private final String streamName = "testStream";
    private final String shardId = "shardId-000000000000";

    @Mock
    private Supplier<CompletionService<DataFetcherResult>> completionServiceSupplier;

    @Mock
    private DataFetcherResult result;

    @Mock
    private KinesisAsyncClient kinesisClient;

    private CompletionService<DataFetcherResult> completionService;
    private GetRecordsResponse getRecordsResponse;

    private AsynchronousGetRecordsRetrievalStrategy getRecordsRetrivalStrategy;
    private KinesisDataFetcher dataFetcher;
    private ExecutorService executorService;
    private RejectedExecutionHandler rejectedExecutionHandler;
    private int numberOfRecords = 10;

    @Before
    public void setup() {
        dataFetcher = spy(new KinesisDataFetcherForTests(kinesisClient, streamName, shardId, numberOfRecords));
        rejectedExecutionHandler = spy(new ThreadPoolExecutor.AbortPolicy());
        executorService = spy(new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                TIME_TO_LIVE,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1),
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("getrecords-worker-%d")
                        .build(),
                rejectedExecutionHandler));
        completionService = spy(new ExecutorCompletionService<DataFetcherResult>(executorService));
        getRecordsRetrivalStrategy = new AsynchronousGetRecordsRetrievalStrategy(
                dataFetcher, executorService, RETRY_GET_RECORDS_IN_SECONDS, completionServiceSupplier, "shardId-0001");
        getRecordsResponse = GetRecordsResponse.builder().build();

        when(completionServiceSupplier.get()).thenReturn(completionService);
        when(result.accept()).thenReturn(getRecordsResponse);
    }

    @Test
    public void oneRequestMultithreadTest() {
        when(result.accept()).thenReturn(null);
        GetRecordsResponse getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        verify(dataFetcher, atLeast(getLeastNumberOfCalls())).getRecords();
        verify(executorService, atLeast(getLeastNumberOfCalls())).execute(any());
        assertNull(getRecordsResult);
    }

    @Test
    public void multiRequestTest() {
        ExecutorCompletionService<DataFetcherResult> completionService1 =
                spy(new ExecutorCompletionService<DataFetcherResult>(executorService));
        when(completionServiceSupplier.get()).thenReturn(completionService1);
        GetRecordsResponse getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        verify(dataFetcher, atLeast(getLeastNumberOfCalls())).getRecords();
        verify(executorService, atLeast(getLeastNumberOfCalls())).execute(any());
        assertThat(getRecordsResult, equalTo(getRecordsResponse));

        when(result.accept()).thenReturn(null);
        ExecutorCompletionService<DataFetcherResult> completionService2 =
                spy(new ExecutorCompletionService<DataFetcherResult>(executorService));
        when(completionServiceSupplier.get()).thenReturn(completionService2);
        getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        assertThat(getRecordsResult, nullValue(GetRecordsResponse.class));
    }

    @Test(expected = ExpiredIteratorException.class)
    public void testExpiredIteratorExcpetion() throws InterruptedException {
        when(dataFetcher.getRecords()).thenAnswer(new Answer<DataFetcherResult>() {
            @Override
            public DataFetcherResult answer(final InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(SLEEP_GET_RECORDS_IN_SECONDS * 1000);
                throw ExpiredIteratorException.builder()
                        .message("ExpiredIterator")
                        .build();
            }
        });

        try {
            getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        } finally {
            verify(dataFetcher, atLeast(getLeastNumberOfCalls())).getRecords();
            verify(executorService, atLeast(getLeastNumberOfCalls())).execute(any());
        }
    }

    private int getLeastNumberOfCalls() {
        int leastNumberOfCalls = 0;
        for (int i = MAX_POOL_SIZE; i > 0; i--) {
            if (i * RETRY_GET_RECORDS_IN_SECONDS <= SLEEP_GET_RECORDS_IN_SECONDS) {
                leastNumberOfCalls = i;
                break;
            }
        }
        return leastNumberOfCalls;
    }

    @After
    public void shutdown() {
        getRecordsRetrivalStrategy.shutdown();
        verify(executorService).shutdownNow();
    }

    private class KinesisDataFetcherForTests extends KinesisDataFetcher {
        public KinesisDataFetcherForTests(
                final KinesisAsyncClient kinesisClient,
                final String streamName,
                final String shardId,
                final int maxRecords) {
            super(kinesisClient, streamName, shardId, maxRecords, NULL_METRICS_FACTORY);
        }

        @Override
        public DataFetcherResult getRecords() {
            try {
                Thread.sleep(SLEEP_GET_RECORDS_IN_SECONDS * 1000);
            } catch (InterruptedException e) {
                // Do nothing
            }

            return result;
        }
    }
}
