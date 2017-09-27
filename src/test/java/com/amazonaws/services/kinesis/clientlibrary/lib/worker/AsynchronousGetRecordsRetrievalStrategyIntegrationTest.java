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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@RunWith(MockitoJUnitRunner.class)
public class AsynchronousGetRecordsRetrievalStrategyIntegrationTest {

    private static final int CORE_POOL_SIZE = 1;
    private static final int MAX_POOL_SIZE = 2;
    private static final int TIME_TO_LIVE = 5;
    private static final int RETRY_GET_RECORDS_IN_SECONDS = 2;
    private static final int SLEEP_GET_RECORDS_IN_SECONDS = 10;

    @Mock
    private IKinesisProxy mockKinesisProxy;
    @Mock
    private ShardInfo mockShardInfo;
    @Mock
    private Supplier<CompletionService<DataFetcherResult>> completionServiceSupplier;
    @Mock
    private DataFetcherResult result;
    @Mock
    private GetRecordsResult recordsResult;

    private CompletionService<DataFetcherResult> completionService;

    private AsynchronousGetRecordsRetrievalStrategy getRecordsRetrivalStrategy;
    private KinesisDataFetcher dataFetcher;
    private ExecutorService executorService;
    private RejectedExecutionHandler rejectedExecutionHandler;
    private int numberOfRecords = 10;


    @Before
    public void setup() {
        dataFetcher = spy(new KinesisDataFetcherForTests(mockKinesisProxy, mockShardInfo));
        rejectedExecutionHandler = spy(new ThreadPoolExecutor.AbortPolicy());
        executorService = spy(new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                TIME_TO_LIVE,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("getrecords-worker-%d").build(),
                rejectedExecutionHandler));
        completionService = spy(new ExecutorCompletionService<DataFetcherResult>(executorService));
        when(completionServiceSupplier.get()).thenReturn(completionService);
        getRecordsRetrivalStrategy = new AsynchronousGetRecordsRetrievalStrategy(dataFetcher, executorService, RETRY_GET_RECORDS_IN_SECONDS, completionServiceSupplier, "shardId-0001");
        when(result.accept()).thenReturn(recordsResult);
    }

    @Test
    public void oneRequestMultithreadTest() {
        when(result.accept()).thenReturn(null);
        GetRecordsResult getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        verify(dataFetcher, atLeast(getLeastNumberOfCalls())).getRecords(eq(numberOfRecords));
        verify(executorService, atLeast(getLeastNumberOfCalls())).execute(any());
        assertNull(getRecordsResult);
    }

    @Test
    public void multiRequestTest() {
        ExecutorCompletionService<DataFetcherResult> completionService1 = spy(new ExecutorCompletionService<DataFetcherResult>(executorService));
        when(completionServiceSupplier.get()).thenReturn(completionService1);
        GetRecordsResult getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        verify(dataFetcher, atLeast(getLeastNumberOfCalls())).getRecords(numberOfRecords);
        verify(executorService, atLeast(getLeastNumberOfCalls())).execute(any());
        assertThat(getRecordsResult, equalTo(recordsResult));

        when(result.accept()).thenReturn(null);
        ExecutorCompletionService<DataFetcherResult> completionService2 = spy(new ExecutorCompletionService<DataFetcherResult>(executorService));
        when(completionServiceSupplier.get()).thenReturn(completionService2);
        getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        assertThat(getRecordsResult, nullValue(GetRecordsResult.class));
    }

    @Test
    @Ignore
    public void testInterrupted() throws InterruptedException, ExecutionException {
        Future<DataFetcherResult> mockFuture = mock(Future.class);
        when(completionService.submit(any())).thenReturn(mockFuture);
        when(completionService.poll()).thenReturn(mockFuture);
        doThrow(InterruptedException.class).when(mockFuture).get();
        GetRecordsResult getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        verify(mockFuture).get();
        assertNull(getRecordsResult);
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
        public KinesisDataFetcherForTests(final IKinesisProxy kinesisProxy, final ShardInfo shardInfo) {
            super(kinesisProxy, shardInfo);
        }

        @Override
        public DataFetcherResult getRecords(final int maxRecords) {
            try {
                Thread.sleep(SLEEP_GET_RECORDS_IN_SECONDS * 1000);
            } catch (InterruptedException e) {
                // Do nothing
            }
            return result;
        }
    }

}
