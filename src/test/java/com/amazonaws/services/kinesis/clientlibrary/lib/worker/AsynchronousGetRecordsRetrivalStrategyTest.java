package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class AsynchronousGetRecordsRetrivalStrategyTest {
    private static final int CORE_POOL_SIZE = 1;
    private static final int MAX_POOL_SIZE = 2;
    private static final int TIME_TO_LIVE = 5;
    private static final int RETRY_GET_RECORDS_IN_SECONDS = 2;
    private static final int SLEEP_GET_RECORDS_IN_SECONDS = 10;

    @Mock
    private IKinesisProxy mockKinesisProxy;

    @Mock
    private ShardInfo mockShardInfo;

    private AsynchronousGetRecordsRetrivalStrategy getRecordsRetrivalStrategy;
    private KinesisDataFetcher dataFetcher;
    private GetRecordsResult result;
    private ExecutorService executorService;
    private RejectedExecutionHandler rejectedExecutionHandler;
    private int numberOfRecords = 10;
    private CompletionService<GetRecordsResult> completionService;

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
        getRecordsRetrivalStrategy = new AsynchronousGetRecordsRetrivalStrategy(dataFetcher, executorService, RETRY_GET_RECORDS_IN_SECONDS);
        completionService = spy(getRecordsRetrivalStrategy.getCompletionService());
        result = null;
    }

    @Test
    public void oneRequestMultithreadTest() {
        GetRecordsResult getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        verify(dataFetcher, atLeast(getLeastNumberOfCalls())).getRecords(eq(numberOfRecords));
        verify(executorService, atLeast(getLeastNumberOfCalls())).execute(any());
        assertNull(getRecordsResult);
    }

    @Test
    public void multiRequestTest() {
        result = mock(GetRecordsResult.class);

        GetRecordsResult getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        verify(dataFetcher, atLeast(getLeastNumberOfCalls())).getRecords(numberOfRecords);
        verify(executorService, atLeast(getLeastNumberOfCalls())).execute(any());
        assertEquals(result, getRecordsResult);

        result = null;
        getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        assertNull(getRecordsResult);
    }
    
    /*@Test
    public void testInterrupted() throws InterruptedException, ExecutionException {
        Future<GetRecordsResult> mockFuture = mock(Future.class);
        System.out.println(completionService);
        when(completionService.submit(any())).thenReturn(mockFuture);
        when(completionService.poll()).thenReturn(mockFuture);
        doThrow(InterruptedException.class).when(mockFuture).get();
        GetRecordsResult getRecordsResult = getRecordsRetrivalStrategy.getRecords(numberOfRecords);
        verify(mockFuture).get();
        assertNull(getRecordsResult);
    }*/

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
        public GetRecordsResult getRecords(final int maxRecords) {
            try {
                Thread.sleep(SLEEP_GET_RECORDS_IN_SECONDS * 1000);
            } catch (InterruptedException e) {
                // Do nothing
            }
            return result;
        }
    }
}
