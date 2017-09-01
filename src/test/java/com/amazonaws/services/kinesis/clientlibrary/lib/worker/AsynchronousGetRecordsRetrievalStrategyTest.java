package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.model.GetRecordsResult;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class AsynchronousGetRecordsRetrievalStrategyTest {

    private static final long RETRY_GET_RECORDS_IN_SECONDS = 5;
    private static final String SHARD_ID = "ShardId-0001";
    @Mock
    private KinesisDataFetcher dataFetcher;
    @Mock
    private ExecutorService executorService;
    @Mock
    private CompletionService<GetRecordsResult> completionService;
    @Mock
    private Future<GetRecordsResult> successfulFuture;
    @Mock
    private Future<GetRecordsResult> blockedFuture;
    @Mock
    private GetRecordsResult expectedResults;

    @Test
    public void testSingleSuccessfulRequestFuture() throws Exception {
        AsynchronousGetRecordsRetrievalStrategy strategy = new AsynchronousGetRecordsRetrievalStrategy(dataFetcher,
                executorService, (int) RETRY_GET_RECORDS_IN_SECONDS, completionService, SHARD_ID);

        when(executorService.isShutdown()).thenReturn(false);
        when(completionService.submit(any())).thenReturn(successfulFuture);
        when(completionService.poll(anyLong(), any())).thenReturn(successfulFuture);
        when(successfulFuture.get()).thenReturn(expectedResults);

        GetRecordsResult result = strategy.getRecords(10);

        verify(executorService).isShutdown();
        verify(completionService).submit(any());
        verify(completionService).poll(eq(RETRY_GET_RECORDS_IN_SECONDS), eq(TimeUnit.SECONDS));
        verify(successfulFuture).get();
        verify(successfulFuture).cancel(eq(true));
        verify(successfulFuture).isCancelled();
        verify(completionService, never()).take();

        assertThat(result, equalTo(expectedResults));
    }

    @Test
    public void testBlockedAndSuccessfulFuture() throws Exception {
        AsynchronousGetRecordsRetrievalStrategy strategy = new AsynchronousGetRecordsRetrievalStrategy(dataFetcher,
                executorService, (int) RETRY_GET_RECORDS_IN_SECONDS, completionService, SHARD_ID);

        when(executorService.isShutdown()).thenReturn(false);
        when(completionService.submit(any())).thenReturn(blockedFuture).thenReturn(successfulFuture);
        when(completionService.poll(anyLong(), any())).thenReturn(null).thenReturn(successfulFuture);
        when(successfulFuture.get()).thenReturn(expectedResults);
        when(successfulFuture.cancel(anyBoolean())).thenReturn(false);
        when(blockedFuture.cancel(anyBoolean())).thenReturn(true);
        when(successfulFuture.isCancelled()).thenReturn(false);
        when(blockedFuture.isCancelled()).thenReturn(true);

        GetRecordsResult actualResults = strategy.getRecords(10);

        verify(completionService, times(2)).submit(any());
        verify(completionService, times(2)).poll(eq(RETRY_GET_RECORDS_IN_SECONDS), eq(TimeUnit.SECONDS));
        verify(successfulFuture).get();
        verify(blockedFuture, never()).get();
        verify(successfulFuture).cancel(eq(true));
        verify(blockedFuture).cancel(eq(true));
        verify(successfulFuture).isCancelled();
        verify(blockedFuture).isCancelled();
        verify(completionService).take();

        assertThat(actualResults, equalTo(expectedResults));
    }

    @Test(expected = IllegalStateException.class)
    public void testStrategyIsShutdown() throws Exception {
        AsynchronousGetRecordsRetrievalStrategy strategy = new AsynchronousGetRecordsRetrievalStrategy(dataFetcher,
                executorService, (int) RETRY_GET_RECORDS_IN_SECONDS, completionService, SHARD_ID);

        when(executorService.isShutdown()).thenReturn(true);

        strategy.getRecords(10);
    }

    @Test
    public void testPoolOutOfResources() throws Exception {
        AsynchronousGetRecordsRetrievalStrategy strategy = new AsynchronousGetRecordsRetrievalStrategy(dataFetcher,
                executorService, (int) RETRY_GET_RECORDS_IN_SECONDS, completionService, SHARD_ID);

        when(executorService.isShutdown()).thenReturn(false);
        when(completionService.submit(any())).thenReturn(blockedFuture).thenThrow(new RejectedExecutionException("Rejected!")).thenReturn(successfulFuture);
        when(completionService.poll(anyLong(), any())).thenReturn(null).thenReturn(null).thenReturn(successfulFuture);
        when(successfulFuture.get()).thenReturn(expectedResults);
        when(successfulFuture.cancel(anyBoolean())).thenReturn(false);
        when(blockedFuture.cancel(anyBoolean())).thenReturn(true);
        when(successfulFuture.isCancelled()).thenReturn(false);
        when(blockedFuture.isCancelled()).thenReturn(true);

        GetRecordsResult actualResult = strategy.getRecords(10);

        verify(completionService, times(3)).submit(any());
        verify(completionService, times(3)).poll(eq(RETRY_GET_RECORDS_IN_SECONDS), eq(TimeUnit.SECONDS));
        verify(successfulFuture).cancel(eq(true));
        verify(blockedFuture).cancel(eq(true));
        verify(successfulFuture).isCancelled();
        verify(blockedFuture).isCancelled();
        verify(completionService).take();

        assertThat(actualResult, equalTo(expectedResults));
    }

}
