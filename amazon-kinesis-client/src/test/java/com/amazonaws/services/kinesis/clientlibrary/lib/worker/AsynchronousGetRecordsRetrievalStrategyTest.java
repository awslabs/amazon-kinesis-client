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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
    private Supplier<CompletionService<DataFetcherResult>> completionServiceSupplier;
    @Mock
    private CompletionService<DataFetcherResult> completionService;
    @Mock
    private Future<DataFetcherResult> successfulFuture;
    @Mock
    private Future<DataFetcherResult> blockedFuture;
    @Mock
    private DataFetcherResult dataFetcherResult;
    @Mock
    private GetRecordsResult expectedResults;

    @Before
    public void before() {
        when(completionServiceSupplier.get()).thenReturn(completionService);
        when(dataFetcherResult.getResult()).thenReturn(expectedResults);
        when(dataFetcherResult.accept()).thenReturn(expectedResults);
    }

    @Test
    public void testSingleSuccessfulRequestFuture() throws Exception {
        AsynchronousGetRecordsRetrievalStrategy strategy = new AsynchronousGetRecordsRetrievalStrategy(dataFetcher,
                executorService, (int) RETRY_GET_RECORDS_IN_SECONDS, completionServiceSupplier, SHARD_ID);

        when(executorService.isShutdown()).thenReturn(false);
        when(completionService.submit(any())).thenReturn(successfulFuture);
        when(completionService.poll(anyLong(), any())).thenReturn(successfulFuture);
        when(successfulFuture.get()).thenReturn(dataFetcherResult);

        GetRecordsResult result = strategy.getRecords(10);

        verify(executorService).isShutdown();
        verify(completionService).submit(any());
        verify(completionService).poll(eq(RETRY_GET_RECORDS_IN_SECONDS), eq(TimeUnit.SECONDS));
        verify(successfulFuture).get();
        verify(successfulFuture).cancel(eq(true));

        assertThat(result, equalTo(expectedResults));
    }

    @Test
    public void testBlockedAndSuccessfulFuture() throws Exception {
        AsynchronousGetRecordsRetrievalStrategy strategy = new AsynchronousGetRecordsRetrievalStrategy(dataFetcher,
                executorService, (int) RETRY_GET_RECORDS_IN_SECONDS, completionServiceSupplier, SHARD_ID);

        when(executorService.isShutdown()).thenReturn(false);
        when(completionService.submit(any())).thenReturn(blockedFuture).thenReturn(successfulFuture);
        when(completionService.poll(anyLong(), any())).thenReturn(null).thenReturn(successfulFuture);
        when(successfulFuture.get()).thenReturn(dataFetcherResult);
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

        assertThat(actualResults, equalTo(expectedResults));
    }

    @Test(expected = IllegalStateException.class)
    public void testStrategyIsShutdown() throws Exception {
        AsynchronousGetRecordsRetrievalStrategy strategy = new AsynchronousGetRecordsRetrievalStrategy(dataFetcher,
                executorService, (int) RETRY_GET_RECORDS_IN_SECONDS, completionServiceSupplier, SHARD_ID);

        when(executorService.isShutdown()).thenReturn(true);

        strategy.getRecords(10);
    }

    @Test
    public void testPoolOutOfResources() throws Exception {
        AsynchronousGetRecordsRetrievalStrategy strategy = new AsynchronousGetRecordsRetrievalStrategy(dataFetcher,
                executorService, (int) RETRY_GET_RECORDS_IN_SECONDS, completionServiceSupplier, SHARD_ID);

        when(executorService.isShutdown()).thenReturn(false);
        when(completionService.submit(any())).thenReturn(blockedFuture).thenThrow(new RejectedExecutionException("Rejected!")).thenReturn(successfulFuture);
        when(completionService.poll(anyLong(), any())).thenReturn(null).thenReturn(null).thenReturn(successfulFuture);
        when(successfulFuture.get()).thenReturn(dataFetcherResult);
        when(successfulFuture.cancel(anyBoolean())).thenReturn(false);
        when(blockedFuture.cancel(anyBoolean())).thenReturn(true);
        when(successfulFuture.isCancelled()).thenReturn(false);
        when(blockedFuture.isCancelled()).thenReturn(true);

        GetRecordsResult actualResult = strategy.getRecords(10);

        verify(completionService, times(3)).submit(any());
        verify(completionService, times(3)).poll(eq(RETRY_GET_RECORDS_IN_SECONDS), eq(TimeUnit.SECONDS));
        verify(successfulFuture).cancel(eq(true));
        verify(blockedFuture).cancel(eq(true));


        assertThat(actualResult, equalTo(expectedResults));
    }
    
    @Test (expected = ExpiredIteratorException.class)
    public void testExpiredIteratorExceptionCase() throws Exception {
        AsynchronousGetRecordsRetrievalStrategy strategy = new AsynchronousGetRecordsRetrievalStrategy(dataFetcher,
                executorService, (int) RETRY_GET_RECORDS_IN_SECONDS, completionServiceSupplier, SHARD_ID);
        Future<DataFetcherResult> successfulFuture2 = mock(Future.class);

        when(executorService.isShutdown()).thenReturn(false);
        when(completionService.submit(any())).thenReturn(successfulFuture, successfulFuture2);
        when(completionService.poll(anyLong(), any())).thenReturn(null).thenReturn(successfulFuture);
        when(successfulFuture.get()).thenThrow(new ExecutionException(new ExpiredIteratorException("ExpiredException")));
        
        try {
            strategy.getRecords(10);
        } finally {
            verify(executorService).isShutdown();
            verify(completionService, times(2)).submit(any());
            verify(completionService, times(2)).poll(eq(RETRY_GET_RECORDS_IN_SECONDS), eq(TimeUnit.SECONDS));
            verify(successfulFuture).cancel(eq(true));
            verify(successfulFuture2).cancel(eq(true));
        }
    }

}
