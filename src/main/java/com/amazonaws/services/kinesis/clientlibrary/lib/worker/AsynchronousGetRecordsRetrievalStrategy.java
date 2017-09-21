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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.impl.ThreadSafeMetricsDelegatingScope;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;

/**
 *
 */
@CommonsLog
public class AsynchronousGetRecordsRetrievalStrategy implements GetRecordsRetrievalStrategy {
    private static final int TIME_TO_KEEP_ALIVE = 5;
    private static final int CORE_THREAD_POOL_COUNT = 1;

    private final KinesisDataFetcher dataFetcher;
    private final ExecutorService executorService;
    private final int retryGetRecordsInSeconds;
    private final String shardId;
    final CompletionService<GetRecordsResult> completionService;

    public AsynchronousGetRecordsRetrievalStrategy(@NonNull final KinesisDataFetcher dataFetcher,
            final int retryGetRecordsInSeconds, final int maxGetRecordsThreadPool, String shardId) {
        this(dataFetcher, buildExector(maxGetRecordsThreadPool, shardId), retryGetRecordsInSeconds, shardId);
    }

    public AsynchronousGetRecordsRetrievalStrategy(final KinesisDataFetcher dataFetcher,
            final ExecutorService executorService, final int retryGetRecordsInSeconds, String shardId) {
        this(dataFetcher, executorService, retryGetRecordsInSeconds, new ExecutorCompletionService<>(executorService),
                shardId);
    }

    AsynchronousGetRecordsRetrievalStrategy(KinesisDataFetcher dataFetcher, ExecutorService executorService,
            int retryGetRecordsInSeconds, CompletionService<GetRecordsResult> completionService, String shardId) {
        this.dataFetcher = dataFetcher;
        this.executorService = executorService;
        this.retryGetRecordsInSeconds = retryGetRecordsInSeconds;
        this.completionService = completionService;
        this.shardId = shardId;
    }

    @Override
    public GetRecordsResult getRecords(final int maxRecords) {
        if (executorService.isShutdown()) {
            throw new IllegalStateException("Strategy has been shutdown");
        }
        GetRecordsResult result = null;
        Set<Future<GetRecordsResult>> futures = new HashSet<>();
        Callable<GetRecordsResult> retrieverCall = createRetrieverCallable(maxRecords);
        while (true) {
            try {
                futures.add(completionService.submit(retrieverCall));
            } catch (RejectedExecutionException e) {
                log.warn("Out of resources, unable to start additional requests.");
            }

            try {
                Future<GetRecordsResult> resultFuture = completionService.poll(retryGetRecordsInSeconds,
                        TimeUnit.SECONDS);
                if (resultFuture != null) {
                    result = resultFuture.get();
                    break;
                }
            } catch (ExecutionException e) {
                log.error("ExecutionException thrown while trying to get records", e);
            } catch (InterruptedException e) {
                log.error("Thread was interrupted", e);
                break;
            }
        }
        futures.stream().peek(f -> f.cancel(true)).filter(Future::isCancelled).forEach(f -> {
            try {
                completionService.take();
            } catch (InterruptedException e) {
                log.error("Exception thrown while trying to empty the threadpool.");
            }
        });
        return result;
    }

    private Callable<GetRecordsResult> createRetrieverCallable(int maxRecords) {
        ThreadSafeMetricsDelegatingScope metricsScope = new ThreadSafeMetricsDelegatingScope(MetricsHelper.getMetricsScope());
        return () -> {
            try {
                MetricsHelper.setMetricsScope(metricsScope);
                return dataFetcher.getRecords(maxRecords);
            } finally {
                MetricsHelper.unsetMetricsScope();
            }
        };
    }

    @Override
    public void shutdown() {
        executorService.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executorService.isShutdown();
    }

    private static ExecutorService buildExector(int maxGetRecordsThreadPool, String shardId) {
        String threadNameFormat = "get-records-worker-" + shardId + "-%d";
        return new ThreadPoolExecutor(CORE_THREAD_POOL_COUNT, maxGetRecordsThreadPool, TIME_TO_KEEP_ALIVE,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(1),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadNameFormat).build(),
                new ThreadPoolExecutor.AbortPolicy());
    }
}
