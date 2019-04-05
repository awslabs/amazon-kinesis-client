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
import java.util.function.Supplier;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;

/**
 *
 */
@Slf4j
@KinesisClientInternalApi
public class AsynchronousGetRecordsRetrievalStrategy implements GetRecordsRetrievalStrategy {
    private static final int TIME_TO_KEEP_ALIVE = 5;
    private static final int CORE_THREAD_POOL_COUNT = 1;

    private final KinesisDataFetcher dataFetcher;
    private final ExecutorService executorService;
    private final int retryGetRecordsInSeconds;
    private final String shardId;
    final Supplier<CompletionService<DataFetcherResult>> completionServiceSupplier;

    public AsynchronousGetRecordsRetrievalStrategy(@NonNull final KinesisDataFetcher dataFetcher,
            final int retryGetRecordsInSeconds, final int maxGetRecordsThreadPool, String shardId) {
        this(dataFetcher, buildExector(maxGetRecordsThreadPool, shardId), retryGetRecordsInSeconds, shardId);
    }

    public AsynchronousGetRecordsRetrievalStrategy(final KinesisDataFetcher dataFetcher,
            final ExecutorService executorService, final int retryGetRecordsInSeconds, String shardId) {
        this(dataFetcher, executorService, retryGetRecordsInSeconds, () -> new ExecutorCompletionService<>(executorService),
                shardId);
    }

    AsynchronousGetRecordsRetrievalStrategy(KinesisDataFetcher dataFetcher, ExecutorService executorService,
            int retryGetRecordsInSeconds, Supplier<CompletionService<DataFetcherResult>> completionServiceSupplier,
            String shardId) {
        this.dataFetcher = dataFetcher;
        this.executorService = executorService;
        this.retryGetRecordsInSeconds = retryGetRecordsInSeconds;
        this.completionServiceSupplier = completionServiceSupplier;
        this.shardId = shardId;
    }

    @Override
    public GetRecordsResponse getRecords(final int maxRecords) {
        if (executorService.isShutdown()) {
            throw new IllegalStateException("Strategy has been shutdown");
        }
        GetRecordsResponse result = null;
        CompletionService<DataFetcherResult> completionService = completionServiceSupplier.get();
        Set<Future<DataFetcherResult>> futures = new HashSet<>();
        Callable<DataFetcherResult> retrieverCall = createRetrieverCallable();
        try {
            while (true) {
                try {
                    futures.add(completionService.submit(retrieverCall));
                } catch (RejectedExecutionException e) {
                    log.warn("Out of resources, unable to start additional requests.");
                }

                try {
                    Future<DataFetcherResult> resultFuture = completionService.poll(retryGetRecordsInSeconds,
                            TimeUnit.SECONDS);
                    if (resultFuture != null) {
                        //
                        // Fix to ensure that we only let the shard iterator advance when we intend to return the result
                        // to the caller.  This ensures that the shard iterator is consistently advance in step with
                        // what the caller sees.
                        //
                        result = resultFuture.get().accept();
                        break;
                    }
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof ExpiredIteratorException) {
                        throw (ExpiredIteratorException) e.getCause();
                    }
                    log.error("ExecutionException thrown while trying to get records", e);
                } catch (InterruptedException e) {
                    log.error("Thread was interrupted", e);
                    break;
                }
            }
        } finally {
            futures.forEach(f -> f.cancel(true));
        }
        return result;
    }

    private Callable<DataFetcherResult> createRetrieverCallable() {
        return dataFetcher::getRecords;
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

    @Override
    public KinesisDataFetcher getDataFetcher() {
        return dataFetcher;
    }
}
