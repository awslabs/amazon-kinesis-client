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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.Validate;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.impl.ThreadSafeMetricsDelegatingFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;

import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;

/**
 * This is the prefetch caching class, this class spins up a thread if prefetching is enabled. That thread fetches the
 * next set of records and stores it in the cache. The size of the cache is limited by setting
 * maxPendingProcessRecordsInput i.e. the maximum number of GetRecordsResult that the cache can store, maxByteSize
 * i.e. the byte size of the records stored in the cache and maxRecordsCount i.e. the max number of records that should
 * be present in the cache across multiple GetRecordsResult object. If no data is available in the cache, the call from
 * the record processor is blocked till records are retrieved from Kinesis.
 */
@CommonsLog
public class PrefetchGetRecordsCache implements GetRecordsCache {
    private static final String EXPIRED_ITERATOR_METRIC = "ExpiredIterator";
    LinkedBlockingQueue<ProcessRecordsInput> getRecordsResultQueue;
    private int maxPendingProcessRecordsInput;
    private int maxByteSize;
    private int maxRecordsCount;
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    private final ExecutorService executorService;
    private final IMetricsFactory metricsFactory;
    private final long idleMillisBetweenCalls;
    private Instant lastSuccessfulCall;
    private final DefaultGetRecordsCacheDaemon defaultGetRecordsCacheDaemon;
    private PrefetchCounters prefetchCounters;
    private boolean started = false;
    private final String operation;
    private final KinesisDataFetcher dataFetcher;
    private final String shardId;

    /**
     * Constructor for the PrefetchGetRecordsCache. This cache prefetches records from Kinesis and stores them in a
     * LinkedBlockingQueue.
     * 
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.PrefetchGetRecordsCache
     * 
     * @param maxPendingProcessRecordsInput Max number of ProcessRecordsInput that can be held in the cache before
     *                                     blocking
     * @param maxByteSize Max byte size of the queue before blocking next get records call
     * @param maxRecordsCount Max number of records in the queue across all ProcessRecordInput objects
     * @param maxRecordsPerCall Max records to be returned per call
     * @param getRecordsRetrievalStrategy Retrieval strategy for the get records call
     * @param executorService Executor service for the cache
     * @param idleMillisBetweenCalls maximum time to wait before dispatching the next get records call
     */
    public PrefetchGetRecordsCache(final int maxPendingProcessRecordsInput, final int maxByteSize, final int maxRecordsCount,
                                   final int maxRecordsPerCall,
                                   @NonNull final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy,
                                   @NonNull final ExecutorService executorService,
                                   final long idleMillisBetweenCalls,
                                   @NonNull final IMetricsFactory metricsFactory,
                                   @NonNull final String operation,
                                   @NonNull final String shardId) {
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.maxPendingProcessRecordsInput = maxPendingProcessRecordsInput;
        this.maxByteSize = maxByteSize;
        this.maxRecordsCount = maxRecordsCount;
        this.getRecordsResultQueue = new LinkedBlockingQueue<>(this.maxPendingProcessRecordsInput);
        this.prefetchCounters = new PrefetchCounters();
        this.executorService = executorService;
        this.metricsFactory = new ThreadSafeMetricsDelegatingFactory(metricsFactory);
        this.idleMillisBetweenCalls = idleMillisBetweenCalls;
        this.defaultGetRecordsCacheDaemon = new DefaultGetRecordsCacheDaemon();
        Validate.notEmpty(operation, "Operation cannot be empty");
        this.operation = operation;
        this.dataFetcher = this.getRecordsRetrievalStrategy.getDataFetcher();
        this.shardId = shardId;
    }

    @Override
    public void start() {
        if (executorService.isShutdown()) {
            throw new IllegalStateException("ExecutorService has been shutdown.");
        }
        
        if (!started) {
            log.info("Starting prefetching thread.");
            executorService.execute(defaultGetRecordsCacheDaemon);
        }
        started = true;
    }

    @Override
    public ProcessRecordsInput getNextResult() {
        if (executorService.isShutdown()) {
            throw new IllegalStateException("Shutdown has been called on the cache, can't accept new requests.");
        }
        
        if (!started) {
            throw new IllegalStateException("Cache has not been initialized, make sure to call start.");
        }
        ProcessRecordsInput result = null;
        try {
            result = getRecordsResultQueue.take().withCacheExitTime(Instant.now());
            prefetchCounters.removed(result);
        } catch (InterruptedException e) {
            log.error("Interrupted while getting records from the cache", e);
        }
        return result;
    }

    @Override
    public GetRecordsRetrievalStrategy getGetRecordsRetrievalStrategy() {
        return getRecordsRetrievalStrategy;
    }

    @Override
    public void shutdown() {
        defaultGetRecordsCacheDaemon.isShutdown = true;
        executorService.shutdownNow();
        started = false;
    }

    private class DefaultGetRecordsCacheDaemon implements Runnable {
        volatile boolean isShutdown = false;
        
        @Override
        public void run() {
            while (!isShutdown) {
                if (Thread.currentThread().isInterrupted()) {
                    log.warn("Prefetch thread was interrupted.");
                    break;
                }
                MetricsHelper.startScope(metricsFactory, operation);
                if (prefetchCounters.shouldGetNewRecords()) {
                    try {
                        sleepBeforeNextCall();
                        GetRecordsResult getRecordsResult = getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
                        lastSuccessfulCall = Instant.now();
                        ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                                .withRecords(getRecordsResult.getRecords())
                                .withMillisBehindLatest(getRecordsResult.getMillisBehindLatest())
                                .withCacheEntryTime(lastSuccessfulCall);
                        getRecordsResultQueue.put(processRecordsInput);
                        prefetchCounters.added(processRecordsInput);
                    } catch (InterruptedException e) {
                        log.info("Thread was interrupted, indicating shutdown was called on the cache.");
                    } catch (ExpiredIteratorException e) {
                        log.info(String.format("ShardId %s: getRecords threw ExpiredIteratorException - restarting"
                                + " after greatest seqNum passed to customer", shardId), e);
                        
                        MetricsHelper.getMetricsScope().addData(EXPIRED_ITERATOR_METRIC, 1, StandardUnit.Count,
                                MetricsLevel.SUMMARY);
                        
                        dataFetcher.restartIterator();
                    } catch (SdkClientException e) {
                        log.error("Exception thrown while fetching records from Kinesis", e);
                    } catch (Throwable e) {
                        log.error("Unexpected exception was thrown. This could probably be an issue or a bug." +
                                " Please search for the exception/error online to check what is going on. If the " +
                                "issue persists or is a recurring problem, feel free to open an issue on, " +
                                "https://github.com/awslabs/amazon-kinesis-client.", e);
                    } finally {
                        MetricsHelper.endScope();
                    }
                } else {
                    //
                    // Consumer isn't ready to receive new records will allow prefetch counters to pause
                    //
                    try {
                        prefetchCounters.waitForConsumer();
                    } catch (InterruptedException ie) {
                        log.info("Thread was interrupted while waiting for the consumer.  " +
                                "Shutdown has probably been started");
                    }
                }
            }
            callShutdownOnStrategy();
        }
        
        private void callShutdownOnStrategy() {
            if (!getRecordsRetrievalStrategy.isShutdown()) {
                getRecordsRetrievalStrategy.shutdown();
            }
        }
        
        private void sleepBeforeNextCall() throws InterruptedException {
            if (lastSuccessfulCall == null) {
                return;
            }
            long timeSinceLastCall = Duration.between(lastSuccessfulCall, Instant.now()).abs().toMillis();
            if (timeSinceLastCall < idleMillisBetweenCalls) {
                Thread.sleep(idleMillisBetweenCalls - timeSinceLastCall);
            }
        }
    }

    private class PrefetchCounters {
        private long size = 0;
        private long byteSize = 0;

        public synchronized void added(final ProcessRecordsInput result) {
            size += getSize(result);
            byteSize += getByteSize(result);
        }

        public synchronized void removed(final ProcessRecordsInput result) {
            size -= getSize(result);
            byteSize -= getByteSize(result);
            this.notifyAll();
        }

        private long getSize(final ProcessRecordsInput result) {
            return result.getRecords().size();
        }

        private long getByteSize(final ProcessRecordsInput result) {
            return result.getRecords().stream().mapToLong(record -> record.getData().array().length).sum();
        }

        public synchronized void waitForConsumer() throws InterruptedException {
            if (!shouldGetNewRecords()) {
                log.debug("Queue is full waiting for consumer for " + idleMillisBetweenCalls + " ms");
                this.wait(idleMillisBetweenCalls);
            }
        }
        
        public synchronized boolean shouldGetNewRecords() {
            if (log.isDebugEnabled()) {
                log.debug("Current Prefetch Counter States: " + this.toString());
            }
            return size < maxRecordsCount && byteSize < maxByteSize;
        }

        @Override
        public String toString() {
            return String.format("{ Requests: %d, Records: %d, Bytes: %d }", getRecordsResultQueue.size(), size,
                    byteSize);
        }
    }

}
