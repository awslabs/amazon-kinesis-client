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

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.GetRecordsResult;

import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;

/**
 * This is the default caching class, this class spins up a thread if prefetching is enabled. That thread fetches the
 * next set of records and stores it in the cache. The size of the cache is limited by setting maxSize i.e. the maximum
 * number of GetRecordsResult that the cache can store, maxByteSize i.e. the byte size of the records stored in the
 * cache and maxRecordsCount i.e. the max number of records that should be present in the cache across multiple
 * GetRecordsResult object. If no data is available in the cache, the call from the record processor is blocked till
 * records are retrieved from Kinesis.
 */
@CommonsLog
public class PrefetchGetRecordsCache implements GetRecordsCache {
    LinkedBlockingQueue<ProcessRecordsInput> getRecordsResultQueue;
    private int maxSize;
    private int maxByteSize;
    private int maxRecordsCount;
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    private final ExecutorService executorService;

    private PrefetchCounters prefetchCounters;

    private boolean started = false;

    public PrefetchGetRecordsCache(final int maxSize, final int maxByteSize, final int maxRecordsCount,
                                   final int maxRecordsPerCall,
                                   @NonNull final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy,
                                   @NonNull final ExecutorService executorService) {
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.maxSize = maxSize;
        this.maxByteSize = maxByteSize;
        this.maxRecordsCount = maxRecordsCount;
        this.getRecordsResultQueue = new LinkedBlockingQueue<>(this.maxSize);
        this.prefetchCounters = new PrefetchCounters();
        this.executorService = executorService;
    }

    @Override
    public void start() {
        if (!started) {
            log.info("Starting prefetching thread.");
            executorService.execute(new DefaultGetRecordsCacheDaemon());
        }
        started = true;
    }

    @Override
    public ProcessRecordsInput getNextResult() {
        if (!started) {
            throw new IllegalStateException("Threadpool in the cache was not started, make sure to call start on the cache");
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
    public void shutdown() {
        executorService.shutdownNow();
    }

    private class DefaultGetRecordsCacheDaemon implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (Thread.interrupted()) {
                    log.warn("Prefetch thread was interrupted.");
                    break;
                }
                if (prefetchCounters.shouldGetNewRecords()) {
                    try {
                        GetRecordsResult getRecordsResult = getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
                        ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                                .withRecords(getRecordsResult.getRecords())
                                .withMillisBehindLatest(getRecordsResult.getMillisBehindLatest())
                                .withCacheEntryTime(Instant.now());
                        getRecordsResultQueue.put(processRecordsInput);
                        prefetchCounters.added(processRecordsInput);
                    } catch (InterruptedException e) {
                        log.info("Thread was interrupted, indicating shutdown was called on the cache", e);
                    }
                }
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
        }

        private long getSize(final ProcessRecordsInput result) {
            return result.getRecords().size();
        }

        private long getByteSize(final ProcessRecordsInput result) {
            return result.getRecords().stream().mapToLong(record -> record.getData().array().length).sum();
        }
        
        public synchronized boolean shouldGetNewRecords() {
            return size < maxRecordsCount && byteSize < maxByteSize;
        }
    }

}
