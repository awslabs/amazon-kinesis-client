package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

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
    private LinkedBlockingQueue<GetRecordsResult> getRecordsResultQueue;
    private int maxSize;
    private int maxByteSize;
    private int maxRecordsCount;
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    private final ExecutorService executorService;
    
    private PrefetchCounters prefetchCounters;
    
    private boolean started = false;

    public PrefetchGetRecordsCache(final int maxSize, final int maxByteSize, final int maxRecordsCount,
                                   final int maxRecordsPerCall, @NonNull final DataFetchingStrategy dataFetchingStrategy,
                                   @NonNull final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy,
                                   @NonNull final ExecutorService executorService) {
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.maxSize = maxSize;
        this.maxByteSize = maxByteSize;
        this.maxRecordsCount = maxRecordsCount;
        this.getRecordsResultQueue = new LinkedBlockingQueue<>(this.maxSize);
        prefetchCounters = new PrefetchCounters();
        this.executorService = executorService;
    }
    
    private void start() {
        if (!started) {
            log.info("Starting prefetching thread.");
            executorService.execute(new DefaultGetRecordsCacheDaemon());
        }
        started = true;
    }

    @Override
    public GetRecordsResult getNextResult() {
        if (!started) {
            start();
        }
        GetRecordsResult result = null;
        try {
            result = getRecordsResultQueue.take();
            prefetchCounters.removed(result);
        } catch (InterruptedException e) {
            log.error("Interrupted while getting records from the cache", e);
        }
        return result;
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }
    
    private class DefaultGetRecordsCacheDaemon implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (prefetchCounters.byteSize < maxByteSize && prefetchCounters.size < maxRecordsCount) {
                    try {
                        GetRecordsResult getRecordsResult = getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
                        getRecordsResultQueue.put(getRecordsResult);
                        prefetchCounters.added(getRecordsResult);
                    } catch (InterruptedException e) {
                        log.error("Interrupted while adding records to the cache", e);
                    }
                }
            }
        }
    }
    
    private class PrefetchCounters {
        private volatile long size = 0;
        private volatile long byteSize = 0;
        
        public void added(final GetRecordsResult result) {
            size += getSize(result);
            byteSize += getByteSize(result);
        }
        
        public void removed(final GetRecordsResult result) {
            size -= getSize(result);
            byteSize -= getByteSize(result);
        }
        
        private long getSize(final GetRecordsResult result) {
            return result.getRecords().size();
        }
        
        private long getByteSize(final GetRecordsResult result) {
            return result.getRecords().stream().mapToLong(record -> record.getData().array().length).sum();
        }
    }
    
}
