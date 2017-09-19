package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
 * records are retrieved from Kinesis. If prefetching is not enabled, the cache is not used and every single call to the
 * GetRecordsRetrievalStrategy is a blocking call.
 */
@CommonsLog
public class PrefetchGetRecordsCache extends GetRecordsCache {
    private LinkedBlockingQueue<GetRecordsResult> getRecordsResultQueue;
    private int maxSize;
    private int maxByteSize;
    private int maxRecordsCount;
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    private final ExecutorService executorService;
    
    private volatile int currentSizeInBytes = 0;
    private volatile int currentRecordsCount = 0;
    
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
            updateBytes(result, false);
            updateRecordsCount(result, false);
        } catch (InterruptedException e) {
            log.error("Interrupted while getting records from the cache", e);
        }
        return result;
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }
    
    private void updateBytes(final GetRecordsResult getRecordsResult, final boolean add) {
        getRecordsResult.getRecords().forEach(record -> {
            int newLength = record.getData().array().length;
            if (add) {
                currentSizeInBytes += newLength;
            } else {
                currentSizeInBytes -= newLength;
            }
        });
    }
    
    private void updateRecordsCount(final GetRecordsResult getRecordsResult, final boolean add) {
        int newSize = getRecordsResult.getRecords().size();
        if (add) {
            currentRecordsCount += newSize;
        } else {
            currentRecordsCount -= newSize;
        }
    }
    
    private class DefaultGetRecordsCacheDaemon implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (currentSizeInBytes < maxByteSize && currentRecordsCount < maxRecordsCount) {
                    try {
                        GetRecordsResult getRecordsResult = validateGetRecordsResult(
                                getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall));
                        getRecordsResultQueue.put(getRecordsResult);
                        if (getRecordsResultQueue.contains(getRecordsResult)) {
                            updateBytes(getRecordsResult, true);
                            updateRecordsCount(getRecordsResult, true);
                        }
                    } catch (InterruptedException e) {
                        log.error("Interrupted while adding records to the cache", e);
                    }
                }
            }
        }
    }
    
}
