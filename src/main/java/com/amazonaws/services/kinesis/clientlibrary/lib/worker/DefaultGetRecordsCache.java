package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.amazonaws.services.kinesis.model.GetRecordsResult;

import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;

/**
 *
 */
@CommonsLog
public class DefaultGetRecordsCache implements GetRecordsCache {
    private static final int DEFAULT_MAX_SIZE = 1;
    private static final int DEFAULT_MAX_BYTE_SIZE = 1;
    private static final int DEFAULT_MAX_RECORDS_COUNT = 1;
    
    private final Queue<GetRecordsResult> getRecordsResultQueue;
    private final int maxSize;
    private final int maxByteSize;
    private final int maxRecordsCount;
    private final int maxRecordsPerCall;
    @NonNull
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;

    public DefaultGetRecordsCache(final Optional<Integer> maxSize, final Optional<Integer> maxByteSize, final Optional<Integer> maxRecordsCount,
                                  final int maxRecordsPerCall, final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy) {
        this.getRecordsResultQueue = new ConcurrentLinkedQueue<>();
        this.maxSize = maxSize.orElse(DEFAULT_MAX_SIZE);
        this.maxByteSize = maxByteSize.orElse(DEFAULT_MAX_BYTE_SIZE);
        this.maxRecordsCount = maxRecordsCount.orElse(DEFAULT_MAX_RECORDS_COUNT);
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
    }

    @Override
    public GetRecordsResult getNextResult() {
        return null;
    }

    @Override
    public void shutdown() {

    }
}
