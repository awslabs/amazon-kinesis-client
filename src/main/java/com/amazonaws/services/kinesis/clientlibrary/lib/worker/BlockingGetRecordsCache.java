package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.GetRecordsResult;

import lombok.extern.apachecommons.CommonsLog;

/**
 * This is the BlockingGetRecordsCache class. This class blocks any calls to the getRecords on the
 * GetRecordsRetrievalStrategy class.
 */
@CommonsLog
public class BlockingGetRecordsCache implements GetRecordsCache {
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;

    public BlockingGetRecordsCache(final int maxRecordsPerCall, final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy) {
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
    }
    
    @Override
    public GetRecordsResult getNextResult() {
        return getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
    }

    @Override
    public void shutdown() {
        // Nothing to do here.
    }
}
