package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.GetRecordsResult;
import lombok.extern.apachecommons.CommonsLog;

/**
 *
 */
@CommonsLog
public class BlockingGetRecordsCache extends GetRecordsCache {
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;

    public BlockingGetRecordsCache(final int maxRecordsPerCall, final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy) {
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
    }
    
    @Override
    public GetRecordsResult getNextResult() {
        return validateGetRecordsResult(getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall));
    }

    @Override
    public void shutdown() {
        // Nothing to do here.
    }
}
