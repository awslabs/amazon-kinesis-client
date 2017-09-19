package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.GetRecordsResult;

import java.util.Collections;

/**
 * This class is used as a cache for Prefetching data from Kinesis.
 */
public abstract class GetRecordsCache {
    /**
     * This method returns the next set of records from the Cache if present, or blocks the request till it gets the
     * next set of records back from Kinesis.
     * 
     * @return The next set of records.
     */
    public abstract GetRecordsResult getNextResult();
    
    public abstract void shutdown();
    
    protected GetRecordsResult validateGetRecordsResult(final GetRecordsResult getRecordsResult) {
        if (getRecordsResult == null) {
            return new GetRecordsResult().withRecords(Collections.emptyList());
        }
        return getRecordsResult;
    }
}
