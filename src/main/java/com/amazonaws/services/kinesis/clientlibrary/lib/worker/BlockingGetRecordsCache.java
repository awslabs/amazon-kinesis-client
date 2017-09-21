package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
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
    public ProcessRecordsInput getNextResult() {
        GetRecordsResult getRecordsResult = getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
        ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
                .withRecords(getRecordsResult.getRecords())
                .withMillisBehindLatest(getRecordsResult.getMillisBehindLatest());
        return processRecordsInput;
    }

    @Override
    public void shutdown() {
        // Nothing to do here.
    }
}
