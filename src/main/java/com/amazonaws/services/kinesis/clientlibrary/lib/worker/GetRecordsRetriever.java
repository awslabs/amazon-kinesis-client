package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.GetRecordsResult;

import java.util.concurrent.Callable;

/**
 * This class uses the GetRecordsRetrievalStrategy class to retrieve the next set of records and update the cache. 
 */
public interface GetRecordsRetriever {
    GetRecordsResult getNextRecords(int maxRecords);
}
