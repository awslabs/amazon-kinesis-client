package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.GetRecordsResult;

/**
 *
 */
public interface GetRecordsRetriever {
    GetRecordsResult getRecords(int maxRecords);
}
