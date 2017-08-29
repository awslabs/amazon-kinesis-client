package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.GetRecordsResult;

/**
 *
 */
public interface GetRecordsExecutor {
    GetRecordsResult getRecords(int maxRecords);
}
