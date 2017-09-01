package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.GetRecordsResult;
import lombok.Data;
import lombok.NonNull;

/**
 *
 */
@Data
public class SynchronousGetRecordsRetrievalStrategy implements GetRecordsRetrievalStrategy {
    @NonNull
    private final KinesisDataFetcher dataFetcher;

    @Override
    public GetRecordsResult getRecords(final int maxRecords) {
        return dataFetcher.getRecords(maxRecords);
    }

    @Override
    public void shutdown() {
        //
        // Does nothing as this retriever doesn't manage any resources
        //
    }

    @Override
    public boolean isShutdown() {
        return false;
    }
}
