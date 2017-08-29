package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.GetRecordsResult;
import lombok.Data;
import lombok.NonNull;

/**
 *
 */
@Data
public class DefaultGetRecordsExecutor implements GetRecordsExecutor {
    @NonNull
    private final KinesisDataFetcher dataFetcher;

    @Override
    public GetRecordsResult getRecords(final int maxRecords) {
        return dataFetcher.getRecords(maxRecords);
    }
}
