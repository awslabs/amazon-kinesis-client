package com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker;

public class StreamRecordProcessingError extends Error {

    public static final String PROCESS_RECORDS_ERROR_MESSAGE = "Failure to process records for shard id: ";

    public StreamRecordProcessingError(String shardId) {
        super(PROCESS_RECORDS_ERROR_MESSAGE + shardId);
    }
}
