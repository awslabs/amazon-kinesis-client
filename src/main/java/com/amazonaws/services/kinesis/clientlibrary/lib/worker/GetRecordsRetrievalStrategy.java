package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.GetRecordsResult;

/**
 * Represents a strategy to retrieve records from Kinesis. Allows for variations on how records are retrieved from
 * Kinesis.
 */
public interface GetRecordsRetrievalStrategy {
    /**
     * Gets a set of records from Kinesis.
     *
     * @param maxRecords
     *            passed to Kinesis, and can be used to restrict the number of records returned from Kinesis.
     * @return the resulting records.
     * @throws IllegalStateException
     *             if the strategy has been shutdown.
     */
    GetRecordsResult getRecords(int maxRecords);

    /**
     * Releases any resources used by the strategy. Once the strategy is shutdown it is no longer safe to call
     * {@link #getRecords(int)}.
     */
    void shutdown();

    /**
     * Returns whether this strategy has been shutdown.
     * 
     * @return true if the strategy has been shutdown, false otherwise.
     */
    boolean isShutdown();
}
