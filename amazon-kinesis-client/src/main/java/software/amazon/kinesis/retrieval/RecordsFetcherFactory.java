/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.kinesis.retrieval;

import software.amazon.kinesis.metrics.MetricsFactory;

/**
 * This factory is used to create the records fetcher to retrieve data from Kinesis for a given shard.
 */
public interface RecordsFetcherFactory {
    /**
     * Returns a RecordsPublisher to be used for retrieving records for a given shard.
     *
     * @param getRecordsRetrievalStrategy GetRecordsRetrievalStrategy to be used with the RecordsPublisher
     * @param shardId ShardId of the shard that the fetcher will retrieve records for
     * @param metricsFactory MetricsFactory used to create metricScope
     * @param maxRecords Max number of records to be returned in a single get call
     *                
     * @return RecordsPublisher used to get records from Kinesis.
     */
    RecordsPublisher createRecordsFetcher(GetRecordsRetrievalStrategy getRecordsRetrievalStrategy, String shardId,
                                          MetricsFactory metricsFactory, int maxRecords);

    /**
     * Sets the maximum number of ProcessRecordsInput objects the RecordsPublisher can hold, before further requests are
     * blocked.
     *
     * @param maxPendingProcessRecordsInput The maximum number of ProcessRecordsInput objects that the cache will accept
     *                                     before blocking.
     */
    void maxPendingProcessRecordsInput(int maxPendingProcessRecordsInput);

    int maxPendingProcessRecordsInput();

    /**
     * Sets the max byte size for the RecordsPublisher, before further requests are blocked. The byte size of the cache
     * is the sum of byte size of all the ProcessRecordsInput objects in the cache at any point of time.
     *
     * @param maxByteSize The maximum byte size for the cache before blocking.
     */
    void maxByteSize(int maxByteSize);

    int maxByteSize();

    /**
     * Sets the max number of records for the RecordsPublisher can hold, before further requests are blocked. The records
     * count is the sum of all records present in across all the ProcessRecordsInput objects in the cache at any point
     * of time.
     *
     * @param maxRecordsCount The mximum number of records in the cache before blocking.
     */
    void maxRecordsCount(int maxRecordsCount);

    int maxRecordsCount();

    /**
     * Sets the dataFetchingStrategy to determine the type of RecordsPublisher to be used.
     *
     * @param dataFetchingStrategy Fetching strategy to be used
     */
    void dataFetchingStrategy(DataFetchingStrategy dataFetchingStrategy);

    DataFetchingStrategy dataFetchingStrategy();

    /**
     * Sets the maximum idle time between two get calls.
     *
     * @param idleMillisBetweenCalls Sleep millis between calls.
     */
    void idleMillisBetweenCalls(long idleMillisBetweenCalls);

    long idleMillisBetweenCalls();

}
