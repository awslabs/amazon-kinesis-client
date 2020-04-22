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

import java.util.Optional;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.kinesis.retrieval.polling.DataFetcher;
import software.amazon.kinesis.retrieval.polling.KinesisDataFetcher;

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
    GetRecordsResponse getRecords(int maxRecords);

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

    /**
     * Returns a DataFetcher used to records from Kinesis.
     *
     * @return DataFetcher
     */
    KinesisDataFetcher getDataFetcher();

    /**
     * Returns a DataFetcher override if applicable, else empty for retrieving records from Kinesis.
     *
     * @return Optional<DataFetcher>
     */
    default Optional<DataFetcher> getDataFetcherOverride() {
        return Optional.empty();
    }

    /**
     * Returns a dataFetcher by first checking for an override if it exists, else using the default data fetcher.
     *
     * @return DataFetcher
     */
    default DataFetcher dataFetcher() {
        return getDataFetcherOverride().orElse(getDataFetcher());
    }
}
