/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.retrieval;

import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
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
     * Returns the KinesisDataFetcher used to records from Kinesis.
     * 
     * @return KinesisDataFetcher
     */
    KinesisDataFetcher getDataFetcher();
}
