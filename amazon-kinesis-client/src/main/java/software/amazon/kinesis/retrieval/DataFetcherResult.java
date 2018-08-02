/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
 * http://aws.amazon.com/asl/ or in the "license" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package software.amazon.kinesis.retrieval;

import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;

/**
 * Represents the result from the DataFetcher, and allows the receiver to accept a result
 */
public interface DataFetcherResult {
    /**
     * The result of the request to Kinesis
     * 
     * @return The result of the request, this can be null if the request failed.
     */
    GetRecordsResponse getResult();

    /**
     * Accepts the result, and advances the shard iterator. A result from the data fetcher must be accepted before any
     * further progress can be made.
     * 
     * @return the result of the request, this can be null if the request failed.
     */
    GetRecordsResponse accept();

    /**
     * Indicates whether this result is at the end of the shard or not
     * 
     * @return true if the result is at the end of a shard, false otherwise
     */
    boolean isShardEnd();
}
