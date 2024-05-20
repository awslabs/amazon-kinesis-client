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

package software.amazon.kinesis.retrieval.polling;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import lombok.NonNull;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

public interface DataFetcher {
    /**
     * Get records from the current position in the stream (up to maxRecords).
     *
     * @return list of records of up to maxRecords size
     */
    DataFetcherResult getRecords();

    /**
     * Initializes this KinesisDataFetcher's iterator based on the checkpointed sequence number.
     *
     * @param initialCheckpoint       Current checkpoint sequence number for this shard.
     * @param initialPositionInStream The initialPositionInStream.
     */
    void initialize(String initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream);

    /**
     * Initializes this KinesisDataFetcher's iterator based on the checkpointed sequence number as an
     * ExtendedSequenceNumber.
     *
     * @param initialCheckpoint       Current checkpoint sequence number for this shard.
     * @param initialPositionInStream The initialPositionInStream.
     */
    void initialize(ExtendedSequenceNumber initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream);

    /**
     * Advances this KinesisDataFetcher's internal iterator to be at the passed-in sequence number.
     *
     * @param sequenceNumber          advance the iterator to the record at this sequence number.
     * @param initialPositionInStream The initialPositionInStream.
     */
    void advanceIteratorTo(String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream);

    /**
     * Gets a new iterator from the last known sequence number i.e. the sequence number of the last record from the last
     * records call.
     */
    void restartIterator();

    /**
     * Resets the iterator by setting shardIterator, sequenceNumber and the position in the stream.
     *
     * @param shardIterator           set the current shard iterator.
     * @param sequenceNumber          reset the iterator to the record at this sequence number.
     * @param initialPositionInStream the current position in the stream to reset the iterator to.
     */
    void resetIterator(
            String shardIterator, String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream);

    /**
     * Retrieves the response based on the request.
     *
     * @param request the current get records request used to receive a response.
     * @return GetRecordsResponse response for getRecords
     */
    GetRecordsResponse getGetRecordsResponse(GetRecordsRequest request) throws Exception;

    /**
     * Retrieves the next get records request based on the current iterator.
     *
     * @param nextIterator specify the iterator to get the next record request
     * @return {@link GetRecordsRequest}
     */
    GetRecordsRequest getGetRecordsRequest(String nextIterator);

    /**
     * Gets the next iterator based on the request.
     *
     * @param request used to obtain the next shard iterator
     * @return next iterator string
     */
    String getNextIterator(GetShardIteratorRequest request)
            throws ExecutionException, InterruptedException, TimeoutException;

    /**
     * Gets the next set of records based on the iterator.
     *
     * @param nextIterator specified shard iterator for getting the next set of records
     * @return {@link GetRecordsResponse}
     */
    GetRecordsResponse getRecords(@NonNull String nextIterator);

    /**
     * Get the current account and stream information.
     *
     * @return {@link StreamIdentifier}
     */
    StreamIdentifier getStreamIdentifier();

    /**
     * Checks if shardEnd is reached.
     * @return boolean to determine whether shard end is reached
     */
    boolean isShardEndReached();
}
