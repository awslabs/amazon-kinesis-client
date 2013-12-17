/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.proxies;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;

/**
 * Kinesis proxy interface. Operates on a single stream (set up at initialization).
 */
public interface IKinesisProxy {

    /**
     * Get records from stream.
     * 
     * @param shardIterator Fetch data records using this shard iterator
     * @param maxRecords Fetch at most this many records
     * @return List of data records from Kinesis.
     * @throws InvalidArgumentException Invalid input parameters
     * @throws ResourceNotFoundException The Kinesis stream or shard was not found
     * @throws ExpiredIteratorException The iterator has expired
     */
    GetRecordsResult get(String shardIterator, int maxRecords)
        throws ResourceNotFoundException, InvalidArgumentException, ExpiredIteratorException;

    /**
     * Fetch information about stream. Useful for fetching the list of shards in a stream.
     * 
     * @param startShardId exclusive start shardId - used when paginating the list of shards.
     * @return DescribeStreamOutput object containing a description of the stream.
     * @throws ResourceNotFoundException The Kinesis stream was not found
     */
    DescribeStreamResult getStreamInfo(String startShardId) throws ResourceNotFoundException;

    /**
     * Fetch the shardIds of all shards in the stream.
     * 
     * @return Set of all shardIds
     * @throws ResourceNotFoundException If the specified Kinesis stream was not found
     */
    Set<String> getAllShardIds() throws ResourceNotFoundException;

    /**
     * Fetch all the shards defined for the stream (e.g. obtained via calls to the DescribeStream API).
     * This can be used to discover new shards and consume data from them.
     * 
     * @return List of all shards in the Kinesis stream.
     * @throws ResourceNotFoundException The Kinesis stream was not found.
     */
    List<Shard> getShardList() throws ResourceNotFoundException;

    /**
     * Fetch a shard iterator from the specified position in the shard.
     * 
     * @param shardId Shard id
     * @param iteratorEnum one of: TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER
     * @param sequenceNumber the sequence number - must be null unless iteratorEnum is AT_SEQUENCE_NUMBER or
     *        AFTER_SEQUENCE_NUMBER
     * @return shard iterator which can be used to read data from Kinesis.
     * @throws ResourceNotFoundException The Kinesis stream or shard was not found
     * @throws InvalidArgumentException Invalid input parameters
     */
    String getIterator(String shardId, String iteratorEnum, String sequenceNumber)
        throws ResourceNotFoundException, InvalidArgumentException;

    /**
     * @param sequenceNumberForOrdering (optional) used for record ordering
     * @param explicitHashKey optionally supplied transformation of partitionkey
     * @param partitionKey for this record
     * @param data payload
     * @return PutRecordResult (contains the Kinesis sequence number of the record).
     * @throws ResourceNotFoundException The Kinesis stream was not found.
     * @throws InvalidArgumentException InvalidArgumentException.
     */
    PutRecordResult put(String sequenceNumberForOrdering,
            String explicitHashKey,
            String partitionKey,
            ByteBuffer data) throws ResourceNotFoundException, InvalidArgumentException;
}
