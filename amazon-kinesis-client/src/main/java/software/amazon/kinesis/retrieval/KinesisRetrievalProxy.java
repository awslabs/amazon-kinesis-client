/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import com.amazonaws.services.kinesis.model.ShardIteratorType;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.kinesis.metrics.IMetricsScope;

import java.util.Date;

/**
 *
 */
@RequiredArgsConstructor
public class KinesisRetrievalProxy implements RetrievalProxy {
    @NonNull
    private final AmazonKinesis amazonKinesis;
    @NonNull
    private final String streamName;
    @NonNull
    private final String shardId;
    private final int maxRecords;
    @NonNull
    private final IMetricsScope metricsScope;

    @Override
    public String getShardIterator(@NonNull final ShardIteratorType shardIteratorType,
                                   final String sequenceNumber,
                                   final Date timestamp) {
        GetShardIteratorRequest request = new GetShardIteratorRequest()
                .withStreamName(streamName)
                .withShardId(shardId)
                .withShardIteratorType(shardIteratorType);

        switch (shardIteratorType) {
            case AT_TIMESTAMP:
                request = request.withTimestamp(timestamp);
                break;
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                request = request.withStartingSequenceNumber(sequenceNumber);
                break;
        }
        return amazonKinesis.getShardIterator(request).getShardIterator();
    }

    @Override
    public GetRecordsResult getRecords(@NonNull final String shardIterator)
            throws ResourceNotFoundException, InvalidArgumentException, ExpiredIteratorException {
        metricsScope.end();
        return amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shardIterator).withLimit(maxRecords));
    }
}
