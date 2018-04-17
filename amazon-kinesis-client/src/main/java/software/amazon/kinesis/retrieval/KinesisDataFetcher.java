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

import java.util.Collections;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.util.CollectionUtils;
import com.google.common.collect.Iterables;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.checkpoint.SentinelCheckpoint;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Used to get data from Amazon Kinesis. Tracks iterator state internally.
 */
@Slf4j
public class KinesisDataFetcher {
    private final AmazonKinesis amazonKinesis;
    private final String streamName;
    private final String shardId;
    private final int maxRecords;

    /** Note: This method has package level access for testing purposes.
     * @return nextIterator
     */
    @Getter(AccessLevel.PACKAGE)
    private String nextIterator;
    @Getter
    private boolean isShardEndReached;
    private boolean isInitialized;
    private String lastKnownSequenceNumber;
    private InitialPositionInStreamExtended initialPositionInStream;

    public KinesisDataFetcher(@NonNull final AmazonKinesis amazonKinesis,
                              @NonNull final String streamName,
                              @NonNull final String shardId,
                              final int maxRecords) {
        this.amazonKinesis = amazonKinesis;
        this.streamName = streamName;
        this.shardId = shardId;
        this.maxRecords = maxRecords;
    }

    /**
     * Get records from the current position in the stream (up to maxRecords).
     *
     * @return list of records of up to maxRecords size
     */
    public DataFetcherResult getRecords() {
        if (!isInitialized) {
            throw new IllegalArgumentException("KinesisDataFetcher.getRecords called before initialization.");
        }

        if (nextIterator != null) {
            try {
                return new AdvancingResult(getRecords(nextIterator));
            } catch (ResourceNotFoundException e) {
                log.info("Caught ResourceNotFoundException when fetching records for shard {}", shardId);
                return TERMINAL_RESULT;
            }
        } else {
            return TERMINAL_RESULT;
        }
    }

    final DataFetcherResult TERMINAL_RESULT = new DataFetcherResult() {
        @Override
        public GetRecordsResult getResult() {
            return new GetRecordsResult().withMillisBehindLatest(null).withRecords(Collections.emptyList())
                    .withNextShardIterator(null);
        }

        @Override
        public GetRecordsResult accept() {
            isShardEndReached = true;
            return getResult();
        }

        @Override
        public boolean isShardEnd() {
            return isShardEndReached;
        }
    };

    @Data
    class AdvancingResult implements DataFetcherResult {

        final GetRecordsResult result;

        @Override
        public GetRecordsResult getResult() {
            return result;
        }

        @Override
        public GetRecordsResult accept() {
            nextIterator = result.getNextShardIterator();
            if (!CollectionUtils.isNullOrEmpty(result.getRecords())) {
                lastKnownSequenceNumber = Iterables.getLast(result.getRecords()).getSequenceNumber();
            }
            if (nextIterator == null) {
                isShardEndReached = true;
            }
            return getResult();
        }

        @Override
        public boolean isShardEnd() {
            return isShardEndReached;
        }
    }

    /**
     * Initializes this KinesisDataFetcher's iterator based on the checkpointed sequence number.
     * @param initialCheckpoint Current checkpoint sequence number for this shard.
     * @param initialPositionInStream The initialPositionInStream.
     */
    public void initialize(final String initialCheckpoint,
                           final InitialPositionInStreamExtended initialPositionInStream) {
        log.info("Initializing shard {} with {}", shardId, initialCheckpoint);
        advanceIteratorTo(initialCheckpoint, initialPositionInStream);
        isInitialized = true;
    }

    public void initialize(final ExtendedSequenceNumber initialCheckpoint,
                           final InitialPositionInStreamExtended initialPositionInStream) {
        log.info("Initializing shard {} with {}", shardId, initialCheckpoint.getSequenceNumber());
        advanceIteratorTo(initialCheckpoint.getSequenceNumber(), initialPositionInStream);
        isInitialized = true;
    }

    /**
     * Advances this KinesisDataFetcher's internal iterator to be at the passed-in sequence number.
     *
     * @param sequenceNumber advance the iterator to the record at this sequence number.
     * @param initialPositionInStream The initialPositionInStream.
     */
    public void advanceIteratorTo(final String sequenceNumber,
                                  final InitialPositionInStreamExtended initialPositionInStream) {
        if (sequenceNumber == null) {
            throw new IllegalArgumentException("SequenceNumber should not be null: shardId " + shardId);
        }

        try {
            final GetShardIteratorResult result;
            if (sequenceNumber.equals(SentinelCheckpoint.LATEST.toString())) {
                result = getShardIterator(amazonKinesis, streamName, shardId,
                        ShardIteratorType.LATEST, null, null);
            } else if (sequenceNumber.equals(SentinelCheckpoint.TRIM_HORIZON.toString())) {
                result = getShardIterator(amazonKinesis, streamName, shardId,
                        ShardIteratorType.TRIM_HORIZON, null, null);
            } else if (sequenceNumber.equals(SentinelCheckpoint.AT_TIMESTAMP.toString())) {
                result = getShardIterator(amazonKinesis, streamName, shardId,
                        ShardIteratorType.AT_TIMESTAMP, null, initialPositionInStream.getTimestamp());
            } else if (sequenceNumber.equals(SentinelCheckpoint.SHARD_END.toString())) {
                result = new GetShardIteratorResult().withShardIterator(null);
            } else {
                result = getShardIterator(amazonKinesis, streamName, shardId,
                        ShardIteratorType.AT_SEQUENCE_NUMBER, sequenceNumber, null);
            }
            nextIterator = result.getShardIterator();
        } catch (ResourceNotFoundException e) {
            log.info("Caught ResourceNotFoundException when getting an iterator for shard {}", shardId, e);
            nextIterator = null;
        }

        if (nextIterator == null) {
            isShardEndReached = true;
        }
        this.lastKnownSequenceNumber = sequenceNumber;
        this.initialPositionInStream = initialPositionInStream;
    }

    /**
     * Gets a new iterator from the last known sequence number i.e. the sequence number of the last record from the last
     * getRecords call.
     */
    public void restartIterator() {
        if (StringUtils.isEmpty(lastKnownSequenceNumber) || initialPositionInStream == null) {
            throw new IllegalStateException("Make sure to initialize the KinesisDataFetcher before restarting the iterator.");
        }
        advanceIteratorTo(lastKnownSequenceNumber, initialPositionInStream);
    }

    private GetShardIteratorResult getShardIterator(@NonNull final AmazonKinesis amazonKinesis,
                                                    @NonNull final String streamName,
                                                    @NonNull final String shardId,
                                                    @NonNull final ShardIteratorType shardIteratorType,
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
        return amazonKinesis.getShardIterator(request);
    }

    private GetRecordsResult getRecords(@NonNull final String nextIterator) {
        final GetRecordsRequest request = new GetRecordsRequest()
                .withShardIterator(nextIterator)
                .withLimit(maxRecords);
        return amazonKinesis.getRecords(request);
    }
}
