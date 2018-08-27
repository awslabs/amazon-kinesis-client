/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.retrieval.polling;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Iterables;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisRequestsBuilder;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.retrieval.AWSExceptionManager;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.IteratorBuilder;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Used to get data from Amazon Kinesis. Tracks iterator state internally.
 */
@RequiredArgsConstructor
@Slf4j
public class KinesisDataFetcher {
    private static final String METRICS_PREFIX = "KinesisDataFetcher";
    private static final String OPERATION = "ProcessTask";

    @NonNull
    private final KinesisAsyncClient kinesisClient;
    @NonNull
    private final String streamName;
    @NonNull
    private final String shardId;
    private final int maxRecords;
    @NonNull
    private final MetricsFactory metricsFactory;

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

    /**
     * Get records from the current position in the stream (up to maxRecords).
     *
     * @return list of records of up to maxRecords size
     */
    public DataFetcherResult getRecords() {
        if (!isInitialized) {
            throw new IllegalArgumentException("KinesisDataFetcher.records called before initialization.");
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
        public GetRecordsResponse getResult() {
            return GetRecordsResponse.builder().millisBehindLatest(null).records(Collections.emptyList())
                    .nextShardIterator(null).build();
        }

        @Override
        public GetRecordsResponse accept() {
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

        final GetRecordsResponse result;

        @Override
        public GetRecordsResponse getResult() {
            return result;
        }

        @Override
        public GetRecordsResponse accept() {
            nextIterator = result.nextShardIterator();
            if (result.records() != null && !result.records().isEmpty()) {
                lastKnownSequenceNumber = Iterables.getLast(result.records()).sequenceNumber();
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
        log.info("Initializing shard {} with {}", shardId, initialCheckpoint.sequenceNumber());
        advanceIteratorTo(initialCheckpoint.sequenceNumber(), initialPositionInStream);
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

        final AWSExceptionManager exceptionManager = createExceptionManager();

        GetShardIteratorRequest.Builder builder = KinesisRequestsBuilder.getShardIteratorRequestBuilder()
                .streamName(streamName).shardId(shardId);
        GetShardIteratorRequest request = IteratorBuilder.request(builder, sequenceNumber, initialPositionInStream)
                .build();

        // TODO: Check if this metric is fine to be added
        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, OPERATION);
        MetricsUtil.addShardId(metricsScope, shardId);
        boolean success = false;
        long startTime = System.currentTimeMillis();

        try {
            try {
                final GetShardIteratorResponse result = kinesisClient.getShardIterator(request).get();
                nextIterator = result.shardIterator();
                success = true;
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: Check behavior
                throw new RuntimeException(e);
            }
        } catch (ResourceNotFoundException e) {
            log.info("Caught ResourceNotFoundException when getting an iterator for shard {}", shardId, e);
            nextIterator = null;
        } finally {
            MetricsUtil.addSuccessAndLatency(metricsScope, String.format("%s.%s", METRICS_PREFIX, "getShardIterator"),
                    success, startTime, MetricsLevel.DETAILED);
            MetricsUtil.endScope(metricsScope);
        }

        if (nextIterator == null) {
            isShardEndReached = true;
        }
        this.lastKnownSequenceNumber = sequenceNumber;
        this.initialPositionInStream = initialPositionInStream;
    }

    /**
     * Gets a new iterator from the last known sequence number i.e. the sequence number of the last record from the last
     * records call.
     */
    public void restartIterator() {
        if (StringUtils.isEmpty(lastKnownSequenceNumber) || initialPositionInStream == null) {
            throw new IllegalStateException(
                    "Make sure to initialize the KinesisDataFetcher before restarting the iterator.");
        }
        advanceIteratorTo(lastKnownSequenceNumber, initialPositionInStream);
    }

    private GetRecordsResponse getRecords(@NonNull final String nextIterator) {
        final AWSExceptionManager exceptionManager = createExceptionManager();
        GetRecordsRequest request = KinesisRequestsBuilder.getRecordsRequestBuilder().shardIterator(nextIterator)
                .limit(maxRecords).build();

        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, OPERATION);
        MetricsUtil.addShardId(metricsScope, shardId);
        boolean success = false;
        long startTime = System.currentTimeMillis();
        try {
            final GetRecordsResponse response = kinesisClient.getRecords(request).get();
            success = true;
            return response;
        } catch (ExecutionException e) {
            throw exceptionManager.apply(e.getCause());
        } catch (InterruptedException e) {
            // TODO: Check behavior
            log.debug("Interrupt called on metod, shutdown initiated");
            throw new RuntimeException(e);
        } finally {
            MetricsUtil.addSuccessAndLatency(metricsScope, String.format("%s.%s", METRICS_PREFIX, "getRecords"),
                    success, startTime, MetricsLevel.DETAILED);
            MetricsUtil.endScope(metricsScope);
        }
    }

    private AWSExceptionManager createExceptionManager() {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);
        exceptionManager.add(KinesisException.class, t -> t);
        return exceptionManager;
    }
}
