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

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Iterables;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisRequestsBuilder;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.retrieval.AWSExceptionManager;
import software.amazon.kinesis.retrieval.DataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.IteratorBuilder;
import software.amazon.kinesis.retrieval.KinesisDataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static software.amazon.kinesis.retrieval.DataRetrievalUtil.isValidResult;

/**
 * Used to get data from Amazon Kinesis. Tracks iterator state internally.
 */
@Slf4j
@KinesisClientInternalApi
public class KinesisDataFetcher implements DataFetcher {

    private static final String METRICS_PREFIX = "KinesisDataFetcher";
    private static final String OPERATION = "ProcessTask";

    /**
     * Reusable {@link AWSExceptionManager}.
     * <p>
     * N.B. This instance is mutable, but thread-safe for <b>read-only</b> use.
     * </p>
     */
    private static final AWSExceptionManager AWS_EXCEPTION_MANAGER = createExceptionManager();

    @NonNull
    private final KinesisAsyncClient kinesisClient;

    @NonNull
    @Getter
    private final StreamIdentifier streamIdentifier;

    @NonNull
    private final String shardId;

    private final int maxRecords;

    @NonNull
    private final MetricsFactory metricsFactory;

    private final Duration maxFutureWait;
    private final String streamAndShardId;

    @Deprecated
    public KinesisDataFetcher(
            KinesisAsyncClient kinesisClient,
            String streamName,
            String shardId,
            int maxRecords,
            MetricsFactory metricsFactory) {
        this(
                kinesisClient,
                new KinesisDataFetcherProviderConfig(
                        StreamIdentifier.singleStreamInstance(streamName),
                        shardId,
                        metricsFactory,
                        maxRecords,
                        PollingConfig.DEFAULT_REQUEST_TIMEOUT));
    }

    /**
     * Note: This method has package level access for testing purposes.
     */
    @Getter(AccessLevel.PACKAGE)
    private String nextIterator;

    /**
     * Constructs KinesisDataFetcher.
     *
     * @param kinesisClient
     * @param kinesisDataFetcherProviderConfig
     */
    public KinesisDataFetcher(
            KinesisAsyncClient kinesisClient, DataFetcherProviderConfig kinesisDataFetcherProviderConfig) {
        this.kinesisClient = kinesisClient;
        this.maxFutureWait = kinesisDataFetcherProviderConfig.getKinesisRequestTimeout();
        this.maxRecords = kinesisDataFetcherProviderConfig.getMaxRecords();
        this.metricsFactory = kinesisDataFetcherProviderConfig.getMetricsFactory();
        this.shardId = kinesisDataFetcherProviderConfig.getShardId();
        this.streamIdentifier = kinesisDataFetcherProviderConfig.getStreamIdentifier();
        this.streamAndShardId = streamIdentifier.serialize() + ":" + shardId;
    }

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
    @Override
    public DataFetcherResult getRecords() {
        if (!isInitialized) {
            throw new IllegalArgumentException("KinesisDataFetcher.records called before initialization.");
        }

        if (nextIterator != null) {
            try {
                return new AdvancingResult(getRecords(nextIterator));
            } catch (ResourceNotFoundException e) {
                log.info("Caught ResourceNotFoundException when fetching records for shard {}", streamAndShardId);
                return TERMINAL_RESULT;
            }
        } else {
            return TERMINAL_RESULT;
        }
    }

    // CHECKSTYLE.OFF: MemberName
    final DataFetcherResult TERMINAL_RESULT = new DataFetcherResult() {
        // CHECKSTYLE.ON: MemberName
        @Override
        public GetRecordsResponse getResult() {
            return GetRecordsResponse.builder()
                    .millisBehindLatest(null)
                    .records(Collections.emptyList())
                    .nextShardIterator(null)
                    .childShards(Collections.emptyList())
                    .build();
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
    @Override
    public void initialize(
            final String initialCheckpoint, final InitialPositionInStreamExtended initialPositionInStream) {
        log.info("Initializing shard {} with {}", streamAndShardId, initialCheckpoint);
        advanceIteratorTo(initialCheckpoint, initialPositionInStream);
        isInitialized = true;
    }

    @Override
    public void initialize(
            final ExtendedSequenceNumber initialCheckpoint,
            final InitialPositionInStreamExtended initialPositionInStream) {
        log.info("Initializing shard {} with {}", streamAndShardId, initialCheckpoint.sequenceNumber());
        advanceIteratorTo(initialCheckpoint.sequenceNumber(), initialPositionInStream);
        isInitialized = true;
    }

    /**
     * Advances this KinesisDataFetcher's internal iterator to be at the passed-in sequence number.
     *
     * @param sequenceNumber advance the iterator to the record at this sequence number.
     * @param initialPositionInStream The initialPositionInStream.
     */
    @Override
    public void advanceIteratorTo(
            final String sequenceNumber, final InitialPositionInStreamExtended initialPositionInStream) {
        advanceIteratorTo(sequenceNumber, initialPositionInStream, false);
    }

    private void advanceIteratorTo(
            final String sequenceNumber,
            final InitialPositionInStreamExtended initialPositionInStream,
            boolean isIteratorRestart) {
        if (sequenceNumber == null) {
            throw new IllegalArgumentException("SequenceNumber should not be null: shardId " + shardId);
        }

        GetShardIteratorRequest.Builder builder = KinesisRequestsBuilder.getShardIteratorRequestBuilder()
                .streamName(streamIdentifier.streamName())
                .shardId(shardId);
        streamIdentifier.streamArnOptional().ifPresent(arn -> builder.streamARN(arn.toString()));

        GetShardIteratorRequest request;
        if (isIteratorRestart) {
            request = IteratorBuilder.reconnectRequest(builder, sequenceNumber, initialPositionInStream)
                    .build();
        } else {
            request = IteratorBuilder.request(builder, sequenceNumber, initialPositionInStream)
                    .build();
        }
        log.debug("[GetShardIterator] Request has parameters {}", request);

        // TODO: Check if this metric is fine to be added
        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, OPERATION);
        MetricsUtil.addStreamId(metricsScope, streamIdentifier);
        MetricsUtil.addShardId(metricsScope, shardId);
        boolean success = false;
        long startTime = System.currentTimeMillis();

        try {
            try {
                nextIterator = getNextIterator(request);
                success = true;
            } catch (ExecutionException e) {
                throw AWS_EXCEPTION_MANAGER.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: Check behavior
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                throw new RetryableRetrievalException(e.getMessage(), e);
            }
        } catch (ResourceNotFoundException e) {
            log.info("Caught ResourceNotFoundException when getting an iterator for shard {}", streamAndShardId, e);
            nextIterator = null;
        } finally {
            MetricsUtil.addSuccessAndLatency(
                    metricsScope,
                    String.format("%s.%s", METRICS_PREFIX, "getShardIterator"),
                    success,
                    startTime,
                    MetricsLevel.DETAILED);
            MetricsUtil.endScope(metricsScope);
        }

        if (nextIterator == null) {
            isShardEndReached = true;
        }
        this.lastKnownSequenceNumber = sequenceNumber;
        this.initialPositionInStream = initialPositionInStream;
    }

    /**
     * Gets a new next shard iterator from last known sequence number i.e. the sequence number of the last
     * record from the last records call.
     */
    @Override
    public void restartIterator() {
        if (StringUtils.isEmpty(lastKnownSequenceNumber) || initialPositionInStream == null) {
            throw new IllegalStateException(
                    "Make sure to initialize the KinesisDataFetcher before restarting the iterator.");
        }
        log.debug(
                "Restarting iterator for sequence number {} on shard id {}", lastKnownSequenceNumber, streamAndShardId);
        advanceIteratorTo(lastKnownSequenceNumber, initialPositionInStream, true);
    }

    @Override
    public void resetIterator(
            String shardIterator, String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream) {
        this.nextIterator = shardIterator;
        this.lastKnownSequenceNumber = sequenceNumber;
        this.initialPositionInStream = initialPositionInStream;
    }

    @Override
    public GetRecordsResponse getGetRecordsResponse(GetRecordsRequest request)
            throws ExecutionException, InterruptedException, TimeoutException {
        final GetRecordsResponse response =
                FutureUtils.resolveOrCancelFuture(kinesisClient.getRecords(request), maxFutureWait);
        if (!isValidResult(response.nextShardIterator(), response.childShards())) {
            throw new RetryableRetrievalException("GetRecords response is not valid for shard: " + streamAndShardId
                    + ". nextShardIterator: " + response.nextShardIterator()
                    + ". childShards: " + response.childShards()
                    + ". Will retry GetRecords with the same nextIterator.");
        }
        return response;
    }

    @Override
    public GetRecordsRequest getGetRecordsRequest(String nextIterator) {
        GetRecordsRequest.Builder builder = KinesisRequestsBuilder.getRecordsRequestBuilder()
                .shardIterator(nextIterator)
                .limit(maxRecords);
        streamIdentifier.streamArnOptional().ifPresent(arn -> builder.streamARN(arn.toString()));
        return builder.build();
    }

    @Override
    public String getNextIterator(GetShardIteratorRequest request)
            throws ExecutionException, InterruptedException, TimeoutException {
        final GetShardIteratorResponse result =
                FutureUtils.resolveOrCancelFuture(kinesisClient.getShardIterator(request), maxFutureWait);
        return result.shardIterator();
    }

    @Override
    public GetRecordsResponse getRecords(@NonNull final String nextIterator) {
        GetRecordsRequest request = getGetRecordsRequest(nextIterator);

        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, OPERATION);
        MetricsUtil.addStreamId(metricsScope, streamIdentifier);
        MetricsUtil.addShardId(metricsScope, shardId);
        boolean success = false;
        long startTime = System.currentTimeMillis();
        try {
            final GetRecordsResponse response = getGetRecordsResponse(request);
            success = true;
            return response;
        } catch (ExecutionException e) {
            throw AWS_EXCEPTION_MANAGER.apply(e.getCause());
        } catch (InterruptedException e) {
            // TODO: Check behavior
            log.debug("{} : Interrupt called on method, shutdown initiated", streamAndShardId);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RetryableRetrievalException(e.getMessage(), e);
        } finally {
            MetricsUtil.addSuccessAndLatency(
                    metricsScope,
                    String.format("%s.%s", METRICS_PREFIX, "getRecords"),
                    success,
                    startTime,
                    MetricsLevel.DETAILED);
            MetricsUtil.endScope(metricsScope);
        }
    }

    private static AWSExceptionManager createExceptionManager() {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);
        exceptionManager.add(KinesisException.class, t -> t);
        exceptionManager.add(SdkException.class, t -> t);
        return exceptionManager;
    }
}
