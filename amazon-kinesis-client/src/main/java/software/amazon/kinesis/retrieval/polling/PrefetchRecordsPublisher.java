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
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.RequestDetails;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.metrics.ThreadSafeMetricsDelegatingFactory;
import software.amazon.kinesis.retrieval.BatchUniqueIdentifier;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RecordsDeliveryAck;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.ThrottlingReporter;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static software.amazon.kinesis.common.DiagnosticUtils.takeDelayedDeliveryActionIfRequired;

/**
 * This is the prefetch caching class, this class spins up a thread if prefetching is enabled. That thread fetches the
 * next set of records and stores it in the cache. The size of the cache is limited by setting
 * maxPendingProcessRecordsInput i.e. the maximum number of GetRecordsResult that the cache can store, maxByteSize
 * i.e. the byte size of the records stored in the cache and maxRecordsCount i.e. the max number of records that should
 * be present in the cache across multiple GetRecordsResult object. If no data is available in the cache, the call from
 * the record processor is blocked till records are retrieved from Kinesis.
 * <br/><br/>
 * There are three threads namely publisher, demand-notifier and ack-notifier which will contend to drain the events
 * to the Subscriber (ShardConsumer in KCL).
 */
@Slf4j
@KinesisClientInternalApi
public class PrefetchRecordsPublisher implements RecordsPublisher {
    private static final String EXPIRED_ITERATOR_METRIC = "ExpiredIterator";
    // Since this package is being used by all KCL clients keeping the upper threshold of 60 seconds
    private static final long DEFAULT_AWAIT_TERMINATION_TIMEOUT_MILLIS = 60_000L;

    private final int maxPendingProcessRecordsInput;
    private final int maxByteSize;
    private final int maxRecordsCount;
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    private final ExecutorService executorService;
    private final MetricsFactory metricsFactory;
    private final long idleMillisBetweenCalls;
    private Instant lastSuccessfulCall;
    private boolean isFirstGetCallTry = true;
    private final DefaultGetRecordsCacheDaemon defaultGetRecordsCacheDaemon;
    private boolean started = false;
    private final String operation;
    private final StreamIdentifier streamId;
    private final String streamAndShardId;
    private final long awaitTerminationTimeoutMillis;
    private Subscriber<? super RecordsRetrieved> subscriber;

    @VisibleForTesting
    @Getter
    private final PublisherSession publisherSession;

    private final ReentrantReadWriteLock resetLock = new ReentrantReadWriteLock();
    private boolean wasReset = false;
    private Instant lastEventDeliveryTime = Instant.EPOCH;
    private final RequestDetails lastSuccessfulRequestDetails = new RequestDetails();
    private final ThrottlingReporter throttlingReporter;

    @Data
    @Accessors(fluent = true)
    static final class PublisherSession {
        private final AtomicLong requestedResponses = new AtomicLong(0);

        @VisibleForTesting
        @Getter
        private final LinkedBlockingQueue<PrefetchRecordsRetrieved> prefetchRecordsQueue;

        private final PrefetchCounters prefetchCounters;
        private final DataFetcher dataFetcher;
        private InitialPositionInStreamExtended initialPositionInStreamExtended;
        private String highestSequenceNumber;

        // Initialize the session on publisher start.
        void init(
                ExtendedSequenceNumber extendedSequenceNumber,
                InitialPositionInStreamExtended initialPositionInStreamExtended) {
            this.initialPositionInStreamExtended = initialPositionInStreamExtended;
            this.highestSequenceNumber = extendedSequenceNumber.sequenceNumber();
            this.dataFetcher.initialize(extendedSequenceNumber, initialPositionInStreamExtended);
        }

        // Reset the session when publisher restarts.
        void reset(PrefetchRecordsRetrieved prefetchRecordsRetrieved) {
            // Reset the demand from ShardConsumer, to prevent this publisher from delivering events to stale RX-Java
            // Subscriber. Publishing will be unblocked when the demand is communicated by the new Rx-Java subscriber.
            requestedResponses.set(0);
            // Clear the queue, so that the publisher repopulates the queue based on sequence number from subscriber.
            prefetchRecordsQueue.clear();
            prefetchCounters.reset();
            highestSequenceNumber = prefetchRecordsRetrieved.lastBatchSequenceNumber();
            dataFetcher.resetIterator(
                    prefetchRecordsRetrieved.shardIterator(), highestSequenceNumber, initialPositionInStreamExtended);
        }

        // Handle records delivery ack and execute nextEventDispatchAction.
        // This method is not thread-safe and needs to be called after acquiring a monitor.
        void handleRecordsDeliveryAck(
                RecordsDeliveryAck recordsDeliveryAck, String streamAndShardId, Runnable nextEventDispatchAction) {
            final PrefetchRecordsRetrieved recordsToCheck = peekNextRecord();
            // Verify if the ack matches the head of the queue and evict it.
            if (recordsToCheck != null
                    && recordsToCheck.batchUniqueIdentifier().equals(recordsDeliveryAck.batchUniqueIdentifier())) {
                evictPublishedRecordAndUpdateDemand(streamAndShardId);
                nextEventDispatchAction.run();
            } else {
                // Log and ignore any other ack received. As long as an ack is received for head of the queue
                // we are good. Any stale or future ack received can be ignored, though the latter is not feasible
                // to happen.
                final BatchUniqueIdentifier peekedBatchUniqueIdentifier =
                        recordsToCheck == null ? null : recordsToCheck.batchUniqueIdentifier();
                log.info(
                        "{} :  Received a stale notification with id {} instead of expected id {} at {}. Will ignore.",
                        streamAndShardId,
                        recordsDeliveryAck.batchUniqueIdentifier(),
                        peekedBatchUniqueIdentifier,
                        Instant.now());
            }
        }

        // Evict the published record from the prefetch queue.
        // This method is not thread-safe and needs to be called after acquiring a monitor.
        @VisibleForTesting
        RecordsRetrieved evictPublishedRecordAndUpdateDemand(String streamAndShardId) {
            final PrefetchRecordsRetrieved result = prefetchRecordsQueue.poll();
            if (result != null) {
                updateDemandTrackersOnPublish(result);
            } else {
                log.info(
                        "{}: No record batch found while evicting from the prefetch queue. This indicates the prefetch buffer"
                                + " was reset.",
                        streamAndShardId);
            }
            return result;
        }

        boolean hasDemandToPublish() {
            return requestedResponses.get() > 0;
        }

        PrefetchRecordsRetrieved peekNextRecord() {
            return prefetchRecordsQueue.peek();
        }

        boolean offerRecords(PrefetchRecordsRetrieved recordsRetrieved, long idleMillisBetweenCalls)
                throws InterruptedException {
            return prefetchRecordsQueue.offer(recordsRetrieved, idleMillisBetweenCalls, TimeUnit.MILLISECONDS);
        }

        private void updateDemandTrackersOnPublish(PrefetchRecordsRetrieved result) {
            prefetchCounters.removed(result.processRecordsInput);
            requestedResponses.decrementAndGet();
        }
    }

    /**
     * Constructor for the PrefetchRecordsPublisher. This cache prefetches records from Kinesis and stores them in a
     * LinkedBlockingQueue.
     *
     * @see PrefetchRecordsPublisher
     *
     * @param maxPendingProcessRecordsInput Max number of ProcessRecordsInput that can be held in the cache before
     *                                     blocking
     * @param maxByteSize Max byte size of the queue before blocking next get records call
     * @param maxRecordsCount Max number of records in the queue across all ProcessRecordInput objects
     * @param maxRecordsPerCall Max records to be returned per call
     * @param getRecordsRetrievalStrategy Retrieval strategy for the get records call
     * @param executorService Executor service for the cache
     * @param idleMillisBetweenCalls maximum time to wait before dispatching the next get records call
     * @param awaitTerminationTimeoutMillis maximum time to wait for graceful shutdown of executorService
     */
    public PrefetchRecordsPublisher(
            final int maxPendingProcessRecordsInput,
            final int maxByteSize,
            final int maxRecordsCount,
            final int maxRecordsPerCall,
            @NonNull final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy,
            @NonNull final ExecutorService executorService,
            final long idleMillisBetweenCalls,
            @NonNull final MetricsFactory metricsFactory,
            @NonNull final String operation,
            @NonNull final String shardId,
            final ThrottlingReporter throttlingReporter,
            final long awaitTerminationTimeoutMillis) {
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.maxPendingProcessRecordsInput = maxPendingProcessRecordsInput;
        this.maxByteSize = maxByteSize;
        this.maxRecordsCount = maxRecordsCount;
        this.publisherSession = new PublisherSession(
                new LinkedBlockingQueue<>(this.maxPendingProcessRecordsInput),
                new PrefetchCounters(),
                this.getRecordsRetrievalStrategy.dataFetcher());
        this.executorService = executorService;
        this.metricsFactory = new ThreadSafeMetricsDelegatingFactory(metricsFactory);
        this.idleMillisBetweenCalls = idleMillisBetweenCalls;
        this.defaultGetRecordsCacheDaemon = new DefaultGetRecordsCacheDaemon();
        Validate.notEmpty(operation, "Operation cannot be empty");
        this.throttlingReporter = throttlingReporter;
        this.operation = operation;
        this.streamId = this.getRecordsRetrievalStrategy.dataFetcher().getStreamIdentifier();
        this.streamAndShardId = this.streamId.serialize() + ":" + shardId;
        this.awaitTerminationTimeoutMillis = awaitTerminationTimeoutMillis;
    }

    /**
     * Constructor for the PrefetchRecordsPublisher. This cache prefetches records from Kinesis and stores them in a
     * LinkedBlockingQueue.
     *
     * @see PrefetchRecordsPublisher
     *
     * @param maxPendingProcessRecordsInput Max number of ProcessRecordsInput that can be held in the cache before
     *                                     blocking
     * @param maxByteSize Max byte size of the queue before blocking next get records call
     * @param maxRecordsCount Max number of records in the queue across all ProcessRecordInput objects
     * @param maxRecordsPerCall Max records to be returned per call
     * @param getRecordsRetrievalStrategy Retrieval strategy for the get records call
     * @param executorService Executor service for the cache
     * @param idleMillisBetweenCalls maximum time to wait before dispatching the next get records call
     */
    public PrefetchRecordsPublisher(
            final int maxPendingProcessRecordsInput,
            final int maxByteSize,
            final int maxRecordsCount,
            final int maxRecordsPerCall,
            final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy,
            final ExecutorService executorService,
            final long idleMillisBetweenCalls,
            final MetricsFactory metricsFactory,
            final String operation,
            final String shardId,
            final ThrottlingReporter throttlingReporter) {
        this(
                maxPendingProcessRecordsInput,
                maxByteSize,
                maxRecordsCount,
                maxRecordsPerCall,
                getRecordsRetrievalStrategy,
                executorService,
                idleMillisBetweenCalls,
                metricsFactory,
                operation,
                shardId,
                throttlingReporter,
                DEFAULT_AWAIT_TERMINATION_TIMEOUT_MILLIS);
    }

    @Override
    public void start(
            ExtendedSequenceNumber extendedSequenceNumber,
            InitialPositionInStreamExtended initialPositionInStreamExtended) {
        if (executorService.isShutdown()) {
            throw new IllegalStateException("ExecutorService has been shutdown.");
        }
        if (!started) {
            log.info("{} : Starting Prefetching thread and initializing publisher session.", streamAndShardId);
            publisherSession.init(extendedSequenceNumber, initialPositionInStreamExtended);
            executorService.execute(defaultGetRecordsCacheDaemon);
        } else {
            log.info("{} : Skipping publisher start as it was already started.", streamAndShardId);
        }
        started = true;
    }

    private void throwOnIllegalState() {
        if (executorService.isShutdown()) {
            throw new IllegalStateException("Shutdown has been called on the cache, can't accept new requests.");
        }

        if (!started) {
            throw new IllegalStateException("Cache has not been initialized, make sure to call start.");
        }
    }

    private PrefetchRecordsRetrieved peekNextResult() {
        return publisherSession.peekNextRecord();
    }

    @Override
    public void shutdown() {
        defaultGetRecordsCacheDaemon.isShutdown = true;
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(awaitTerminationTimeoutMillis, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
                // Wait a while for tasks to respond to being cancelled
                if (!executorService.awaitTermination(awaitTerminationTimeoutMillis, TimeUnit.MILLISECONDS)) {
                    log.error("Executor service didn't terminate");
                }
            }
        } catch (InterruptedException e) {
            // (Re-)Cancel if current thread also interrupted
            executorService.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
        started = false;
    }

    @Override
    public RequestDetails getLastSuccessfulRequestDetails() {
        return lastSuccessfulRequestDetails;
    }

    @Override
    public void restartFrom(RecordsRetrieved recordsRetrieved) {
        if (!(recordsRetrieved instanceof PrefetchRecordsRetrieved)) {
            throw new IllegalArgumentException(
                    "Provided RecordsRetrieved was not produced by the PrefetchRecordsPublisher");
        }
        resetLock.writeLock().lock();
        try {
            publisherSession.reset((PrefetchRecordsRetrieved) recordsRetrieved);
            wasReset = true;
        } finally {
            resetLock.writeLock().unlock();
        }
    }

    @Override
    public void subscribe(Subscriber<? super RecordsRetrieved> s) {
        throwOnIllegalState();
        subscriber = s;
        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                publisherSession.requestedResponses().addAndGet(n);
                drainQueueForRequests();
            }

            @Override
            public void cancel() {
                // When the subscription is cancelled, the demand is set to 0, to prevent further
                // records from being dispatched to the consumer/subscriber. The publisher session state will be
                // reset when restartFrom(*) is called by the consumer/subscriber.
                publisherSession.requestedResponses().set(0);
            }
        });
    }

    @Override
    public synchronized void notify(RecordsDeliveryAck recordsDeliveryAck) {
        publisherSession.handleRecordsDeliveryAck(recordsDeliveryAck, streamAndShardId, () -> drainQueueForRequests());
        // Take action based on the time spent by the event in queue.
        takeDelayedDeliveryActionIfRequired(streamAndShardId, lastEventDeliveryTime, log);
    }

    // Note : Do not make this method synchronous as notify() will not be able to evict any entry from the queue.
    private void addArrivedRecordsInput(PrefetchRecordsRetrieved recordsRetrieved) throws InterruptedException {
        wasReset = false;
        while (!publisherSession.offerRecords(recordsRetrieved, idleMillisBetweenCalls)) {
            //
            // Unlocking the read lock, and then reacquiring the read lock, should allow any waiters on the write lock a
            // chance to run. If the write lock is acquired by restartFrom than the readLock will now block until
            // restartFrom(...) has completed. This is to ensure that if a reset has occurred we know to discard the
            // data we received and start a new fetch of data.
            //
            resetLock.readLock().unlock();
            resetLock.readLock().lock();
            if (wasReset) {
                throw new PositionResetException();
            }
        }
        publisherSession.prefetchCounters().added(recordsRetrieved.processRecordsInput);
    }

    /**
     * Method to drain the queue based on the demand and the events availability in the queue.
     */
    @VisibleForTesting
    synchronized void drainQueueForRequests() {
        final PrefetchRecordsRetrieved recordsToDeliver = peekNextResult();
        // If there is an event available to drain and if there is at least one demand,
        // then schedule it for delivery
        if (publisherSession.hasDemandToPublish() && canDispatchRecord(recordsToDeliver)) {
            throwOnIllegalState();
            subscriber.onNext(recordsToDeliver.prepareForPublish());
            recordsToDeliver.dispatched();
            lastEventDeliveryTime = Instant.now();
        }
    }

    // This method is thread-safe and informs the caller on whether this record is eligible to be dispatched.
    // If this record was already dispatched earlier, then this method would return false.
    private static boolean canDispatchRecord(PrefetchRecordsRetrieved recordsToDeliver) {
        return recordsToDeliver != null && !recordsToDeliver.isDispatched();
    }

    @Accessors(fluent = true)
    @Data
    static class PrefetchRecordsRetrieved implements RecordsRetrieved {

        final ProcessRecordsInput processRecordsInput;
        final String lastBatchSequenceNumber;
        final String shardIterator;
        final BatchUniqueIdentifier batchUniqueIdentifier;

        @Accessors(fluent = false)
        @Setter(AccessLevel.NONE)
        boolean dispatched = false;

        PrefetchRecordsRetrieved prepareForPublish() {
            return new PrefetchRecordsRetrieved(
                    processRecordsInput.toBuilder().cacheExitTime(Instant.now()).build(),
                    lastBatchSequenceNumber,
                    shardIterator,
                    batchUniqueIdentifier);
        }

        @Override
        public BatchUniqueIdentifier batchUniqueIdentifier() {
            return batchUniqueIdentifier;
        }

        // Indicates if this record batch was already dispatched for delivery.
        void dispatched() {
            dispatched = true;
        }

        /**
         * Generate batch unique identifier for PrefetchRecordsRetrieved, where flow will be empty.
         * @return BatchUniqueIdentifier
         */
        public static BatchUniqueIdentifier generateBatchUniqueIdentifier() {
            return new BatchUniqueIdentifier(UUID.randomUUID().toString(), StringUtils.EMPTY);
        }
    }

    private String calculateHighestSequenceNumber(ProcessRecordsInput processRecordsInput) {
        String result = publisherSession.highestSequenceNumber();
        if (processRecordsInput.records() != null
                && !processRecordsInput.records().isEmpty()) {
            result = processRecordsInput
                    .records()
                    .get(processRecordsInput.records().size() - 1)
                    .sequenceNumber();
        }
        return result;
    }

    private static class PositionResetException extends RuntimeException {}

    private class DefaultGetRecordsCacheDaemon implements Runnable {
        volatile boolean isShutdown = false;

        @Override
        public void run() {
            while (!isShutdown) {
                if (Thread.currentThread().isInterrupted()) {
                    log.warn("{} : Prefetch thread was interrupted.", streamAndShardId);
                    break;
                }

                try {
                    resetLock.readLock().lock();
                    makeRetrievalAttempt();
                } catch (PositionResetException pre) {
                    log.debug("{} : Position was reset while attempting to add item to queue.", streamAndShardId);
                } catch (Throwable e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    log.error(
                            "{} :  Unexpected exception was thrown. This could probably be an issue or a bug."
                                    + " Please search for the exception/error online to check what is going on. If the "
                                    + "issue persists or is a recurring problem, feel free to open an issue on, "
                                    + "https://github.com/awslabs/amazon-kinesis-client.",
                            streamAndShardId,
                            e);
                } finally {
                    resetLock.readLock().unlock();
                }
            }
            callShutdownOnStrategy();
        }

        private void makeRetrievalAttempt() {
            MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, operation);
            if (publisherSession.prefetchCounters().shouldGetNewRecords()) {
                try {
                    sleepBeforeNextCall();
                    GetRecordsResponse getRecordsResult = getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
                    lastSuccessfulCall = Instant.now();

                    final List<KinesisClientRecord> records = getRecordsResult.records().stream()
                            .map(KinesisClientRecord::fromRecord)
                            .collect(Collectors.toList());
                    ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder()
                            .records(records)
                            .millisBehindLatest(getRecordsResult.millisBehindLatest())
                            .cacheEntryTime(lastSuccessfulCall)
                            .isAtShardEnd(
                                    getRecordsRetrievalStrategy.dataFetcher().isShardEndReached())
                            .childShards(getRecordsResult.childShards())
                            .build();

                    PrefetchRecordsRetrieved recordsRetrieved = new PrefetchRecordsRetrieved(
                            processRecordsInput,
                            calculateHighestSequenceNumber(processRecordsInput),
                            getRecordsResult.nextShardIterator(),
                            PrefetchRecordsRetrieved.generateBatchUniqueIdentifier());
                    publisherSession.highestSequenceNumber(recordsRetrieved.lastBatchSequenceNumber);
                    log.debug(
                            "Last sequence number retrieved for streamAndShardId {} is {}",
                            streamAndShardId,
                            recordsRetrieved.lastBatchSequenceNumber);
                    addArrivedRecordsInput(recordsRetrieved);
                    drainQueueForRequests();
                    throttlingReporter.success();
                } catch (PositionResetException pse) {
                    throw pse;
                } catch (RetryableRetrievalException rre) {
                    log.info(
                            "{} :  Timeout occurred while waiting for response from Kinesis.  Will retry the request.",
                            streamAndShardId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.info(
                            "{} :  Thread was interrupted, indicating shutdown was called on the cache.",
                            streamAndShardId);
                } catch (InvalidArgumentException e) {
                    log.info(
                            "{} :  records threw InvalidArgumentException - iterator will be refreshed before retrying",
                            streamAndShardId,
                            e);
                    publisherSession.dataFetcher().restartIterator();
                } catch (ExpiredIteratorException e) {
                    log.info(
                            "{} :  records threw ExpiredIteratorException - restarting"
                                    + " after greatest seqNum passed to customer",
                            streamAndShardId,
                            e);

                    MetricsUtil.addStreamId(scope, streamId);
                    scope.addData(EXPIRED_ITERATOR_METRIC, 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);

                    publisherSession.dataFetcher().restartIterator();
                } catch (ProvisionedThroughputExceededException e) {
                    log.error(
                            "{} : ProvisionedThroughputExceededException thrown while fetching records from Kinesis",
                            streamAndShardId,
                            e);
                    throttlingReporter.throttled();
                } catch (SdkException e) {
                    log.error("{} :  Exception thrown while fetching records from Kinesis", streamAndShardId, e);
                } finally {
                    MetricsUtil.endScope(scope);
                }
            } else {
                //
                // Consumer isn't ready to receive new records will allow prefetch counters to pause
                //
                try {
                    publisherSession.prefetchCounters().waitForConsumer();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.info(
                            "{} :  Thread was interrupted while waiting for the consumer.  "
                                    + "Shutdown has probably been started",
                            streamAndShardId);
                }
            }
        }

        private void callShutdownOnStrategy() {
            if (!getRecordsRetrievalStrategy.isShutdown()) {
                getRecordsRetrievalStrategy.shutdown();
            }
        }

        private void sleepBeforeNextCall() throws InterruptedException {
            if (lastSuccessfulCall == null && isFirstGetCallTry) {
                isFirstGetCallTry = false;
                return;
            }
            // Add a sleep if lastSuccessfulCall is still null but this is not the first try to avoid retry storm
            if (lastSuccessfulCall == null) {
                Thread.sleep(idleMillisBetweenCalls);
                return;
            }
            long timeSinceLastCall =
                    Duration.between(lastSuccessfulCall, Instant.now()).abs().toMillis();
            if (timeSinceLastCall < idleMillisBetweenCalls) {
                Thread.sleep(idleMillisBetweenCalls - timeSinceLastCall);
            }

            // avoid immediate-retry storms
            lastSuccessfulCall = null;
        }
    }

    private class PrefetchCounters {
        private long size = 0;
        private long byteSize = 0;

        public synchronized void added(final ProcessRecordsInput result) {
            size += getSize(result);
            byteSize += getByteSize(result);
        }

        public synchronized void removed(final ProcessRecordsInput result) {
            size -= getSize(result);
            byteSize -= getByteSize(result);
            this.notifyAll();
        }

        private long getSize(final ProcessRecordsInput result) {
            return result.records().size();
        }

        private long getByteSize(final ProcessRecordsInput result) {
            return result.records().stream()
                    .mapToLong(record -> record.data().limit())
                    .sum();
        }

        public synchronized void waitForConsumer() throws InterruptedException {
            if (!shouldGetNewRecords()) {
                log.debug(
                        "{} : Queue is full waiting for consumer for {} ms", streamAndShardId, idleMillisBetweenCalls);
                this.wait(idleMillisBetweenCalls);
            }
        }

        public synchronized boolean shouldGetNewRecords() {
            if (log.isDebugEnabled()) {
                log.debug("{} : Current Prefetch Counter States: {}", streamAndShardId, this.toString());
            }
            return size < maxRecordsCount && byteSize < maxByteSize;
        }

        void reset() {
            size = 0;
            byteSize = 0;
        }

        @Override
        public String toString() {
            return String.format(
                    "{ Requests: %d, Records: %d, Bytes: %d }",
                    publisherSession.prefetchRecordsQueue().size(), size, byteSize);
        }
    }
}
