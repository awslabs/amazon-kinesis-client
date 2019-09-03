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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.annotations.VisibleForTesting;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
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
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static software.amazon.kinesis.common.DiagnosticUtils.takeDelayedDeliveryActionIfRequired;

/**
 * This is the prefetch caching class, this class spins up a thread if prefetching is enabled. That thread fetches the
 * next set of records and stores it in the cache. The size of the cache is limited by setting
 * maxPendingProcessRecordsInput i.e. the maximum number of GetRecordsResult that the cache can store, maxByteSize
 * i.e. the byte size of the records stored in the cache and maxRecordsCount i.e. the max number of records that should
 * be present in the cache across multiple GetRecordsResult object. If no data is available in the cache, the call from
 * the record processor is blocked till records are retrieved from Kinesis.
 *
 * There are three threads namely publisher, demand-notifier and ack-notifier which will contend to drain the events
 * to the Subscriber (ShardConsumer in KCL). The publisher/demand-notifier thread gains the control to drain only when
 * there is no pending event in the prefetch queue waiting for the ack. Otherwise, it will be the ack-notifier thread
 * which will drain an event on the receipt of an ack.
 *
 */
@Slf4j
@KinesisClientInternalApi
public class PrefetchRecordsPublisher implements RecordsPublisher {
    private static final String EXPIRED_ITERATOR_METRIC = "ExpiredIterator";
    @VisibleForTesting
    LinkedBlockingQueue<PrefetchRecordsRetrieved> getRecordsResultQueue;
    private int maxPendingProcessRecordsInput;
    private int maxByteSize;
    private int maxRecordsCount;
    private final int maxRecordsPerCall;
    private final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    private final ExecutorService executorService;
    private final MetricsFactory metricsFactory;
    private final long idleMillisBetweenCalls;
    private Instant lastSuccessfulCall;
    private final DefaultGetRecordsCacheDaemon defaultGetRecordsCacheDaemon;
    private PrefetchCounters prefetchCounters;
    private boolean started = false;
    private final String operation;
    private final KinesisDataFetcher dataFetcher;
    private final String shardId;

    private Subscriber<? super RecordsRetrieved> subscriber;
    private final AtomicLong requestedResponses = new AtomicLong(0);

    private String highestSequenceNumber;
    private InitialPositionInStreamExtended initialPositionInStreamExtended;

    private final ReentrantReadWriteLock resetLock = new ReentrantReadWriteLock();
    private boolean wasReset = false;

    private Instant lastEventDeliveryTime = Instant.EPOCH;
    // This flag controls who should drain the next request in the prefetch queue.
    // When set to false, the publisher and demand-notifier thread would have the control.
    // When set to true, the event-notifier thread would have the control.
    private AtomicBoolean shouldDrainEventOnlyOnAck = new AtomicBoolean(false);

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
    public PrefetchRecordsPublisher(final int maxPendingProcessRecordsInput, final int maxByteSize, final int maxRecordsCount,
                                    final int maxRecordsPerCall,
                                    @NonNull final GetRecordsRetrievalStrategy getRecordsRetrievalStrategy,
                                    @NonNull final ExecutorService executorService,
                                    final long idleMillisBetweenCalls,
                                    @NonNull final MetricsFactory metricsFactory,
                                    @NonNull final String operation,
                                    @NonNull final String shardId) {
        this.getRecordsRetrievalStrategy = getRecordsRetrievalStrategy;
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.maxPendingProcessRecordsInput = maxPendingProcessRecordsInput;
        this.maxByteSize = maxByteSize;
        this.maxRecordsCount = maxRecordsCount;
        this.getRecordsResultQueue = new LinkedBlockingQueue<>(this.maxPendingProcessRecordsInput);
        this.prefetchCounters = new PrefetchCounters();
        this.executorService = executorService;
        this.metricsFactory = new ThreadSafeMetricsDelegatingFactory(metricsFactory);
        this.idleMillisBetweenCalls = idleMillisBetweenCalls;
        this.defaultGetRecordsCacheDaemon = new DefaultGetRecordsCacheDaemon();
        Validate.notEmpty(operation, "Operation cannot be empty");
        this.operation = operation;
        this.dataFetcher = this.getRecordsRetrievalStrategy.getDataFetcher();
        this.shardId = shardId;
    }

    @Override
    public void start(ExtendedSequenceNumber extendedSequenceNumber, InitialPositionInStreamExtended initialPositionInStreamExtended) {
        if (executorService.isShutdown()) {
            throw new IllegalStateException("ExecutorService has been shutdown.");
        }

        this.initialPositionInStreamExtended = initialPositionInStreamExtended;
        highestSequenceNumber = extendedSequenceNumber.sequenceNumber();
        dataFetcher.initialize(extendedSequenceNumber, initialPositionInStreamExtended);

        if (!started) {
            log.info("{} : Starting prefetching thread.", shardId);
            executorService.execute(defaultGetRecordsCacheDaemon);
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

    private RecordsRetrieved peekNextResult() {
        throwOnIllegalState();
        final PrefetchRecordsRetrieved result = getRecordsResultQueue.peek();
        return result == null ? result : result.prepareForPublish();
    }

    @VisibleForTesting
    RecordsRetrieved pollNextResultAndUpdatePrefetchCounters() {
        throwOnIllegalState();
        final PrefetchRecordsRetrieved result = getRecordsResultQueue.poll();
        if (result != null) {
            prefetchCounters.removed(result.processRecordsInput);
            requestedResponses.decrementAndGet();
        } else {
            log.info(
                    "{}: No record batch found while evicting from the prefetch queue. This indicates the prefetch buffer"
                            + "was reset.", shardId);
        }
        return result;
    }

    @Override
    public void shutdown() {
        defaultGetRecordsCacheDaemon.isShutdown = true;
        executorService.shutdownNow();
        started = false;
    }

    @Override
    public void restartFrom(RecordsRetrieved recordsRetrieved) {
        if (!(recordsRetrieved instanceof PrefetchRecordsRetrieved)) {
            throw new IllegalArgumentException(
                    "Provided RecordsRetrieved was not produced by the PrefetchRecordsPublisher");
        }
        PrefetchRecordsRetrieved prefetchRecordsRetrieved = (PrefetchRecordsRetrieved) recordsRetrieved;
        resetLock.writeLock().lock();
        try {
            getRecordsResultQueue.clear();

            // Give the drain control to publisher/demand-notifier thread.
            log.debug("{} : Publisher thread takes over the draining control. Queue Size : {}, Demand : {}", shardId,
                    getRecordsResultQueue.size(), requestedResponses.get());
            shouldDrainEventOnlyOnAck.set(false);

            prefetchCounters.reset();

            highestSequenceNumber = prefetchRecordsRetrieved.lastBatchSequenceNumber();
            dataFetcher.resetIterator(prefetchRecordsRetrieved.shardIterator(), highestSequenceNumber,
                    initialPositionInStreamExtended);
            wasReset = true;
        } finally {
            resetLock.writeLock().unlock();
        }
    }

    @Override
    public void subscribe(Subscriber<? super RecordsRetrieved> s) {
        subscriber = s;
        subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                requestedResponses.addAndGet(n);
                drainQueueForRequestsIfAllowed();
            }

            @Override
            public void cancel() {
                requestedResponses.set(0);
            }
        });
    }

    @Override
    public synchronized void notify(RecordsDeliveryAck recordsDeliveryAck) {
        final RecordsRetrieved recordsToCheck = peekNextResult();
        // Verify if the ack matches the head of the queue and evict it.
        if (recordsToCheck != null && recordsToCheck.batchUniqueIdentifier()
                .equals(recordsDeliveryAck.batchUniqueIdentifier())) {
            pollNextResultAndUpdatePrefetchCounters();
            // Upon evicting, check if queue is empty. if yes, then give the drain control back to publisher thread.
            if (getRecordsResultQueue.isEmpty()) {
                log.debug("{} : Publisher thread takes over the draining control. Queue Size : {}, Demand : {}",
                        shardId, getRecordsResultQueue.size(), requestedResponses.get());
                shouldDrainEventOnlyOnAck.set(false);
            } else {
                // Else attempt to drain the queue.
                drainQueueForRequests();
            }
        } else {
            // Log and ignore any other ack received. As long as an ack is received for head of the queue
            // we are good. Any stale or future ack received can be ignored, though the latter is not feasible
            // to happen.
            final BatchUniqueIdentifier peekedBatchUniqueIdentifier =
                    recordsToCheck == null ? null : recordsToCheck.batchUniqueIdentifier();
            log.info("{} :  Received a stale notification with id {} instead of expected id {} at {}. Will ignore.",
                    shardId, recordsDeliveryAck.batchUniqueIdentifier(), peekedBatchUniqueIdentifier, Instant.now());
        }
        // Take action based on the time spent by the event in queue.
        takeDelayedDeliveryActionIfRequired(shardId, lastEventDeliveryTime, log);
    }

    // Note : Do not make this method synchronous as notify() will not be able to evict any entry from the queue.
    private void addArrivedRecordsInput(PrefetchRecordsRetrieved recordsRetrieved) throws InterruptedException {
        wasReset = false;
        while (!getRecordsResultQueue.offer(recordsRetrieved, idleMillisBetweenCalls, TimeUnit.MILLISECONDS)) {
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
        prefetchCounters.added(recordsRetrieved.processRecordsInput);
    }

    /**
     * Method that will be called by the 'publisher thread' and the 'demand notifying thread',
     * to drain the events if the 'event notifying thread' do not have the control.
     */
    private synchronized void drainQueueForRequestsIfAllowed() {
        if (!shouldDrainEventOnlyOnAck.get()) {
            drainQueueForRequests();
        }
    }

    /**
     * Method to drain the queue based on the demand and the events availability in the queue.
     */
    private synchronized void drainQueueForRequests() {
        final RecordsRetrieved recordsToDeliver = peekNextResult();
        // If there is an event available to drain and if there is at least one demand,
        // then schedule it for delivery
        if (requestedResponses.get() > 0 && recordsToDeliver != null) {
            lastEventDeliveryTime = Instant.now();
            subscriber.onNext(recordsToDeliver);
            if (!shouldDrainEventOnlyOnAck.get()) {
                log.debug("{} : Notifier thread takes over the draining control. Queue Size : {}, Demand : {}", shardId,
                        getRecordsResultQueue.size(), requestedResponses.get());
                shouldDrainEventOnlyOnAck.set(true);
            }
        } else {
            // Since we haven't scheduled the event delivery, give the drain control back to publisher/demand-notifier
            // thread.
            if (shouldDrainEventOnlyOnAck.get()) {
                log.debug("{} : Publisher thread takes over the draining control. Queue Size : {}, Demand : {}",
                        shardId, getRecordsResultQueue.size(), requestedResponses.get());
                shouldDrainEventOnlyOnAck.set(false);
            }
        }
    }

    @Accessors(fluent = true)
    @Data
    static class PrefetchRecordsRetrieved implements RecordsRetrieved {

        final ProcessRecordsInput processRecordsInput;
        final String lastBatchSequenceNumber;
        final String shardIterator;
        final BatchUniqueIdentifier batchUniqueIdentifier;

        PrefetchRecordsRetrieved prepareForPublish() {
            return new PrefetchRecordsRetrieved(processRecordsInput.toBuilder().cacheExitTime(Instant.now()).build(),
                    lastBatchSequenceNumber, shardIterator, batchUniqueIdentifier);
        }

        @Override
        public BatchUniqueIdentifier batchUniqueIdentifier() {
            return batchUniqueIdentifier;
        }

        /**
         * Generate batch unique identifier for PrefetchRecordsRetrieved, where flow will be empty.
         * @return BatchUniqueIdentifier
         */
        public static BatchUniqueIdentifier generateBatchUniqueIdentifier() {
            return new BatchUniqueIdentifier(UUID.randomUUID().toString(),
                    StringUtils.EMPTY);
        }
    }

    private String calculateHighestSequenceNumber(ProcessRecordsInput processRecordsInput) {
        String result = this.highestSequenceNumber;
        if (processRecordsInput.records() != null && !processRecordsInput.records().isEmpty()) {
            result = processRecordsInput.records().get(processRecordsInput.records().size() - 1).sequenceNumber();
        }
        return result;
    }

    private static class PositionResetException extends RuntimeException {

    }


    private class DefaultGetRecordsCacheDaemon implements Runnable {
        volatile boolean isShutdown = false;

        @Override
        public void run() {
            while (!isShutdown) {
                if (Thread.currentThread().isInterrupted()) {
                    log.warn("{} : Prefetch thread was interrupted.", shardId);
                    break;
                }

                resetLock.readLock().lock();
                try {
                    makeRetrievalAttempt();
                } catch(PositionResetException pre) {
                    log.debug("{} : Position was reset while attempting to add item to queue.", shardId);
                } finally {
                    resetLock.readLock().unlock();
                }


            }
            callShutdownOnStrategy();
        }

        private void makeRetrievalAttempt() {
            MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, operation);
            if (prefetchCounters.shouldGetNewRecords()) {
                try {
                    sleepBeforeNextCall();
                    GetRecordsResponse getRecordsResult = getRecordsRetrievalStrategy.getRecords(maxRecordsPerCall);
                    lastSuccessfulCall = Instant.now();

                    final List<KinesisClientRecord> records = getRecordsResult.records().stream()
                            .map(KinesisClientRecord::fromRecord).collect(Collectors.toList());
                    ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder()
                            .records(records)
                            .millisBehindLatest(getRecordsResult.millisBehindLatest())
                            .cacheEntryTime(lastSuccessfulCall)
                            .isAtShardEnd(getRecordsRetrievalStrategy.getDataFetcher().isShardEndReached())
                            .build();

                    highestSequenceNumber = calculateHighestSequenceNumber(processRecordsInput);
                    PrefetchRecordsRetrieved recordsRetrieved = new PrefetchRecordsRetrieved(processRecordsInput,
                            highestSequenceNumber, getRecordsResult.nextShardIterator(),
                            PrefetchRecordsRetrieved.generateBatchUniqueIdentifier());
                    highestSequenceNumber = recordsRetrieved.lastBatchSequenceNumber;
                    addArrivedRecordsInput(recordsRetrieved);
                    drainQueueForRequestsIfAllowed();
                } catch (PositionResetException pse) {
                    throw pse;
                } catch (RetryableRetrievalException rre) {
                    log.info("{} :  Timeout occurred while waiting for response from Kinesis.  Will retry the request.", shardId);
                } catch (InterruptedException e) {
                    log.info("{} :  Thread was interrupted, indicating shutdown was called on the cache.", shardId);
                } catch (ExpiredIteratorException e) {
                    log.info("{} :  records threw ExpiredIteratorException - restarting"
                            + " after greatest seqNum passed to customer", shardId, e);

                    scope.addData(EXPIRED_ITERATOR_METRIC, 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);

                    dataFetcher.restartIterator();
                } catch (SdkException e) {
                    log.error("{} :  Exception thrown while fetching records from Kinesis", shardId, e);
                } catch (Throwable e) {
                    log.error("{} :  Unexpected exception was thrown. This could probably be an issue or a bug." +
                            " Please search for the exception/error online to check what is going on. If the " +
                            "issue persists or is a recurring problem, feel free to open an issue on, " +
                            "https://github.com/awslabs/amazon-kinesis-client.", shardId, e);
                } finally {
                    MetricsUtil.endScope(scope);
                }
            } else {
                //
                // Consumer isn't ready to receive new records will allow prefetch counters to pause
                //
                try {
                    prefetchCounters.waitForConsumer();
                } catch (InterruptedException ie) {
                    log.info("{} :  Thread was interrupted while waiting for the consumer.  " +
                            "Shutdown has probably been started", shardId);
                }
            }
        }

        private void callShutdownOnStrategy() {
            if (!getRecordsRetrievalStrategy.isShutdown()) {
                getRecordsRetrievalStrategy.shutdown();
            }
        }

        private void sleepBeforeNextCall() throws InterruptedException {
            if (lastSuccessfulCall == null) {
                return;
            }
            long timeSinceLastCall = Duration.between(lastSuccessfulCall, Instant.now()).abs().toMillis();
            if (timeSinceLastCall < idleMillisBetweenCalls) {
                Thread.sleep(idleMillisBetweenCalls - timeSinceLastCall);
            }
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
            return result.records().stream().mapToLong(record -> record.data().limit()).sum();
        }

        public synchronized void waitForConsumer() throws InterruptedException {
            if (!shouldGetNewRecords()) {
                log.debug("{} : Queue is full waiting for consumer for {} ms", shardId, idleMillisBetweenCalls);
                this.wait(idleMillisBetweenCalls);
            }
        }

        public synchronized boolean shouldGetNewRecords() {
            if (log.isDebugEnabled()) {
                log.debug("{} : Current Prefetch Counter States: {}", shardId, this.toString());
            }
            return size < maxRecordsCount && byteSize < maxByteSize;
        }

        void reset() {
            size = 0;
            byteSize = 0;
        }

        @Override
        public String toString() {
            return String.format("{ Requests: %d, Records: %d, Bytes: %d }", getRecordsResultQueue.size(), size,
                    byteSize);
        }
    }

}
