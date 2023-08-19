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

package software.amazon.kinesis.retrieval.fanout;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;
import software.amazon.awssdk.utils.Either;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisRequestsBuilder;
import software.amazon.kinesis.common.RequestDetails;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.BatchUniqueIdentifier;
import software.amazon.kinesis.retrieval.IteratorBuilder;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RecordsDeliveryAck;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static software.amazon.kinesis.common.DiagnosticUtils.takeDelayedDeliveryActionIfRequired;
import static software.amazon.kinesis.retrieval.DataRetrievalUtil.isValidResult;

@Slf4j
@KinesisClientInternalApi
public class FanOutRecordsPublisher implements RecordsPublisher {
    private static final ThrowableCategory ACQUIRE_TIMEOUT_CATEGORY = new ThrowableCategory(
            ThrowableType.ACQUIRE_TIMEOUT);
    private static final ThrowableCategory READ_TIMEOUT_CATEGORY = new ThrowableCategory(ThrowableType.READ_TIMEOUT);
    // Max burst of 10 payload events + 1 terminal event (onError/onComplete) from the service.
    private static final int MAX_EVENT_BURST_FROM_SERVICE = 10 + 1;

    private final KinesisAsyncClient kinesis;
    private final String shardId;
    private final String consumerArn;
    private final String streamAndShardId;
    private final Object lockObject = new Object();

    private final AtomicInteger subscribeToShardId = new AtomicInteger(0);
    private RecordFlow flow;
    @Getter @VisibleForTesting
    private String currentSequenceNumber;
    private InitialPositionInStreamExtended initialPositionInStreamExtended;
    private boolean isFirstConnection = true;

    private Subscriber<? super RecordsRetrieved> subscriber;
    private long availableQueueSpace = 0;

    private BlockingQueue<RecordsRetrievedContext> recordsDeliveryQueue = new LinkedBlockingQueue<>(
            MAX_EVENT_BURST_FROM_SERVICE);

    private RequestDetails lastSuccessfulRequestDetails = new RequestDetails();

    public FanOutRecordsPublisher(KinesisAsyncClient kinesis, String shardId, String consumerArn) {
        this.kinesis = kinesis;
        this.shardId = shardId;
        this.consumerArn = consumerArn;
        this.streamAndShardId = shardId;
    }

    public FanOutRecordsPublisher(KinesisAsyncClient kinesis, String shardId, String consumerArn, String streamIdentifierSer) {
        this.kinesis = kinesis;
        this.shardId = shardId;
        this.consumerArn = consumerArn;
        this.streamAndShardId = streamIdentifierSer + ":" + shardId;
    }

    @Override
    public void start(ExtendedSequenceNumber extendedSequenceNumber,
            InitialPositionInStreamExtended initialPositionInStreamExtended) {
        synchronized (lockObject) {
            log.debug("[{}] Initializing Publisher @ Sequence: {} -- Initial Position: {}", streamAndShardId,
                    extendedSequenceNumber, initialPositionInStreamExtended);
            this.initialPositionInStreamExtended = initialPositionInStreamExtended;
            this.currentSequenceNumber = extendedSequenceNumber.sequenceNumber();
            this.isFirstConnection = true;
        }
    }

    @Override
    public void shutdown() {
        synchronized (lockObject) {
            if (flow != null) {
                flow.cancel();
            }
            flow = null;
        }
    }

    @Override
    public void restartFrom(RecordsRetrieved recordsRetrieved) {
        synchronized (lockObject) {
            if (flow != null) {
                //
                // The flow should not be running at this time
                //
                flow.cancel();
            }
            flow = null;
            if (!(recordsRetrieved instanceof FanoutRecordsRetrieved)) {
                throw new IllegalArgumentException(
                        "Provided ProcessRecordsInput not created from the FanOutRecordsPublisher");
            }
            currentSequenceNumber = ((FanoutRecordsRetrieved) recordsRetrieved).continuationSequenceNumber();
        }
    }

    @Override
    public void notify(RecordsDeliveryAck recordsDeliveryAck) {
        synchronized (lockObject) {
            RecordFlow triggeringFlow = null;
            try {
                triggeringFlow = evictAckedEventAndScheduleNextEvent(recordsDeliveryAck);
            } catch (Throwable t) {
                errorOccurred(triggeringFlow, t);
            }
            if (triggeringFlow != null) {
                updateAvailableQueueSpaceAndRequestUpstream(triggeringFlow);
            }
        }
    }

    @Override
    public RequestDetails getLastSuccessfulRequestDetails() {
        return lastSuccessfulRequestDetails;
    }

    private void setLastSuccessfulRequestDetails(RequestDetails requestDetails) {
        lastSuccessfulRequestDetails = requestDetails;
    }

    // This method is not thread-safe. You need to acquire a lock in the caller in order to execute this.
    @VisibleForTesting
    RecordFlow evictAckedEventAndScheduleNextEvent(RecordsDeliveryAck recordsDeliveryAck) {
        // Peek the head of the queue on receiving the ack.
        // Note : This does not block wait to retrieve an element.
        final RecordsRetrievedContext recordsRetrievedContext = recordsDeliveryQueue.peek();
        // RecordFlow of the current event that needs to be returned
        RecordFlow flowToBeReturned = null;

        final RecordsRetrieved recordsRetrieved = recordsRetrievedContext != null ?
                recordsRetrievedContext.getRecordsRetrieved() : null;

        // Check if the ack corresponds to the head of the delivery queue.
        if (recordsRetrieved != null && recordsRetrieved.batchUniqueIdentifier()
                .equals(recordsDeliveryAck.batchUniqueIdentifier())) {
            // It is now safe to remove the element
            recordsDeliveryQueue.poll();
            // Take action based on the time spent by the event in queue.
            takeDelayedDeliveryActionIfRequired(streamAndShardId, recordsRetrievedContext.getEnqueueTimestamp(), log);
            // Update current sequence number for the successfully delivered event.
            currentSequenceNumber = ((FanoutRecordsRetrieved) recordsRetrieved).continuationSequenceNumber();
            // Update the triggering flow for post scheduling upstream request.
            flowToBeReturned = recordsRetrievedContext.getRecordFlow();
            // Try scheduling the next event in the queue or execute the subscription shutdown action.
            if (!recordsDeliveryQueue.isEmpty()) {
                recordsDeliveryQueue.peek().executeEventAction(subscriber);
            }
        } else {
            // Check if the mismatched event belongs to active flow. If publisher receives an ack for a
            // missing event in active flow, then it means the event was already acked or cleared
            // from the queue due to a potential bug.
            if (flow != null && recordsDeliveryAck.batchUniqueIdentifier().getFlowIdentifier()
                    .equals(flow.getSubscribeToShardId())) {
                log.error(
                        "{}: Received unexpected ack for the active subscription {}. Throwing.",
                        streamAndShardId, recordsDeliveryAck.batchUniqueIdentifier().getFlowIdentifier());
                throw new IllegalStateException("Unexpected ack for the active subscription");
            }
            // Otherwise publisher received a stale ack.
            else {
                log.info("{}: Publisher received an ack for stale subscription {}. Ignoring.", streamAndShardId,
                        recordsDeliveryAck.batchUniqueIdentifier().getFlowIdentifier());
            }
        }
        return flowToBeReturned;
    }

    // This method is not thread-safe. You need to acquire a lock in the caller in order to execute this.
    @VisibleForTesting
    void bufferCurrentEventAndScheduleIfRequired(RecordsRetrieved recordsRetrieved, RecordFlow triggeringFlow) {
        final RecordsRetrievedContext recordsRetrievedContext =
                new RecordsRetrievedContext(Either.left(recordsRetrieved), triggeringFlow, Instant.now());
        try {
            // Try enqueueing the RecordsRetrieved batch to the queue, which would throw exception on failure.
            // Note: This does not block wait to enqueue.
            recordsDeliveryQueue.add(recordsRetrievedContext);
            // If the current batch is the only element in the queue, then try scheduling the event delivery.
            if (recordsDeliveryQueue.size() == 1) {
                subscriber.onNext(recordsRetrieved);
            }
        } catch (IllegalStateException e) {
            // CHECKSTYLE.OFF: LineLength
            log.warn("{}: Unable to enqueue the payload due to capacity restrictions in delivery queue with remaining capacity {}. Last successful request details -- {}",
                    // CHECKSTYLE.ON: LineLength
                    streamAndShardId, recordsDeliveryQueue.remainingCapacity(), lastSuccessfulRequestDetails);
            throw e;
        } catch (Throwable t) {
            log.error("{}: Unable to deliver event to the shard consumer.", streamAndShardId, t);
            throw t;
        }
    }

    @Data
    private static final class RecordsRetrievedContext {
        @Getter(AccessLevel.NONE)
        private final Either<RecordsRetrieved, SubscriptionShutdownEvent> recordsOrShutdownEvent;
        private final RecordFlow recordFlow;
        private final Instant enqueueTimestamp;

        RecordsRetrieved getRecordsRetrieved() {
            return recordsOrShutdownEvent.map(recordsEvent -> recordsEvent, shutdownEvent -> null);
        }

        // This method is not thread-safe. You need to acquire a lock in the caller in order to execute this.
        void executeEventAction(Subscriber<? super RecordsRetrieved> subscriber) {
            recordsOrShutdownEvent.apply(recordsEvent -> subscriber.onNext(recordsEvent),
                                         shutdownEvent -> shutdownEvent.getSubscriptionShutdownAction().run());
        }
    }

    @Getter
    private static final class SubscriptionShutdownEvent {
        private final Runnable subscriptionShutdownAction;
        private final String eventIdentifier;
        private final Throwable shutdownEventThrowableOptional;

        SubscriptionShutdownEvent(Runnable subscriptionShutdownAction, String eventIdentifier, Throwable shutdownEventThrowableOptional) {
            this.subscriptionShutdownAction = subscriptionShutdownAction;
            this.eventIdentifier = eventIdentifier;
            this.shutdownEventThrowableOptional = shutdownEventThrowableOptional;
        }

        SubscriptionShutdownEvent(Runnable subscriptionShutdownAction, String eventIdentifier) {
            this(subscriptionShutdownAction, eventIdentifier, null);
        }
    }

    private boolean hasValidSubscriber() {
        return subscriber != null;
    }

    private boolean hasValidFlow() {
        return flow != null;
    }

    private void subscribeToShard(String sequenceNumber) {
        synchronized (lockObject) {
            // Clear the delivery queue so that any stale entries from previous subscription are discarded.
            resetRecordsDeliveryStateOnSubscriptionOnInit();
            SubscribeToShardRequest.Builder builder = KinesisRequestsBuilder.subscribeToShardRequestBuilder()
                    .shardId(shardId).consumerARN(consumerArn);
            SubscribeToShardRequest request;
            if (isFirstConnection) {
                request = IteratorBuilder.request(builder, sequenceNumber, initialPositionInStreamExtended).build();
            } else {
                request = IteratorBuilder.reconnectRequest(builder, sequenceNumber, initialPositionInStreamExtended)
                        .build();
            }

            Instant connectionStart = Instant.now();
            int subscribeInvocationId = subscribeToShardId.incrementAndGet();
            String instanceId = shardId + "-" + subscribeInvocationId;
            log.debug(
                    "{}: [SubscriptionLifetime]: (FanOutRecordsPublisher#subscribeToShard) @ {} id: {} -- Starting subscribe to shard",
                    streamAndShardId, connectionStart, instanceId);
            flow = new RecordFlow(this, connectionStart, instanceId);
            kinesis.subscribeToShard(request, flow);
        }
    }

    private void errorOccurred(RecordFlow triggeringFlow, Throwable t) {
        synchronized (lockObject) {

            if (!hasValidSubscriber()) {
                if (hasValidFlow()) {
                    log.warn(
                            "{}: [SubscriptionLifetime] - (FanOutRecordsPublisher#errorOccurred) @ {} id: {} -- Subscriber is null." +
                                    " Last successful request details -- {}", streamAndShardId, flow.connectionStartedAt,
                            flow.subscribeToShardId, lastSuccessfulRequestDetails);
                } else {
                    log.warn(
                            "{}: [SubscriptionLifetime] - (FanOutRecordsPublisher#errorOccurred) -- Subscriber and flow are null." +
                                    " Last successful request details -- {}", streamAndShardId, lastSuccessfulRequestDetails);
                }
                return;
            }

            Throwable propagationThrowable = t;
            ThrowableCategory category = throwableCategory(propagationThrowable);

            if (isActiveFlow(triggeringFlow)) {
                if (flow != null) {
                    String logMessage = String.format(
                            "%s: [SubscriptionLifetime] - (FanOutRecordsPublisher#errorOccurred) @ %s id: %s -- %s." +
                                    " Last successful request details -- %s", streamAndShardId, flow.connectionStartedAt,
                            flow.subscribeToShardId, category.throwableTypeString, lastSuccessfulRequestDetails);
                    switch (category.throwableType) {
                    case READ_TIMEOUT:
                        log.debug(logMessage, propagationThrowable);
                        propagationThrowable = new RetryableRetrievalException(category.throwableTypeString,
                                (Exception) propagationThrowable.getCause());
                        break;
                    case ACQUIRE_TIMEOUT:
                        logAcquireTimeoutMessage(t);
                        //
                        // Fall through is intentional here as we still want to log the details of the exception
                        //
                    default:
                        log.warn(logMessage, propagationThrowable);

                    }
                    flow.cancel();
                }
                log.debug("{}: availableQueueSpace zeroing from {}", streamAndShardId, availableQueueSpace);
                availableQueueSpace = 0;

                try {
                    handleFlowError(propagationThrowable, triggeringFlow);
                } catch (Throwable innerThrowable) {
                    log.warn("{}: Exception while calling subscriber.onError. Last successful request details -- {}",
                            streamAndShardId, lastSuccessfulRequestDetails, innerThrowable);
                }
                subscriber = null;
                flow = null;
            } else {
                if (triggeringFlow != null) {
                    log.debug(
                            // CHECKSTYLE.OFF: LineLength
                            "{}: [SubscriptionLifetime] - (FanOutRecordsPublisher#errorOccurred) @ {} id: {} -- {} -> triggeringFlow wasn't the active flow.  Didn't dispatch error",
                            // CHECKSTYLE.ON: LineLength
                            streamAndShardId, triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId,
                            category.throwableTypeString);
                    triggeringFlow.cancel();
                }
            }

        }
    }

    // This method is not thread safe. This needs to be executed after acquiring lock on this.lockObject
    private void resetRecordsDeliveryStateOnSubscriptionOnInit() {
        // Clear any lingering records in the queue.
        if (!recordsDeliveryQueue.isEmpty()) {
            log.warn("{}: Found non-empty queue while starting subscription. This indicates unsuccessful clean up of "
                    + "previous subscription - {}. Last successful request details -- {}",
                    streamAndShardId, subscribeToShardId, lastSuccessfulRequestDetails);
            recordsDeliveryQueue.clear();
        }
    }

    protected void logAcquireTimeoutMessage(Throwable t) {
        log.error("An acquire timeout occurred which usually indicates that the KinesisAsyncClient supplied has a " +
                "low maximum streams limit.  " +
                "Please use the software.amazon.kinesis.common.KinesisClientUtil to setup the client, " +
                "or refer to the class to setup the client manually.");
    }

    private void handleFlowError(Throwable t, RecordFlow triggeringFlow) {
        if (t.getCause() instanceof ResourceNotFoundException) {
            log.debug(
                    "{}: Could not call SubscribeToShard successfully because shard no longer exists. Marking shard for completion.",
                    streamAndShardId);
            // The ack received for this onNext event will be ignored by the publisher as the global flow object should
            // be either null or renewed when the ack's flow identifier is evaluated.
            FanoutRecordsRetrieved response = new FanoutRecordsRetrieved(
                    ProcessRecordsInput.builder().records(Collections.emptyList()).isAtShardEnd(true)
                            .childShards(Collections.emptyList()).build(), null,
                    triggeringFlow != null ? triggeringFlow.getSubscribeToShardId() : shardId + "-no-flow-found");
            subscriber.onNext(response);
            subscriber.onComplete();
        } else {
            subscriber.onError(t);
        }
    }

    private enum ThrowableType {
        ACQUIRE_TIMEOUT("AcquireTimeout"), READ_TIMEOUT("ReadTimeout"), OTHER("Other");

        String value;

        ThrowableType(final String value) {
            this.value = value;
        }
    }

    private static class ThrowableCategory {
        @NonNull
        final ThrowableType throwableType;
        @NonNull
        final String throwableTypeString;

        ThrowableCategory(final ThrowableType throwableType) {
            this(throwableType, throwableType.value);
        }

        ThrowableCategory(final ThrowableType throwableType, final String throwableTypeString) {
            this.throwableType = throwableType;
            this.throwableTypeString = throwableTypeString;
        }
    }

    private ThrowableCategory throwableCategory(Throwable t) {
        Throwable current = t;
        StringBuilder builder = new StringBuilder();
        do {
            if (current.getMessage() != null && current.getMessage().startsWith("Acquire operation")) {
                return ACQUIRE_TIMEOUT_CATEGORY;
            }
            if (current.getClass().getName().equals("io.netty.handler.timeout.ReadTimeoutException")) {
                return READ_TIMEOUT_CATEGORY;
            }

            if (current.getCause() == null) {
                //
                // At the bottom
                //
                builder.append(current.getClass().getName()).append(": ").append(current.getMessage());
            } else {
                builder.append(current.getClass().getSimpleName());
                builder.append("/");
            }
            current = current.getCause();
        } while (current != null);
        return new ThrowableCategory(ThrowableType.OTHER, builder.toString());
    }

    private void recordsReceived(RecordFlow triggeringFlow, SubscribeToShardEvent recordBatchEvent) {
        synchronized (lockObject) {
            if (!hasValidSubscriber()) {
                log.debug(
                        "{}: [SubscriptionLifetime] (FanOutRecordsPublisher#recordsReceived) @ {} id: {} -- Subscriber is null.",
                        streamAndShardId, triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
                triggeringFlow.cancel();
                if (flow != null) {
                    flow.cancel();
                }
                return;
            }
            if (!isActiveFlow(triggeringFlow)) {
                log.debug(
                        "{}: [SubscriptionLifetime] (FanOutRecordsPublisher#recordsReceived) @ {} id: {} -- Received records for an inactive flow.",
                        streamAndShardId, triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
                return;
            }

            try {
                // If recordBatchEvent is not valid event, RuntimeException will be thrown here and trigger the errorOccurred call.
                // Since the triggeringFlow is active flow, it will then trigger the handleFlowError call.
                // Since the exception is not ResourceNotFoundException, it will trigger onError in the ShardConsumerSubscriber.
                // The ShardConsumerSubscriber will finally cancel the subscription.
                if (!isValidResult(recordBatchEvent.continuationSequenceNumber(), recordBatchEvent.childShards())) {
                    throw new InvalidStateException("RecordBatchEvent for flow " + triggeringFlow.toString() + " is invalid."
                                               + " event.continuationSequenceNumber: " + recordBatchEvent.continuationSequenceNumber()
                                               + ". event.childShards: " + recordBatchEvent.childShards());
                }

                List<KinesisClientRecord> records = recordBatchEvent.records().stream().map(KinesisClientRecord::fromRecord)
                                                                    .collect(Collectors.toList());
                ProcessRecordsInput input = ProcessRecordsInput.builder()
                                                               .cacheEntryTime(Instant.now())
                                                               .millisBehindLatest(recordBatchEvent.millisBehindLatest())
                                                               .isAtShardEnd(recordBatchEvent.continuationSequenceNumber() == null)
                                                               .records(records)
                                                               .childShards(recordBatchEvent.childShards())
                                                               .build();
                FanoutRecordsRetrieved recordsRetrieved = new FanoutRecordsRetrieved(input,
                        recordBatchEvent.continuationSequenceNumber(), triggeringFlow.subscribeToShardId);
                bufferCurrentEventAndScheduleIfRequired(recordsRetrieved, triggeringFlow);
            } catch (Throwable t) {
                log.warn("{}: Unable to buffer or schedule onNext for subscriber.  Failing publisher." +
                        " Last successful request details -- {}", streamAndShardId, lastSuccessfulRequestDetails);
                errorOccurred(triggeringFlow, t);
            }
        }
    }

    private void updateAvailableQueueSpaceAndRequestUpstream(RecordFlow triggeringFlow) {
        if (availableQueueSpace <= 0) {
            log.debug(
                    // CHECKSTYLE.OFF: LineLength
                    "{}: [SubscriptionLifetime] (FanOutRecordsPublisher#recordsReceived) @ {} id: {} -- Attempted to decrement availableQueueSpace to below 0",
                    // CHECKSTYLE.ON: LineLength
                    streamAndShardId, triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
        } else {
            availableQueueSpace--;
            if (availableQueueSpace > 0) {
                triggeringFlow.request(1);
            }
        }
    }

    private boolean shouldShutdownSubscriptionNow() {
        return recordsDeliveryQueue.isEmpty();
    }

    private void onComplete(RecordFlow triggeringFlow) {
        synchronized (lockObject) {
            log.debug("{}: [SubscriptionLifetime]: (FanOutRecordsPublisher#onComplete) @ {} id: {}", streamAndShardId,
                    triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);

            triggeringFlow.cancel();
            if (!hasValidSubscriber()) {
                log.debug("{}: [SubscriptionLifetime]: (FanOutRecordsPublisher#onComplete) @ {} id: {}",
                        streamAndShardId,
                        triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
                return;
            }

            if (!isActiveFlow(triggeringFlow)) {
                log.debug(
                        // CHECKSTYLE.OFF: LineLength
                        "{}: [SubscriptionLifetime]: (FanOutRecordsPublisher#onComplete) @ {} id: {} -- Received spurious onComplete from unexpected flow. Ignoring.",
                        // CHECKSTYLE.ON: LineLength
                        streamAndShardId, triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
                return;
            }

            if (currentSequenceNumber != null) {
                log.debug("{}: Shard hasn't ended. Resubscribing.", streamAndShardId);
                subscribeToShard(currentSequenceNumber);
            } else {
                log.debug("{}: Shard has ended completing subscriber.", streamAndShardId);
                subscriber.onComplete();
            }
        }
    }

    @Override
    public void subscribe(Subscriber<? super RecordsRetrieved> s) {
        synchronized (lockObject) {
            if (subscriber != null) {
                log.error(
                        "{}: A subscribe occurred while there was an active subscriber.  Sending error to current subscriber",
                        streamAndShardId);
                MultipleSubscriberException multipleSubscriberException = new MultipleSubscriberException();

                //
                // Notify current subscriber
                //
                subscriber.onError(multipleSubscriberException);
                subscriber = null;

                //
                // Notify attempted subscriber
                //
                s.onError(multipleSubscriberException);
                terminateExistingFlow();
                return;
            }
            terminateExistingFlow();

            subscriber = s;
            try {
                subscribeToShard(currentSequenceNumber);
            } catch (Throwable t) {
                errorOccurred(flow, t);
                return;
            }
            if (flow == null) {
                //
                // Failed to subscribe to a flow
                //
                errorOccurred(flow, new IllegalStateException("SubscribeToShard failed"));
                return;
            }
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    synchronized (lockObject) {
                        if (subscriber != s) {
                            log.warn(
                                    // CHECKSTYLE.OFF: LineLength
                                    "{}: (FanOutRecordsPublisher/Subscription#request) - Rejected an attempt to request({}), because subscribers don't match. Last successful request details -- {}",
                                    // CHECKSTYLE.ON: LineLength
                                    streamAndShardId, n, lastSuccessfulRequestDetails);
                            return;
                        }
                        if (flow == null) {
                            //
                            // Flow has been terminated, so we can't make any requests on it anymore.
                            //
                            log.debug(
                                    "{}: (FanOutRecordsPublisher/Subscription#request) - Request called for a null flow.",
                                    streamAndShardId);
                            errorOccurred(flow, new IllegalStateException("Attempted to request on a null flow."));
                            return;
                        }
                        long previous = availableQueueSpace;
                        availableQueueSpace += n;
                        if (previous <= 0) {
                            flow.request(1);
                        }
                    }
                }

                @Override
                public void cancel() {
                    synchronized (lockObject) {
                        if (subscriber != s) {
                            log.warn(
                                    // CHECKSTYLE.OFF: LineLength
                                    "{}: (FanOutRecordsPublisher/Subscription#cancel) - Rejected attempt to cancel subscription, because subscribers don't match. Last successful request details -- {}",
                                    // CHECKSTYLE.ON: LineLength
                                    streamAndShardId, lastSuccessfulRequestDetails);
                            return;
                        }
                        if (!hasValidSubscriber()) {
                            log.warn(
                                    // CHECKSTYLE.OFF: LineLength
                                    "{}: (FanOutRecordsPublisher/Subscription#cancel) - Cancelled called even with an invalid subscriber. Last successful request details -- {}",
                                    // CHECKSTYLE.ON: LineLength
                                    streamAndShardId, lastSuccessfulRequestDetails);
                        }
                        subscriber = null;
                        if (flow != null) {
                            log.debug(
                                    "{}: [SubscriptionLifetime]: (FanOutRecordsPublisher/Subscription#cancel) @ {} id: {}",
                                    streamAndShardId, flow.connectionStartedAt, flow.subscribeToShardId);
                            flow.cancel();
                            availableQueueSpace = 0;
                        }
                    }
                }
            });
        }
    }

    private void terminateExistingFlow() {
        if (flow != null) {
            RecordFlow current = flow;
            flow = null;
            current.cancel();
        }
    }

    private boolean isActiveFlow(RecordFlow requester) {
        synchronized (lockObject) {
            return requester == flow;
        }
    }

    private void rejectSubscription(SdkPublisher<SubscribeToShardEventStream> publisher) {
        publisher.subscribe(new Subscriber<SubscribeToShardEventStream>() {
            Subscription localSub;

            @Override
            public void onSubscribe(Subscription s) {
                localSub = s;
                localSub.cancel();
            }

            @Override
            public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
                localSub.cancel();
            }

            @Override
            public void onError(Throwable t) {
                localSub.cancel();
            }

            @Override
            public void onComplete() {
                localSub.cancel();
            }
        });
    }

    @Accessors(fluent = true)
    @Data
    static class FanoutRecordsRetrieved implements RecordsRetrieved {

        private final ProcessRecordsInput processRecordsInput;
        private final String continuationSequenceNumber;
        private final String flowIdentifier;
        private final String batchUniqueIdentifier = UUID.randomUUID().toString();

        @Override
        public ProcessRecordsInput processRecordsInput() {
            return processRecordsInput;
        }

        @Override
        public BatchUniqueIdentifier batchUniqueIdentifier() {
            return new BatchUniqueIdentifier(batchUniqueIdentifier, flowIdentifier);
        }
    }

    @RequiredArgsConstructor
    @Slf4j
    static class RecordFlow implements SubscribeToShardResponseHandler {

        private final FanOutRecordsPublisher parent;
        private final Instant connectionStartedAt;
        @Getter @VisibleForTesting
        private final String subscribeToShardId;

        private RecordSubscription subscription;
        private boolean isDisposed = false;
        private boolean isErrorDispatched = false;
        private boolean isCancelled = false;

        @Override
        public void onEventStream(SdkPublisher<SubscribeToShardEventStream> publisher) {
            synchronized (parent.lockObject) {
                log.debug("{}: [SubscriptionLifetime]: (RecordFlow#onEventStream)  @ {} id: {} -- Subscribe",
                        parent.streamAndShardId, connectionStartedAt, subscribeToShardId);
                if (!parent.isActiveFlow(this)) {
                    this.isDisposed = true;
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordFlow#onEventStream) @ {} id: {} -- parent is disposed",
                            parent.streamAndShardId, connectionStartedAt, subscribeToShardId);
                    parent.rejectSubscription(publisher);
                    return;
                }

                try {
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordFlow#onEventStream) @ {} id: {} -- creating record subscription",
                            parent.streamAndShardId, connectionStartedAt, subscribeToShardId);
                    subscription = new RecordSubscription(parent, this, connectionStartedAt, subscribeToShardId);
                    publisher.subscribe(subscription);

                    //
                    // Only flip this once we succeed
                    //
                    parent.isFirstConnection = false;
                } catch (Throwable t) {
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordFlow#onEventStream) @ {} id: {} -- throwable during record subscription: {}",
                            parent.streamAndShardId, connectionStartedAt, subscribeToShardId, t.getMessage());
                    parent.errorOccurred(this, t);
                }
            }
        }

        @Override
        public void responseReceived(SubscribeToShardResponse response) {
            log.debug("{}: [SubscriptionLifetime]: (RecordFlow#responseReceived) @ {} id: {} -- Response received. Request id - {}",
                    parent.streamAndShardId, connectionStartedAt, subscribeToShardId, response.responseMetadata().requestId());

            final RequestDetails requestDetails = new RequestDetails(response.responseMetadata().requestId(), connectionStartedAt.toString());
            parent.setLastSuccessfulRequestDetails(requestDetails);
        }

        @Override
        public void exceptionOccurred(Throwable throwable) {
            synchronized (parent.lockObject) {
                if (parent.shouldShutdownSubscriptionNow()) {
                    executeExceptionOccurred(throwable);
                } else {
                    final SubscriptionShutdownEvent subscriptionShutdownEvent = new SubscriptionShutdownEvent(
                            () -> {
                                parent.recordsDeliveryQueue.poll();
                                executeExceptionOccurred(throwable);
                                },
                            "onError", throwable);
                    tryEnqueueSubscriptionShutdownEvent(subscriptionShutdownEvent);
                }
            }
        }

        private void executeExceptionOccurred(Throwable throwable) {
            synchronized (parent.lockObject) {
                log.debug("{}: [SubscriptionLifetime]: (RecordFlow#exceptionOccurred) @ {} id: {} -- {}: {}",
                        parent.streamAndShardId, connectionStartedAt, subscribeToShardId, throwable.getClass().getName(),
                        throwable.getMessage());
                if (this.isDisposed) {
                    log.debug(
                            // CHECKSTYLE.OFF: LineLength
                            "{}: [SubscriptionLifetime]: (RecordFlow#exceptionOccurred) @ {} id: {} -- This flow has been disposed, not dispatching error. {}: {}",
                            // CHECKSTYLE.ON: LineLength
                            parent.streamAndShardId, connectionStartedAt, subscribeToShardId, throwable.getClass().getName(),
                            throwable.getMessage());
                    this.isErrorDispatched = true;
                }
                this.isDisposed = true;
                if (!isErrorDispatched) {
                    parent.errorOccurred(this, throwable);
                    isErrorDispatched = true;
                } else {
                    log.debug(
                            // CHECKSTYLE.OFF: LineLength
                            "{}: [SubscriptionLifetime]: (RecordFlow#exceptionOccurred) @ {} id: {} -- An error has previously been dispatched, not dispatching this error {}: {}",
                            // CHECKSTYLE.OFF: LineLength
                            parent.streamAndShardId, connectionStartedAt, subscribeToShardId, throwable.getClass().getName(),
                            throwable.getMessage());
                }
            }
        }

        @Override
        public void complete() {
            synchronized (parent.lockObject) {
                if (parent.shouldShutdownSubscriptionNow()) {
                    executeComplete();
                } else {
                    final SubscriptionShutdownEvent subscriptionShutdownEvent = new SubscriptionShutdownEvent(
                            () -> {
                                parent.recordsDeliveryQueue.poll();
                                executeComplete();
                                },
                            "onComplete");
                    tryEnqueueSubscriptionShutdownEvent(subscriptionShutdownEvent);
                }
            }
        }

        // This method is not thread safe. This needs to be executed after acquiring lock on parent.lockObject
        private void tryEnqueueSubscriptionShutdownEvent(SubscriptionShutdownEvent subscriptionShutdownEvent) {
            try {
                parent.recordsDeliveryQueue
                        .add(new RecordsRetrievedContext(Either.right(subscriptionShutdownEvent), this, Instant.now()));
            } catch (Exception e) {
                log.warn(
                        // CHECKSTYLE.OFF: LineLength
                        "{}: Unable to enqueue the {} shutdown event due to capacity restrictions in delivery queue with remaining capacity {}. Ignoring. Last successful request details -- {}",
                        // CHECKSTYLE.ON: LineLength
                        parent.streamAndShardId, subscriptionShutdownEvent.getEventIdentifier(), parent.recordsDeliveryQueue.remainingCapacity(),
                        parent.lastSuccessfulRequestDetails, subscriptionShutdownEvent.getShutdownEventThrowableOptional());
            }
        }

        private void executeComplete() {
            synchronized (parent.lockObject) {
                log.debug("{}: [SubscriptionLifetime]: (RecordFlow#complete) @ {} id: {} -- Connection completed",
                        parent.streamAndShardId, connectionStartedAt, subscribeToShardId);

                if (isCancelled) {
                    //
                    // The SDK currently calls onComplete when the subscription is cancelled, which we really don't
                    // want to do. When that happens we don't want to call the parent onComplete since that will restart
                    // the
                    // subscription, which was cancelled for a reason (usually queue overflow).
                    //
                    log.warn("{}: complete called on a cancelled subscription. Ignoring completion. Last successful request details -- {}",
                            parent.streamAndShardId, parent.lastSuccessfulRequestDetails);
                    return;
                }
                if (this.isDisposed) {
                    log.warn(
                            // CHECKSTYLE.OFF: LineLength
                            "{}: [SubscriptionLifetime]: (RecordFlow#complete) @ {} id: {} -- This flow has been disposed not dispatching completion. Last successful request details -- {}",
                            // CHECKSTYLE.ON: LineLength
                            parent.streamAndShardId, connectionStartedAt, subscribeToShardId, parent.lastSuccessfulRequestDetails);
                    return;
                }

                parent.onComplete(this);
            }
        }

        public void cancel() {
            synchronized (parent.lockObject) {
                this.isDisposed = true;
                this.isCancelled = true;
                if (subscription != null) {
                    try {
                        subscription.cancel();
                    } catch (Throwable t) {
                        log.error(
                                // CHECKSTYLE.OFF: LineLength
                                "{}: [SubscriptionLifetime]: (RecordFlow#complete) @ {} id: {} -- Exception while trying to cancel failed subscription: {}",
                                // CHECKSTYLE.ON: LineLength
                                parent.streamAndShardId, connectionStartedAt, subscribeToShardId, t.getMessage(), t);
                    }
                }
            }
        }

        private boolean shouldSubscriptionCancel() {
            return this.isDisposed || this.isCancelled || !parent.isActiveFlow(this);
        }

        public void request(long n) {
            if (subscription != null && !shouldSubscriptionCancel()) {
                subscription.request(n);
            }
        }

        private void recordsReceived(SubscribeToShardEvent event) {
            parent.recordsReceived(this, event);
        }
    }

    @RequiredArgsConstructor
    @Slf4j
    static class RecordSubscription implements Subscriber<SubscribeToShardEventStream> {

        private final FanOutRecordsPublisher parent;
        private final RecordFlow flow;
        private final Instant connectionStartedAt;
        private final String subscribeToShardId;

        private Subscription subscription;

        public void request(long n) {
            synchronized (parent.lockObject) {
                subscription.request(n);
            }
        }

        public void cancel() {
            synchronized (parent.lockObject) {
                log.debug("{}: [SubscriptionLifetime]: (RecordSubscription#cancel) @ {} id: {} -- Cancel called",
                        parent.streamAndShardId, connectionStartedAt, subscribeToShardId);
                flow.isCancelled = true;
                if (subscription != null) {
                    subscription.cancel();
                } else {
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordSubscription#cancel) @ {} id: {} -- SDK subscription is null",
                            parent.streamAndShardId, connectionStartedAt, subscribeToShardId);
                }
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            synchronized (parent.lockObject) {
                subscription = s;

                if (flow.shouldSubscriptionCancel()) {
                    if (flow.isCancelled) {
                        log.debug(
                                // CHECKSTYLE.OFF: LineLength
                                "{}: [SubscriptionLifetime]: (RecordSubscription#onSubscribe) @ {} id: {} -- Subscription was cancelled before onSubscribe",
                                // CHECKSTYLE.ON: LineLength
                                parent.streamAndShardId, connectionStartedAt, subscribeToShardId);
                    }
                    if (flow.isDisposed) {
                        log.debug(
                                // CHECKSTYLE.OFF: LineLength
                                "{}: [SubscriptionLifetime]: (RecordSubscription#onSubscribe) @ {} id: {} -- RecordFlow has been disposed cancelling subscribe",
                                // CHECKSTYLE.ON: LineLength
                                parent.streamAndShardId, connectionStartedAt, subscribeToShardId);
                    }
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordSubscription#onSubscribe) @ {} id: {} -- RecordFlow requires cancelling",
                            parent.streamAndShardId, connectionStartedAt, subscribeToShardId);
                    cancel();
                }
                log.debug(
                        "{}: [SubscriptionLifetime]: (RecordSubscription#onSubscribe) @ {} id: {} -- Outstanding: {} items so requesting an item",
                        parent.streamAndShardId, connectionStartedAt, subscribeToShardId, parent.availableQueueSpace);
                if (parent.availableQueueSpace > 0) {
                    request(1);
                }
            }
        }

        @Override
        public void onNext(SubscribeToShardEventStream recordBatchEvent) {
            synchronized (parent.lockObject) {
                if (flow.shouldSubscriptionCancel()) {
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordSubscription#onNext) @ {} id: {} -- RecordFlow requires cancelling",
                            parent.streamAndShardId, connectionStartedAt, subscribeToShardId);
                    cancel();
                    return;
                }
                recordBatchEvent.accept(new SubscribeToShardResponseHandler.Visitor() {
                    @Override
                    public void visit(SubscribeToShardEvent event) {
                        flow.recordsReceived(event);
                    }
                });
            }
        }

        @Override
        public void onError(Throwable t) {
            log.debug("{}: [SubscriptionLifetime]: (RecordSubscription#onError) @ {} id: {} -- {}: {}", parent.streamAndShardId,
                    connectionStartedAt, subscribeToShardId, t.getClass().getName(), t.getMessage());

            //
            // We don't propagate the throwable, as the SDK will call
            // SubscribeToShardResponseHandler#exceptionOccurred()
            //
        }

        @Override
        public void onComplete() {
            log.debug(
                    "{}: [SubscriptionLifetime]: (RecordSubscription#onComplete) @ {} id: {} -- Allowing RecordFlow to call onComplete",
                    parent.streamAndShardId, connectionStartedAt, subscribeToShardId);
        }
    }
}
