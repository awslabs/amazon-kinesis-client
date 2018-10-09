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

package software.amazon.kinesis.retrieval.fanout;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisRequestsBuilder;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.IteratorBuilder;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@RequiredArgsConstructor
@Slf4j
@KinesisClientInternalApi
public class FanOutRecordsPublisher implements RecordsPublisher {
    private static final ThrowableCategory ACQUIRE_TIMEOUT_CATEGORY = new ThrowableCategory(
            ThrowableType.ACQUIRE_TIMEOUT);
    private static final ThrowableCategory READ_TIMEOUT_CATEGORY = new ThrowableCategory(ThrowableType.READ_TIMEOUT);

    private final KinesisAsyncClient kinesis;
    private final String shardId;
    private final String consumerArn;

    private final Object lockObject = new Object();

    private final AtomicInteger subscribeToShardId = new AtomicInteger(0);

    private RecordFlow flow;

    private String currentSequenceNumber;
    private InitialPositionInStreamExtended initialPositionInStreamExtended;
    private boolean isFirstConnection = true;

    private Subscriber<? super ProcessRecordsInput> subscriber;
    private long availableQueueSpace = 0;

    @Override
    public void start(ExtendedSequenceNumber extendedSequenceNumber,
            InitialPositionInStreamExtended initialPositionInStreamExtended) {
        synchronized (lockObject) {
            log.debug("[{}] Initializing Publisher @ Sequence: {} -- Initial Position: {}", shardId,
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

    private boolean hasValidSubscriber() {
        return subscriber != null;
    }

    private void subscribeToShard(String sequenceNumber) {
        synchronized (lockObject) {
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
                    shardId, connectionStart, instanceId);
            flow = new RecordFlow(this, connectionStart, instanceId);
            kinesis.subscribeToShard(request, flow);
        }
    }

    private void errorOccurred(RecordFlow triggeringFlow, Throwable t) {
        synchronized (lockObject) {
            if (!hasValidSubscriber()) {
                log.warn(
                        "{}: [SubscriptionLifetime] - (FanOutRecordsPublisher#errorOccurred) @ {} id: {} -- Subscriber is null",
                        shardId, flow.connectionStartedAt, flow.subscribeToShardId);
                return;
            }
            Throwable propagationThrowable = t;
            ThrowableCategory category = throwableCategory(propagationThrowable);

            if (isActiveFlow(triggeringFlow)) {
                if (flow != null) {
                    String logMessage = String.format(
                            "%s: [SubscriptionLifetime] - (FanOutRecordsPublisher#errorOccurred) @ %s id: %s -- %s",
                            shardId, flow.connectionStartedAt, flow.subscribeToShardId, category.throwableTypeString);
                    if (category.throwableType.equals(ThrowableType.READ_TIMEOUT)) {
                        log.debug(logMessage, propagationThrowable);
                        propagationThrowable = new RetryableRetrievalException(category.throwableTypeString,
                                (Exception) propagationThrowable.getCause());
                    } else {
                        log.warn(logMessage, propagationThrowable);
                    }

                    flow.cancel();
                }
                log.debug("{}: availableQueueSpace zeroing from {}", shardId, availableQueueSpace);
                availableQueueSpace = 0;

                try {
                    handleFlowError(propagationThrowable);
                } catch (Throwable innerThrowable) {
                    log.warn("{}: Exception while calling subscriber.onError", shardId, innerThrowable);
                }
                subscriber = null;
                flow = null;
            } else {
                if (triggeringFlow != null) {
                    log.debug(
                            "{}: [SubscriptionLifetime] - (FanOutRecordsPublisher#errorOccurred) @ {} id: {} -- {} -> triggeringFlow wasn't the active flow.  Didn't dispatch error",
                            shardId, triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId,
                            category.throwableTypeString);
                    triggeringFlow.cancel();
                }
            }

        }
    }

    private void handleFlowError(Throwable t) {
        if (t.getCause() instanceof ResourceNotFoundException) {
            log.debug(
                    "{}: Could not call SubscribeToShard successfully because shard no longer exists. Marking shard for completion.",
                    shardId);
            subscriber
                    .onNext(ProcessRecordsInput.builder().records(Collections.emptyList()).isAtShardEnd(true).build());
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
                        shardId, triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
                triggeringFlow.cancel();
                if (flow != null) {
                    flow.cancel();
                }
                return;
            }
            if (!isActiveFlow(triggeringFlow)) {
                log.debug(
                        "{}: [SubscriptionLifetime] (FanOutRecordsPublisher#recordsReceived) @ {} id: {} -- Received records for an inactive flow.",
                        shardId, triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
                return;
            }

            List<KinesisClientRecord> records = recordBatchEvent.records().stream().map(KinesisClientRecord::fromRecord)
                    .collect(Collectors.toList());
            ProcessRecordsInput input = ProcessRecordsInput.builder().cacheEntryTime(Instant.now())
                    .millisBehindLatest(recordBatchEvent.millisBehindLatest())
                    .isAtShardEnd(recordBatchEvent.continuationSequenceNumber() == null).records(records).build();

            try {
                subscriber.onNext(input);
                //
                // Only advance the currentSequenceNumber if we successfully dispatch the last received input
                //
                currentSequenceNumber = recordBatchEvent.continuationSequenceNumber();
            } catch (Throwable t) {
                log.warn("{}: Unable to call onNext for subscriber.  Failing publisher.", shardId);
                errorOccurred(triggeringFlow, t);
            }

            if (availableQueueSpace <= 0) {
                log.debug(
                        "{}: [SubscriptionLifetime] (FanOutRecordsPublisher#recordsReceived) @ {} id: {} -- Attempted to decrement availableQueueSpace to below 0",
                        shardId, triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
            } else {
                availableQueueSpace--;
                if (availableQueueSpace > 0) {
                    triggeringFlow.request(1);
                }
            }
        }
    }

    private void onComplete(RecordFlow triggeringFlow) {
        synchronized (lockObject) {
            log.debug("{}: [SubscriptionLifetime]: (FanOutRecordsPublisher#onComplete) @ {} id: {}", shardId,
                    triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
            triggeringFlow.cancel();
            if (!hasValidSubscriber()) {
                log.debug("{}: [SubscriptionLifetime]: (FanOutRecordsPublisher#onComplete) @ {} id: {}", shardId,
                        triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
                return;
            }

            if (!isActiveFlow(triggeringFlow)) {
                log.debug(
                        "{}: [SubscriptionLifetime]: (FanOutRecordsPublisher#onComplete) @ {} id: {} -- Received spurious onComplete from unexpected flow. Ignoring.",
                        shardId, triggeringFlow.connectionStartedAt, triggeringFlow.subscribeToShardId);
                return;
            }

            if (currentSequenceNumber != null) {
                log.debug("{}: Shard hasn't ended resubscribing.", shardId);
                subscribeToShard(currentSequenceNumber);
            } else {
                log.debug("{}: Shard has ended completing subscriber.", shardId);
                subscriber.onComplete();
            }
        }
    }

    @Override
    public void subscribe(Subscriber<? super ProcessRecordsInput> s) {
        synchronized (lockObject) {
            if (subscriber != null) {
                log.error(
                        "{}: A subscribe occurred while there was an active subscriber.  Sending error to current subscriber",
                        shardId);
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
                                    "{}: (FanOutRecordsPublisher/Subscription#request) - Rejected an attempt to request({}), because subscribers don't match.",
                                    shardId, n);
                            return;
                        }
                        if (flow == null) {
                            //
                            // Flow has been terminated, so we can't make any requests on it anymore.
                            //
                            log.debug(
                                    "{}: (FanOutRecordsPublisher/Subscription#request) - Request called for a null flow.",
                                    shardId);
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
                                    "{}: (FanOutRecordsPublisher/Subscription#cancel) - Rejected attempt to cancel subscription, because subscribers don't match.",
                                    shardId);
                            return;
                        }
                        if (!hasValidSubscriber()) {
                            log.warn(
                                    "{}: (FanOutRecordsPublisher/Subscription#cancel) - Cancelled called even with an invalid subscriber",
                                    shardId);
                        }
                        subscriber = null;
                        if (flow != null) {
                            log.debug("{}: [SubscriptionLifetime]: (FanOutRecordsPublisher/Subscription#cancel) @ {} id: {}",
                                    shardId, flow.connectionStartedAt, flow.subscribeToShardId);
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

    @RequiredArgsConstructor
    @Slf4j
    static class RecordFlow implements SubscribeToShardResponseHandler {

        private final FanOutRecordsPublisher parent;
        private final Instant connectionStartedAt;
        private final String subscribeToShardId;

        private RecordSubscription subscription;
        private boolean isDisposed = false;
        private boolean isErrorDispatched = false;
        private boolean isCancelled = false;

        @Override
        public void onEventStream(SdkPublisher<SubscribeToShardEventStream> publisher) {
            synchronized (parent.lockObject) {
                log.debug("{}: [SubscriptionLifetime]: (RecordFlow#onEventStream)  @ {} id: {} -- Subscribe",
                        parent.shardId, connectionStartedAt, subscribeToShardId);
                if (!parent.isActiveFlow(this)) {
                    this.isDisposed = true;
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordFlow#onEventStream) @ {} id: {} -- parent is disposed",
                            parent.shardId, connectionStartedAt, subscribeToShardId);
                    parent.rejectSubscription(publisher);
                    return;
                }

                try {
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordFlow#onEventStream) @ {} id: {} -- creating record subscription",
                            parent.shardId, connectionStartedAt, subscribeToShardId);
                    subscription = new RecordSubscription(parent, this, connectionStartedAt, subscribeToShardId);
                    publisher.subscribe(subscription);

                    //
                    // Only flip this once we succeed
                    //
                    parent.isFirstConnection = false;
                } catch (Throwable t) {
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordFlow#onEventStream) @ {} id: {} -- throwable during record subscription: {}",
                            parent.shardId, connectionStartedAt, subscribeToShardId, t.getMessage());
                    parent.errorOccurred(this, t);
                }
            }
        }

        @Override
        public void responseReceived(SubscribeToShardResponse response) {
            log.debug("{}: [SubscriptionLifetime]: (RecordFlow#responseReceived) @ {} id: {} -- Response received",
                    parent.shardId, connectionStartedAt, subscribeToShardId);
        }

        @Override
        public void exceptionOccurred(Throwable throwable) {
            synchronized (parent.lockObject) {

                log.debug("{}: [SubscriptionLifetime]: (RecordFlow#exceptionOccurred) @ {} id: {} -- {}: {}",
                        parent.shardId, connectionStartedAt, subscribeToShardId, throwable.getClass().getName(),
                        throwable.getMessage());
                if (this.isDisposed) {
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordFlow#exceptionOccurred) @ {} id: {} -- This flow has been disposed, not dispatching error. {}: {}",
                            parent.shardId, connectionStartedAt, subscribeToShardId, throwable.getClass().getName(),
                            throwable.getMessage());
                    this.isErrorDispatched = true;
                }
                this.isDisposed = true;
                if (!isErrorDispatched) {
                    parent.errorOccurred(this, throwable);
                    isErrorDispatched = true;
                } else {
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordFlow#exceptionOccurred) @ {} id: {} -- An error has previously been dispatched, not dispatching this error {}: {}",
                            parent.shardId, connectionStartedAt, subscribeToShardId, throwable.getClass().getName(),
                            throwable.getMessage());
                }
            }
        }

        @Override
        public void complete() {
            synchronized (parent.lockObject) {
                log.debug("{}: [SubscriptionLifetime]: (RecordFlow#complete) @ {} id: {} -- Connection completed",
                        parent.shardId, connectionStartedAt, subscribeToShardId);

                if (isCancelled) {
                    //
                    // The SDK currently calls onComplete when the subscription is cancelled, which we really don't
                    // want to do. When that happens we don't want to call the parent onComplete since that will restart
                    // the
                    // subscription, which was cancelled for a reason (usually queue overflow).
                    //
                    log.warn("{}: complete called on a cancelled subscription.  Ignoring completion", parent.shardId);
                    return;
                }
                if (this.isDisposed) {
                    log.warn(
                            "{}: [SubscriptionLifetime]: (RecordFlow#complete) @ {} id: {} -- This flow has been disposed not dispatching completion",
                            parent.shardId, connectionStartedAt, subscribeToShardId);
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
                                "{}: [SubscriptionLifetime]: (RecordFlow#complete) @ {} id: {} -- Exception while trying to cancel failed subscription: {}",
                                parent.shardId, connectionStartedAt, subscribeToShardId, t.getMessage(), t);
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
                        parent.shardId, connectionStartedAt, subscribeToShardId);
                flow.isCancelled = true;
                if (subscription != null) {
                    subscription.cancel();
                } else {
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordSubscription#cancel) @ {} id: {} -- SDK subscription is null",
                            parent.shardId, connectionStartedAt, subscribeToShardId);
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
                                "{}: [SubscriptionLifetime]: (RecordSubscription#onSubscribe) @ {} id: {} -- Subscription was cancelled before onSubscribe",
                                parent.shardId, connectionStartedAt, subscribeToShardId);
                    }
                    if (flow.isDisposed) {
                        log.debug(
                                "{}: [SubscriptionLifetime]: (RecordSubscription#onSubscribe) @ {} id: {} -- RecordFlow has been disposed cancelling subscribe",
                                parent.shardId, connectionStartedAt, subscribeToShardId);
                    }
                    log.debug(
                            "{}: [SubscriptionLifetime]: (RecordSubscription#onSubscribe) @ {} id: {} -- RecordFlow requires cancelling",
                            parent.shardId, connectionStartedAt, subscribeToShardId);
                    cancel();
                }
                log.debug(
                        "{}: [SubscriptionLifetime]: (RecordSubscription#onSubscribe) @ {} id: {} -- Outstanding: {} items so requesting an item",
                        parent.shardId, connectionStartedAt, subscribeToShardId, parent.availableQueueSpace);
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
                            parent.shardId, connectionStartedAt, subscribeToShardId);
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
            log.debug("{}: [SubscriptionLifetime]: (RecordSubscription#onError) @ {} id: {} -- {}: {}", parent.shardId,
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
                    parent.shardId, connectionStartedAt, subscribeToShardId);

        }
    }

}
