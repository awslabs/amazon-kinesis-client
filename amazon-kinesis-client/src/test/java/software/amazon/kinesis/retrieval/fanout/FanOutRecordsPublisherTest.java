package software.amazon.kinesis.retrieval.fanout;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.handler.timeout.ReadTimeoutException;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subscribers.SafeSubscriber;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.leases.ShardObjectHelper;
import software.amazon.kinesis.lifecycle.ShardConsumerNotifyingSubscriber;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.BatchUniqueIdentifier;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.utils.SubscribeToShardRequestMatcher;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class FanOutRecordsPublisherTest {

    private static final String SHARD_ID = "Shard-001";
    private static final String CONSUMER_ARN = "arn:consumer";
    private static final String CONTINUATION_SEQUENCE_NUMBER = "continuationSequenceNumber";

    @Mock
    private KinesisAsyncClient kinesisClient;

    @Mock
    private SdkPublisher<SubscribeToShardEventStream> publisher;

    @Mock
    private Subscription subscription;

    @Mock
    private Subscriber<RecordsRetrieved> subscriber;

    private SubscribeToShardEvent batchEvent;

    @Test
    public void testSimple() {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        source.subscribe(new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        fail("Caught throwable in subscriber");
                    }

                    @Override
                    public void onComplete() {
                        fail("OnComplete called when not expected");
                    }
                },
                source));

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        batchEvent = SubscribeToShardEvent.builder()
                .millisBehindLatest(100L)
                .records(records)
                .continuationSequenceNumber("test")
                .childShards(Collections.emptyList())
                .build();

        captor.getValue().onNext(batchEvent);
        captor.getValue().onNext(batchEvent);
        captor.getValue().onNext(batchEvent);

        verify(subscription, times(4)).request(1);
        assertThat(receivedInput.size(), equalTo(3));

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });
    }

    @Test
    public void testInvalidEvent() {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        source.subscribe(new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        fail("Caught throwable in subscriber");
                    }

                    @Override
                    public void onComplete() {
                        fail("OnComplete called when not expected");
                    }
                },
                source));

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        batchEvent = SubscribeToShardEvent.builder()
                .millisBehindLatest(100L)
                .records(records)
                .continuationSequenceNumber(CONTINUATION_SEQUENCE_NUMBER)
                .build();
        SubscribeToShardEvent invalidEvent = SubscribeToShardEvent.builder()
                .millisBehindLatest(100L)
                .records(records)
                .childShards(Collections.emptyList())
                .build();

        captor.getValue().onNext(batchEvent);
        captor.getValue().onNext(invalidEvent);
        captor.getValue().onNext(batchEvent);

        // When the second request failed with invalid event, it should stop sending requests and cancel the flow.
        verify(subscription, times(2)).request(1);
        assertThat(receivedInput.size(), equalTo(1));

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });
    }

    @Test
    public void testIfAllEventsReceivedWhenNoTasksRejectedByExecutor() {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        Subscriber<RecordsRetrieved> shardConsumerSubscriber = new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        fail("Caught throwable in subscriber");
                    }

                    @Override
                    public void onComplete() {
                        fail("OnComplete called when not expected");
                    }
                },
                source);

        Scheduler testScheduler = getScheduler(getBlockingExecutor(getSpiedExecutor(getTestExecutor())));
        int bufferSize = 8;

        Flowable.fromPublisher(source)
                .subscribeOn(testScheduler)
                .observeOn(testScheduler, true, bufferSize)
                .subscribe(shardConsumerSubscriber);

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        Stream.of("1000", "2000", "3000")
                .map(contSeqNum -> SubscribeToShardEvent.builder()
                        .millisBehindLatest(100L)
                        .continuationSequenceNumber(contSeqNum)
                        .records(records)
                        .childShards(Collections.emptyList())
                        .build())
                .forEach(batchEvent -> captor.getValue().onNext(batchEvent));

        verify(subscription, times(4)).request(1);
        assertThat(receivedInput.size(), equalTo(3));

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });

        assertThat(source.getCurrentSequenceNumber(), equalTo("3000"));
    }

    @Test
    public void testIfEventsAreNotDeliveredToShardConsumerWhenPreviousEventDeliveryTaskGetsRejected() {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        Subscriber<RecordsRetrieved> shardConsumerSubscriber = new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        fail("Caught throwable in subscriber");
                    }

                    @Override
                    public void onComplete() {
                        fail("OnComplete called when not expected");
                    }
                },
                source);

        Scheduler testScheduler = getScheduler(getOverwhelmedBlockingExecutor(getSpiedExecutor(getTestExecutor())));
        int bufferSize = 8;

        Flowable.fromPublisher(source)
                .subscribeOn(testScheduler)
                .observeOn(testScheduler, true, bufferSize)
                .subscribe(new SafeSubscriber<>(shardConsumerSubscriber));

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        Stream.of("1000", "2000", "3000")
                .map(contSeqNum -> SubscribeToShardEvent.builder()
                        .millisBehindLatest(100L)
                        .continuationSequenceNumber(contSeqNum)
                        .records(records)
                        .childShards(Collections.emptyList())
                        .build())
                .forEach(batchEvent -> captor.getValue().onNext(batchEvent));

        verify(subscription, times(2)).request(1);
        assertThat(receivedInput.size(), equalTo(1));

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });

        assertThat(source.getCurrentSequenceNumber(), equalTo("1000"));
    }

    @Test
    public void testIfStreamOfEventsAreDeliveredInOrderWithBackpressureAdheringServicePublisher() throws Exception {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());

        Consumer<Integer> servicePublisherAction = contSeqNum -> captor.getValue()
                .onNext(SubscribeToShardEvent.builder()
                        .millisBehindLatest(100L)
                        .continuationSequenceNumber(contSeqNum + "")
                        .records(records)
                        .childShards(Collections.emptyList())
                        .build());

        CountDownLatch servicePublisherTaskCompletionLatch = new CountDownLatch(2);
        int totalServicePublisherEvents = 1000;
        int initialDemand = 0;
        BackpressureAdheringServicePublisher servicePublisher = new BackpressureAdheringServicePublisher(
                servicePublisherAction,
                totalServicePublisherEvents,
                servicePublisherTaskCompletionLatch,
                initialDemand);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        Subscriber<RecordsRetrieved> shardConsumerSubscriber = new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    private Subscription subscription;
                    private int lastSeenSeqNum = 0;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                        servicePublisher.request(1);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        assertEquals(
                                "" + ++lastSeenSeqNum,
                                ((FanOutRecordsPublisher.FanoutRecordsRetrieved) input).continuationSequenceNumber());
                        subscription.request(1);
                        servicePublisher.request(1);
                        if (receivedInput.size() == totalServicePublisherEvents) {
                            servicePublisherTaskCompletionLatch.countDown();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        fail("Caught throwable in subscriber");
                    }

                    @Override
                    public void onComplete() {
                        fail("OnComplete called when not expected");
                    }
                },
                source);

        ExecutorService executorService = getTestExecutor();
        Scheduler testScheduler = getScheduler(getInitiallyBlockingExecutor(getSpiedExecutor(executorService)));
        int bufferSize = 8;

        Flowable.fromPublisher(source)
                .subscribeOn(testScheduler)
                .observeOn(testScheduler, true, bufferSize)
                .subscribe(shardConsumerSubscriber);

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        executorService.submit(servicePublisher);
        servicePublisherTaskCompletionLatch.await(5000, TimeUnit.MILLISECONDS);

        assertThat(receivedInput.size(), equalTo(totalServicePublisherEvents));

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });

        assertThat(source.getCurrentSequenceNumber(), equalTo(totalServicePublisherEvents + ""));
    }

    @Test
    public void testIfStreamOfEventsAndOnCompleteAreDeliveredInOrderWithBackpressureAdheringServicePublisher()
            throws Exception {
        CountDownLatch onS2SCallLatch = new CountDownLatch(2);

        doAnswer(new Answer() {
                    @Override
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        onS2SCallLatch.countDown();
                        return null;
                    }
                })
                .when(kinesisClient)
                .subscribeToShard(any(SubscribeToShardRequest.class), any());

        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());

        Consumer<Integer> servicePublisherAction = contSeqNum -> captor.getValue()
                .onNext(SubscribeToShardEvent.builder()
                        .millisBehindLatest(100L)
                        .continuationSequenceNumber(contSeqNum + "")
                        .records(records)
                        .childShards(Collections.emptyList())
                        .build());

        CountDownLatch servicePublisherTaskCompletionLatch = new CountDownLatch(2);
        int totalServicePublisherEvents = 1000;
        int initialDemand = 9;
        int triggerCompleteAtNthEvent = 200;
        BackpressureAdheringServicePublisher servicePublisher = new BackpressureAdheringServicePublisher(
                servicePublisherAction,
                totalServicePublisherEvents,
                servicePublisherTaskCompletionLatch,
                initialDemand);
        servicePublisher.setCompleteTrigger(
                triggerCompleteAtNthEvent, () -> flowCaptor.getValue().complete());

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        Subscriber<RecordsRetrieved> shardConsumerSubscriber = new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    private Subscription subscription;
                    private int lastSeenSeqNum = 0;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                        servicePublisher.request(1);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        assertEquals(
                                "" + ++lastSeenSeqNum,
                                ((FanOutRecordsPublisher.FanoutRecordsRetrieved) input).continuationSequenceNumber());
                        subscription.request(1);
                        servicePublisher.request(1);
                        if (receivedInput.size() == triggerCompleteAtNthEvent) {
                            servicePublisherTaskCompletionLatch.countDown();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        fail("Caught throwable in subscriber");
                    }

                    @Override
                    public void onComplete() {
                        fail("OnComplete called when not expected");
                    }
                },
                source);

        ExecutorService executorService = getTestExecutor();
        Scheduler testScheduler = getScheduler(getInitiallyBlockingExecutor(getSpiedExecutor(executorService)));
        int bufferSize = 8;

        Flowable.fromPublisher(source)
                .subscribeOn(testScheduler)
                .observeOn(testScheduler, true, bufferSize)
                .subscribe(shardConsumerSubscriber);

        verify(kinesisClient, times(1)).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());

        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        executorService.submit(servicePublisher);
        servicePublisherTaskCompletionLatch.await(5000, TimeUnit.MILLISECONDS);

        assertThat(receivedInput.size(), equalTo(triggerCompleteAtNthEvent));

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });

        assertThat(source.getCurrentSequenceNumber(), equalTo(triggerCompleteAtNthEvent + ""));
        // In non-shard end cases, upon successful completion, the publisher would re-subscribe to service.
        // Let's wait for sometime to allow the publisher to re-subscribe
        onS2SCallLatch.await(5000, TimeUnit.MILLISECONDS);
        verify(kinesisClient, times(2)).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
    }

    @Test
    public void testIfShardEndEventAndOnCompleteAreDeliveredInOrderWithBackpressureAdheringServicePublisher()
            throws Exception {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());

        Consumer<Integer> servicePublisherAction = contSeqNum -> captor.getValue()
                .onNext(SubscribeToShardEvent.builder()
                        .millisBehindLatest(100L)
                        .continuationSequenceNumber(contSeqNum + "")
                        .records(records)
                        .childShards(Collections.emptyList())
                        .build());

        List<ChildShard> childShards = new ArrayList<>();
        List<String> parentShards = new ArrayList<>();
        parentShards.add(SHARD_ID);
        ChildShard leftChild = ChildShard.builder()
                .shardId("Shard-002")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"))
                .build();
        ChildShard rightChild = ChildShard.builder()
                .shardId("Shard-003")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("50", "99"))
                .build();
        childShards.add(leftChild);
        childShards.add(rightChild);
        Consumer<Integer> servicePublisherShardEndAction = contSeqNum -> captor.getValue()
                .onNext(SubscribeToShardEvent.builder()
                        .millisBehindLatest(100L)
                        .continuationSequenceNumber(null)
                        .records(records)
                        .childShards(childShards)
                        .build());

        CountDownLatch servicePublisherTaskCompletionLatch = new CountDownLatch(2);
        CountDownLatch onCompleteLatch = new CountDownLatch(1);
        int totalServicePublisherEvents = 1000;
        int initialDemand = 9;
        int triggerCompleteAtNthEvent = 200;
        BackpressureAdheringServicePublisher servicePublisher = new BackpressureAdheringServicePublisher(
                servicePublisherAction,
                totalServicePublisherEvents,
                servicePublisherTaskCompletionLatch,
                initialDemand);

        servicePublisher.setShardEndAndCompleteTrigger(
                triggerCompleteAtNthEvent, () -> flowCaptor.getValue().complete(), servicePublisherShardEndAction);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        final boolean[] isOnCompleteTriggered = {false};

        Subscriber<RecordsRetrieved> shardConsumerSubscriber = new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    private Subscription subscription;
                    private int lastSeenSeqNum = 0;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                        servicePublisher.request(1);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        subscription.request(1);
                        servicePublisher.request(1);
                        if (receivedInput.size() == triggerCompleteAtNthEvent) {
                            servicePublisherTaskCompletionLatch.countDown();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        fail("Caught throwable in subscriber");
                    }

                    @Override
                    public void onComplete() {
                        isOnCompleteTriggered[0] = true;
                        onCompleteLatch.countDown();
                    }
                },
                source);

        ExecutorService executorService = getTestExecutor();
        Scheduler testScheduler = getScheduler(getInitiallyBlockingExecutor(getSpiedExecutor(executorService)));
        int bufferSize = 8;

        Flowable.fromPublisher(source)
                .subscribeOn(testScheduler)
                .observeOn(testScheduler, true, bufferSize)
                .subscribe(shardConsumerSubscriber);

        verify(kinesisClient, times(1)).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());

        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        executorService.submit(servicePublisher);
        servicePublisherTaskCompletionLatch.await(5000, TimeUnit.MILLISECONDS);

        assertThat(receivedInput.size(), equalTo(triggerCompleteAtNthEvent));

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });

        assertNull(source.getCurrentSequenceNumber());
        // With shard end event, onComplete must be propagated to the subscriber.
        onCompleteLatch.await(5000, TimeUnit.MILLISECONDS);
        assertTrue("OnComplete should be triggered", isOnCompleteTriggered[0]);
    }

    @Test
    public void testIfStreamOfEventsAndOnErrorAreDeliveredInOrderWithBackpressureAdheringServicePublisher()
            throws Exception {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());

        Consumer<Integer> servicePublisherAction = contSeqNum -> captor.getValue()
                .onNext(SubscribeToShardEvent.builder()
                        .millisBehindLatest(100L)
                        .continuationSequenceNumber(contSeqNum + "")
                        .records(records)
                        .childShards(Collections.emptyList())
                        .build());

        CountDownLatch servicePublisherTaskCompletionLatch = new CountDownLatch(2);
        CountDownLatch onErrorReceiveLatch = new CountDownLatch(1);
        int totalServicePublisherEvents = 1000;
        int initialDemand = 9;
        int triggerErrorAtNthEvent = 241;
        BackpressureAdheringServicePublisher servicePublisher = new BackpressureAdheringServicePublisher(
                servicePublisherAction,
                totalServicePublisherEvents,
                servicePublisherTaskCompletionLatch,
                initialDemand);
        servicePublisher.setErrorTrigger(
                triggerErrorAtNthEvent,
                () -> flowCaptor.getValue().exceptionOccurred(new RuntimeException("Service Exception")));

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        final boolean[] isOnErrorThrown = {false};

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        Subscriber<RecordsRetrieved> shardConsumerSubscriber = new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    private Subscription subscription;
                    private int lastSeenSeqNum = 0;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                        servicePublisher.request(1);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        assertEquals(
                                "" + ++lastSeenSeqNum,
                                ((FanOutRecordsPublisher.FanoutRecordsRetrieved) input).continuationSequenceNumber());
                        subscription.request(1);
                        servicePublisher.request(1);
                        if (receivedInput.size() == triggerErrorAtNthEvent) {
                            servicePublisherTaskCompletionLatch.countDown();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        isOnErrorThrown[0] = true;
                        onErrorReceiveLatch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        fail("OnComplete called when not expected");
                    }
                },
                source);

        ExecutorService executorService = getTestExecutor();
        Scheduler testScheduler = getScheduler(getInitiallyBlockingExecutor(getSpiedExecutor(executorService)));
        int bufferSize = 8;

        Flowable.fromPublisher(source)
                .subscribeOn(testScheduler)
                .observeOn(testScheduler, true, bufferSize)
                .subscribe(shardConsumerSubscriber);

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        executorService.submit(servicePublisher);
        servicePublisherTaskCompletionLatch.await(5000, TimeUnit.MILLISECONDS);

        assertThat(receivedInput.size(), equalTo(triggerErrorAtNthEvent));

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });

        assertThat(source.getCurrentSequenceNumber(), equalTo(triggerErrorAtNthEvent + ""));
        onErrorReceiveLatch.await(5000, TimeUnit.MILLISECONDS);
        assertTrue("OnError should have been thrown", isOnErrorThrown[0]);
    }

    @Test
    public void
            testIfStreamOfEventsAreDeliveredInOrderWithBackpressureAdheringServicePublisherHavingInitialBurstWithinLimit()
                    throws Exception {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());

        Consumer<Integer> servicePublisherAction = contSeqNum -> captor.getValue()
                .onNext(SubscribeToShardEvent.builder()
                        .millisBehindLatest(100L)
                        .continuationSequenceNumber(contSeqNum + "")
                        .records(records)
                        .childShards(Collections.emptyList())
                        .build());

        CountDownLatch servicePublisherTaskCompletionLatch = new CountDownLatch(2);
        int totalServicePublisherEvents = 1000;
        int initialDemand = 9;
        BackpressureAdheringServicePublisher servicePublisher = new BackpressureAdheringServicePublisher(
                servicePublisherAction,
                totalServicePublisherEvents,
                servicePublisherTaskCompletionLatch,
                initialDemand);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        Subscriber<RecordsRetrieved> shardConsumerSubscriber = new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    private Subscription subscription;
                    private int lastSeenSeqNum = 0;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                        servicePublisher.request(1);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        assertEquals(
                                "" + ++lastSeenSeqNum,
                                ((FanOutRecordsPublisher.FanoutRecordsRetrieved) input).continuationSequenceNumber());
                        subscription.request(1);
                        servicePublisher.request(1);
                        if (receivedInput.size() == totalServicePublisherEvents) {
                            servicePublisherTaskCompletionLatch.countDown();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        fail("Caught throwable in subscriber");
                    }

                    @Override
                    public void onComplete() {
                        fail("OnComplete called when not expected");
                    }
                },
                source);

        ExecutorService executorService = getTestExecutor();
        Scheduler testScheduler = getScheduler(getInitiallyBlockingExecutor(getSpiedExecutor(executorService)));
        int bufferSize = 8;

        Flowable.fromPublisher(source)
                .subscribeOn(testScheduler)
                .observeOn(testScheduler, true, bufferSize)
                .subscribe(shardConsumerSubscriber);

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        executorService.submit(servicePublisher);
        servicePublisherTaskCompletionLatch.await(5000, TimeUnit.MILLISECONDS);

        assertThat(receivedInput.size(), equalTo(totalServicePublisherEvents));

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });

        assertThat(source.getCurrentSequenceNumber(), equalTo(totalServicePublisherEvents + ""));
    }

    @Test
    public void
            testIfStreamOfEventsAreDeliveredInOrderWithBackpressureAdheringServicePublisherHavingInitialBurstOverLimit()
                    throws Exception {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());

        Consumer<Integer> servicePublisherAction = contSeqNum -> captor.getValue()
                .onNext(SubscribeToShardEvent.builder()
                        .millisBehindLatest(100L)
                        .continuationSequenceNumber(contSeqNum + "")
                        .records(records)
                        .childShards(Collections.emptyList())
                        .build());

        CountDownLatch servicePublisherTaskCompletionLatch = new CountDownLatch(1);
        int totalServicePublisherEvents = 1000;
        int initialDemand = 11;
        BackpressureAdheringServicePublisher servicePublisher = new BackpressureAdheringServicePublisher(
                servicePublisherAction,
                totalServicePublisherEvents,
                servicePublisherTaskCompletionLatch,
                initialDemand);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();
        AtomicBoolean onErrorSet = new AtomicBoolean(false);

        Subscriber<RecordsRetrieved> shardConsumerSubscriber = new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    private Subscription subscription;
                    private int lastSeenSeqNum = 0;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                        servicePublisher.request(1);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        assertEquals(
                                "" + ++lastSeenSeqNum,
                                ((FanOutRecordsPublisher.FanoutRecordsRetrieved) input).continuationSequenceNumber());
                        subscription.request(1);
                        servicePublisher.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        onErrorSet.set(true);
                        servicePublisherTaskCompletionLatch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        fail("OnComplete called when not expected");
                    }
                },
                source);

        ExecutorService executorService = getTestExecutor();
        Scheduler testScheduler = getScheduler(getInitiallyBlockingExecutor(getSpiedExecutor(executorService)));
        int bufferSize = 8;

        Flowable.fromPublisher(source)
                .subscribeOn(testScheduler)
                .observeOn(testScheduler, true, bufferSize)
                .subscribe(shardConsumerSubscriber);

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        executorService.submit(servicePublisher);
        servicePublisherTaskCompletionLatch.await(5000, TimeUnit.MILLISECONDS);

        assertTrue("onError should have triggered", onErrorSet.get());

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });
    }

    private Scheduler getScheduler(ExecutorService executorService) {
        return Schedulers.from(executorService);
    }

    private ExecutorService getTestExecutor() {
        return Executors.newFixedThreadPool(
                8,
                new ThreadFactoryBuilder()
                        .setNameFormat("test-fanout-record-publisher-%04d")
                        .setDaemon(true)
                        .build());
    }

    private ExecutorService getSpiedExecutor(ExecutorService executorService) {
        return spy(executorService);
    }

    private ExecutorService getBlockingExecutor(ExecutorService executorService) {
        doAnswer(invocation -> directlyExecuteRunnable(invocation))
                .when(executorService)
                .execute(any());
        return executorService;
    }

    private ExecutorService getInitiallyBlockingExecutor(ExecutorService executorService) {
        doAnswer(invocation -> directlyExecuteRunnable(invocation))
                .doAnswer(invocation -> directlyExecuteRunnable(invocation))
                .doCallRealMethod()
                .when(executorService)
                .execute(any());
        return executorService;
    }

    private ExecutorService getOverwhelmedBlockingExecutor(ExecutorService executorService) {
        doAnswer(invocation -> directlyExecuteRunnable(invocation))
                .doAnswer(invocation -> directlyExecuteRunnable(invocation))
                .doAnswer(invocation -> directlyExecuteRunnable(invocation))
                .doThrow(new RejectedExecutionException())
                .doAnswer(invocation -> directlyExecuteRunnable(invocation))
                .when(executorService)
                .execute(any());
        return executorService;
    }

    private Object directlyExecuteRunnable(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        Runnable runnable = (Runnable) args[0];
        runnable.run();
        return null;
    }

    @Test
    public void largeRequestTest() throws Exception {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        source.subscribe(new ShardConsumerNotifyingSubscriber(
                new Subscriber<RecordsRetrieved>() {
                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(3);
                    }

                    @Override
                    public void onNext(RecordsRetrieved input) {
                        receivedInput.add(input.processRecordsInput());
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Caught throwable in subscriber", t);
                        fail("Caught throwable in subscriber");
                    }

                    @Override
                    public void onComplete() {
                        fail("OnComplete called when not expected");
                    }
                },
                source));

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        batchEvent = SubscribeToShardEvent.builder()
                .millisBehindLatest(100L)
                .records(records)
                .continuationSequenceNumber(CONTINUATION_SEQUENCE_NUMBER)
                .childShards(Collections.emptyList())
                .build();

        captor.getValue().onNext(batchEvent);
        captor.getValue().onNext(batchEvent);
        captor.getValue().onNext(batchEvent);

        verify(subscription, times(4)).request(1);
        assertThat(receivedInput.size(), equalTo(3));

        receivedInput.stream().map(ProcessRecordsInput::records).forEach(clientRecordsList -> {
            assertThat(clientRecordsList.size(), equalTo(matchers.size()));
            for (int i = 0; i < clientRecordsList.size(); ++i) {
                assertThat(clientRecordsList.get(i), matchers.get(i));
            }
        });
    }

    @Test
    public void testResourceNotFoundForShard() {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);
        ArgumentCaptor<RecordsRetrieved> inputCaptor = ArgumentCaptor.forClass(RecordsRetrieved.class);

        source.subscribe(subscriber);

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        FanOutRecordsPublisher.RecordFlow recordFlow = flowCaptor.getValue();
        recordFlow.exceptionOccurred(
                new RuntimeException(ResourceNotFoundException.builder().build()));

        verify(subscriber).onSubscribe(any());
        verify(subscriber, never()).onError(any());
        verify(subscriber).onNext(inputCaptor.capture());
        verify(subscriber).onComplete();

        ProcessRecordsInput input = inputCaptor.getValue().processRecordsInput();
        assertThat(input.isAtShardEnd(), equalTo(true));
        assertThat(input.records().isEmpty(), equalTo(true));
    }

    @Test
    public void testReadTimeoutExceptionForShard() {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        source.subscribe(subscriber);

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        FanOutRecordsPublisher.RecordFlow recordFlow = flowCaptor.getValue();
        recordFlow.exceptionOccurred(new RuntimeException(ReadTimeoutException.INSTANCE));

        verify(subscriber).onSubscribe(any());
        verify(subscriber).onError(any(RetryableRetrievalException.class));
        verify(subscriber, never()).onNext(any());
        verify(subscriber, never()).onComplete();
    }

    @Test
    public void testContinuesAfterSequence() {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(
                new ExtendedSequenceNumber("0"),
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        NonFailingSubscriber nonFailingSubscriber = new NonFailingSubscriber();

        source.subscribe(new ShardConsumerNotifyingSubscriber(nonFailingSubscriber, source));

        SubscribeToShardRequest expected = SubscribeToShardRequest.builder()
                .consumerARN(CONSUMER_ARN)
                .shardId(SHARD_ID)
                .startingPosition(StartingPosition.builder()
                        .sequenceNumber("0")
                        .type(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .build())
                .build();

        verify(kinesisClient)
                .subscribeToShard(argThat(new SubscribeToShardRequestMatcher(expected)), flowCaptor.capture());

        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> matchers =
                records.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        batchEvent = SubscribeToShardEvent.builder()
                .millisBehindLatest(100L)
                .records(records)
                .continuationSequenceNumber("3")
                .childShards(Collections.emptyList())
                .build();

        captor.getValue().onNext(batchEvent);
        captor.getValue().onComplete();
        flowCaptor.getValue().complete();

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> nextSubscribeCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> nextFlowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        SubscribeToShardRequest nextExpected = SubscribeToShardRequest.builder()
                .consumerARN(CONSUMER_ARN)
                .shardId(SHARD_ID)
                .startingPosition(StartingPosition.builder()
                        .sequenceNumber("3")
                        .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                        .build())
                .build();

        verify(kinesisClient)
                .subscribeToShard(argThat(new SubscribeToShardRequestMatcher(nextExpected)), nextFlowCaptor.capture());
        reset(publisher);
        doNothing().when(publisher).subscribe(nextSubscribeCaptor.capture());

        nextFlowCaptor.getValue().onEventStream(publisher);
        nextSubscribeCaptor.getValue().onSubscribe(subscription);

        List<Record> nextRecords = Stream.of(4, 5, 6).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> nextMatchers =
                nextRecords.stream().map(KinesisClientRecordMatcher::new).collect(Collectors.toList());

        batchEvent = SubscribeToShardEvent.builder()
                .millisBehindLatest(100L)
                .records(nextRecords)
                .continuationSequenceNumber("6")
                .childShards(Collections.emptyList())
                .build();
        nextSubscribeCaptor.getValue().onNext(batchEvent);

        verify(subscription, times(4)).request(1);

        assertThat(nonFailingSubscriber.received.size(), equalTo(2));

        verifyRecords(nonFailingSubscriber.received.get(0).records(), matchers);
        verifyRecords(nonFailingSubscriber.received.get(1).records(), nextMatchers);
    }

    @Test
    public void testIfBufferingRecordsWithinCapacityPublishesOneEvent() {
        FanOutRecordsPublisher fanOutRecordsPublisher =
                new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);
        RecordsRetrieved recordsRetrieved = ProcessRecordsInput.builder()::build;
        FanOutRecordsPublisher.RecordFlow recordFlow =
                new FanOutRecordsPublisher.RecordFlow(fanOutRecordsPublisher, Instant.now(), "shard-001-001");
        final int[] totalRecordsRetrieved = {0};
        fanOutRecordsPublisher.subscribe(new Subscriber<RecordsRetrieved>() {
            @Override
            public void onSubscribe(Subscription subscription) {}

            @Override
            public void onNext(RecordsRetrieved recordsRetrieved) {
                totalRecordsRetrieved[0]++;
            }

            @Override
            public void onError(Throwable throwable) {}

            @Override
            public void onComplete() {}
        });
        IntStream.rangeClosed(1, 10)
                .forEach(i ->
                        fanOutRecordsPublisher.bufferCurrentEventAndScheduleIfRequired(recordsRetrieved, recordFlow));
        assertEquals(1, totalRecordsRetrieved[0]);
    }

    @Test
    public void testIfBufferingRecordsOverCapacityPublishesOneEventAndThrows() {
        FanOutRecordsPublisher fanOutRecordsPublisher =
                new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);
        RecordsRetrieved recordsRetrieved = ProcessRecordsInput.builder()::build;
        FanOutRecordsPublisher.RecordFlow recordFlow =
                new FanOutRecordsPublisher.RecordFlow(fanOutRecordsPublisher, Instant.now(), "shard-001");
        final int[] totalRecordsRetrieved = {0};
        fanOutRecordsPublisher.subscribe(new Subscriber<RecordsRetrieved>() {
            @Override
            public void onSubscribe(Subscription subscription) {}

            @Override
            public void onNext(RecordsRetrieved recordsRetrieved) {
                totalRecordsRetrieved[0]++;
            }

            @Override
            public void onError(Throwable throwable) {}

            @Override
            public void onComplete() {}
        });
        try {
            IntStream.rangeClosed(1, 12)
                    .forEach(i -> fanOutRecordsPublisher.bufferCurrentEventAndScheduleIfRequired(
                            recordsRetrieved, recordFlow));
            fail("Should throw Queue full exception");
        } catch (IllegalStateException e) {
            assertEquals("Queue full", e.getMessage());
        }
        assertEquals(1, totalRecordsRetrieved[0]);
    }

    @Test
    public void testIfPublisherAlwaysPublishesWhenQueueIsEmpty() {
        FanOutRecordsPublisher fanOutRecordsPublisher =
                new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);
        FanOutRecordsPublisher.RecordFlow recordFlow =
                new FanOutRecordsPublisher.RecordFlow(fanOutRecordsPublisher, Instant.now(), "shard-001");
        final int[] totalRecordsRetrieved = {0};
        fanOutRecordsPublisher.subscribe(new Subscriber<RecordsRetrieved>() {
            @Override
            public void onSubscribe(Subscription subscription) {}

            @Override
            public void onNext(RecordsRetrieved recordsRetrieved) {
                totalRecordsRetrieved[0]++;
                // This makes sure the queue is immediately made empty, so that the next event enqueued will
                // be the only element in the queue.
                fanOutRecordsPublisher.evictAckedEventAndScheduleNextEvent(
                        () -> recordsRetrieved.batchUniqueIdentifier());
            }

            @Override
            public void onError(Throwable throwable) {}

            @Override
            public void onComplete() {}
        });
        IntStream.rangeClosed(1, 137)
                .forEach(i -> fanOutRecordsPublisher.bufferCurrentEventAndScheduleIfRequired(
                        new FanOutRecordsPublisher.FanoutRecordsRetrieved(
                                ProcessRecordsInput.builder().build(), i + "", recordFlow.getSubscribeToShardId()),
                        recordFlow));
        assertEquals(137, totalRecordsRetrieved[0]);
    }

    @Test
    public void testIfPublisherIgnoresStaleEventsAndContinuesWithNextFlow() {
        FanOutRecordsPublisher fanOutRecordsPublisher =
                new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);
        FanOutRecordsPublisher.RecordFlow recordFlow =
                new FanOutRecordsPublisher.RecordFlow(fanOutRecordsPublisher, Instant.now(), "shard-001");
        final int[] totalRecordsRetrieved = {0};
        fanOutRecordsPublisher.subscribe(new Subscriber<RecordsRetrieved>() {
            @Override
            public void onSubscribe(Subscription subscription) {}

            @Override
            public void onNext(RecordsRetrieved recordsRetrieved) {
                totalRecordsRetrieved[0]++;
                // This makes sure the queue is immediately made empty, so that the next event enqueued will
                // be the only element in the queue.
                fanOutRecordsPublisher.evictAckedEventAndScheduleNextEvent(
                        () -> recordsRetrieved.batchUniqueIdentifier());
                // Send stale event periodically
                if (totalRecordsRetrieved[0] % 10 == 0) {
                    fanOutRecordsPublisher.evictAckedEventAndScheduleNextEvent(
                            () -> new BatchUniqueIdentifier("some_uuid_str", "some_old_flow"));
                }
            }

            @Override
            public void onError(Throwable throwable) {}

            @Override
            public void onComplete() {}
        });
        IntStream.rangeClosed(1, 100)
                .forEach(i -> fanOutRecordsPublisher.bufferCurrentEventAndScheduleIfRequired(
                        new FanOutRecordsPublisher.FanoutRecordsRetrieved(
                                ProcessRecordsInput.builder().build(), i + "", recordFlow.getSubscribeToShardId()),
                        recordFlow));
        assertEquals(100, totalRecordsRetrieved[0]);
    }

    @Test
    public void testIfPublisherIgnoresStaleEventsAndContinuesWithNextFlowWhenDeliveryQueueIsNotEmpty()
            throws InterruptedException {
        FanOutRecordsPublisher fanOutRecordsPublisher =
                new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);
        FanOutRecordsPublisher.RecordFlow recordFlow =
                new FanOutRecordsPublisher.RecordFlow(fanOutRecordsPublisher, Instant.now(), "shard-001");
        final int[] totalRecordsRetrieved = {0};
        BlockingQueue<BatchUniqueIdentifier> ackQueue = new LinkedBlockingQueue<>();
        fanOutRecordsPublisher.subscribe(new Subscriber<RecordsRetrieved>() {
            @Override
            public void onSubscribe(Subscription subscription) {}

            @Override
            public void onNext(RecordsRetrieved recordsRetrieved) {
                totalRecordsRetrieved[0]++;
                // Enqueue the ack for bursty delivery
                ackQueue.add(recordsRetrieved.batchUniqueIdentifier());
                // Send stale event periodically
            }

            @Override
            public void onError(Throwable throwable) {}

            @Override
            public void onComplete() {}
        });
        IntStream.rangeClosed(1, 10)
                .forEach(i -> fanOutRecordsPublisher.bufferCurrentEventAndScheduleIfRequired(
                        new FanOutRecordsPublisher.FanoutRecordsRetrieved(
                                ProcessRecordsInput.builder().build(), i + "", recordFlow.getSubscribeToShardId()),
                        recordFlow));
        BatchUniqueIdentifier batchUniqueIdentifierQueued;
        int count = 0;
        // Now that we allowed upto 10 elements queued up, send a pair of good and stale ack to verify records
        // delivered as expected.
        while (count++ < 10 && (batchUniqueIdentifierQueued = ackQueue.take()) != null) {
            final BatchUniqueIdentifier batchUniqueIdentifierFinal = batchUniqueIdentifierQueued;
            fanOutRecordsPublisher.evictAckedEventAndScheduleNextEvent(() -> batchUniqueIdentifierFinal);
            fanOutRecordsPublisher.evictAckedEventAndScheduleNextEvent(
                    () -> new BatchUniqueIdentifier("some_uuid_str", "some_old_flow"));
        }
        assertEquals(10, totalRecordsRetrieved[0]);
    }

    @Test(expected = IllegalStateException.class)
    public void testIfPublisherThrowsWhenMismatchAckforActiveFlowSeen() throws InterruptedException {
        FanOutRecordsPublisher fanOutRecordsPublisher =
                new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);
        FanOutRecordsPublisher.RecordFlow recordFlow =
                new FanOutRecordsPublisher.RecordFlow(fanOutRecordsPublisher, Instant.now(), "Shard-001-1");
        final int[] totalRecordsRetrieved = {0};
        BlockingQueue<BatchUniqueIdentifier> ackQueue = new LinkedBlockingQueue<>();
        fanOutRecordsPublisher.subscribe(new Subscriber<RecordsRetrieved>() {
            @Override
            public void onSubscribe(Subscription subscription) {}

            @Override
            public void onNext(RecordsRetrieved recordsRetrieved) {
                totalRecordsRetrieved[0]++;
                // Enqueue the ack for bursty delivery
                ackQueue.add(recordsRetrieved.batchUniqueIdentifier());
                // Send stale event periodically
            }

            @Override
            public void onError(Throwable throwable) {}

            @Override
            public void onComplete() {}
        });
        IntStream.rangeClosed(1, 10)
                .forEach(i -> fanOutRecordsPublisher.bufferCurrentEventAndScheduleIfRequired(
                        new FanOutRecordsPublisher.FanoutRecordsRetrieved(
                                ProcessRecordsInput.builder().build(), i + "", recordFlow.getSubscribeToShardId()),
                        recordFlow));
        BatchUniqueIdentifier batchUniqueIdentifierQueued;
        int count = 0;
        // Now that we allowed upto 10 elements queued up, send a pair of good and stale ack to verify records
        // delivered as expected.
        while (count++ < 2 && (batchUniqueIdentifierQueued = ackQueue.poll(1000, TimeUnit.MILLISECONDS)) != null) {
            final BatchUniqueIdentifier batchUniqueIdentifierFinal = batchUniqueIdentifierQueued;
            fanOutRecordsPublisher.evictAckedEventAndScheduleNextEvent(
                    () -> new BatchUniqueIdentifier("some_uuid_str", batchUniqueIdentifierFinal.getFlowIdentifier()));
        }
    }

    @Test
    public void acquireTimeoutTriggersLogMethodForActiveFlow() {
        AtomicBoolean acquireTimeoutLogged = new AtomicBoolean(false);
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN) {
            @Override
            protected void logAcquireTimeoutMessage(Throwable t) {
                super.logAcquireTimeoutMessage(t);
                acquireTimeoutLogged.set(true);
            }
        };

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor =
                ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordFlow.class);

        source.start(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));
        RecordingSubscriber subscriber = new RecordingSubscriber();
        source.subscribe(subscriber);

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());

        Throwable exception = new CompletionException(
                "software.amazon.awssdk.core.exception.SdkClientException",
                SdkClientException.create(
                        null,
                        new Throwable(
                                "Acquire operation took longer than the configured maximum time. This indicates that a "
                                        + "request cannot get a connection from the pool within the specified maximum time. "
                                        + "This can be due to high request rate.\n"
                                        + "Consider taking any of the following actions to mitigate the issue: increase max "
                                        + "connections, increase acquire timeout, or slowing the request rate.\n"
                                        + "Increasing the max connections can increase client throughput (unless the network "
                                        + "interface is already fully utilized), but can eventually start to hit operation "
                                        + "system limitations on the number of file descriptors used by the process. "
                                        + "If you already are fully utilizing your network interface or cannot further "
                                        + "increase your connection count, increasing the acquire timeout gives extra time "
                                        + "for requests to acquire a connection before timing out. "
                                        + "If the connections doesn't free up, the subsequent requests will still timeout.\n"
                                        + "If the above mechanisms are not able to fix the issue, try smoothing out your "
                                        + "requests so that large traffic bursts cannot overload the client, being more "
                                        + "efficient with the number of times you need to call AWS, or by increasing the "
                                        + "number of hosts sending requests.")));

        flowCaptor.getValue().exceptionOccurred(exception);

        Optional<OnErrorEvent> onErrorEvent = subscriber.events.stream()
                .filter(e -> e instanceof OnErrorEvent)
                .map(e -> (OnErrorEvent) e)
                .findFirst();

        assertThat(onErrorEvent, equalTo(Optional.of(new OnErrorEvent(exception))));
        assertThat(acquireTimeoutLogged.get(), equalTo(true));
    }

    private void verifyRecords(List<KinesisClientRecord> clientRecordsList, List<KinesisClientRecordMatcher> matchers) {
        assertThat(clientRecordsList.size(), equalTo(matchers.size()));
        for (int i = 0; i < clientRecordsList.size(); ++i) {
            assertThat(clientRecordsList.get(i), matchers.get(i));
        }
    }

    private interface SubscriberEvent {}

    @Data
    private static class SubscribeEvent implements SubscriberEvent {
        final Subscription subscription;
    }

    @Data
    private static class OnNextEvent implements SubscriberEvent {
        final RecordsRetrieved recordsRetrieved;
    }

    @Data
    private static class OnErrorEvent implements SubscriberEvent {
        final Throwable throwable;
    }

    @Data
    private static class OnCompleteEvent implements SubscriberEvent {}

    @Data
    private static class RequestEvent implements SubscriberEvent {
        final long requested;
    }

    private static class RecordingSubscriber implements Subscriber<RecordsRetrieved> {

        final List<SubscriberEvent> events = new LinkedList<>();

        Subscription subscription;

        @Override
        public void onSubscribe(Subscription s) {
            events.add(new SubscribeEvent(s));
            subscription = s;
            subscription.request(1);
            events.add(new RequestEvent(1));
        }

        @Override
        public void onNext(RecordsRetrieved recordsRetrieved) {
            events.add(new OnNextEvent(recordsRetrieved));
            subscription.request(1);
            events.add(new RequestEvent(1));
        }

        @Override
        public void onError(Throwable t) {
            events.add(new OnErrorEvent(t));
        }

        @Override
        public void onComplete() {
            events.add(new OnCompleteEvent());
        }
    }

    private static class NonFailingSubscriber implements Subscriber<RecordsRetrieved> {
        final List<ProcessRecordsInput> received = new ArrayList<>();
        Subscription subscription;

        @Override
        public void onSubscribe(Subscription s) {
            subscription = s;
            subscription.request(1);
        }

        @Override
        public void onNext(RecordsRetrieved input) {
            received.add(input.processRecordsInput());
            subscription.request(1);
        }

        @Override
        public void onError(Throwable t) {
            log.error("Caught throwable in subscriber", t);
            fail("Caught throwable in subscriber");
        }

        @Override
        public void onComplete() {
            fail("OnComplete called when not expected");
        }
    }

    @RequiredArgsConstructor
    private static class BackpressureAdheringServicePublisher implements Runnable {

        private final Consumer<Integer> action;
        private final Integer numOfTimes;
        private final CountDownLatch taskCompletionLatch;
        private final Semaphore demandNotifier;
        private Integer sendCompletionAt;
        private Runnable completeAction;
        private Integer sendErrorAt;
        private Runnable errorAction;
        private Consumer<Integer> shardEndAction;

        BackpressureAdheringServicePublisher(
                Consumer<Integer> action,
                Integer numOfTimes,
                CountDownLatch taskCompletionLatch,
                Integer initialDemand) {
            this(action, numOfTimes, taskCompletionLatch, new Semaphore(initialDemand));
            sendCompletionAt = Integer.MAX_VALUE;
            sendErrorAt = Integer.MAX_VALUE;
        }

        public void request(int n) {
            demandNotifier.release(n);
        }

        public void run() {
            for (int i = 1; i <= numOfTimes; ) {
                demandNotifier.acquireUninterruptibly();
                if (i == sendCompletionAt) {
                    if (shardEndAction != null) {
                        shardEndAction.accept(i++);
                    } else {
                        action.accept(i++);
                    }
                    completeAction.run();
                    break;
                }
                if (i == sendErrorAt) {
                    action.accept(i++);
                    errorAction.run();
                    break;
                }
                action.accept(i++);
            }
            taskCompletionLatch.countDown();
        }

        public void setCompleteTrigger(Integer sendCompletionAt, Runnable completeAction) {
            this.sendCompletionAt = sendCompletionAt;
            this.completeAction = completeAction;
        }

        public void setShardEndAndCompleteTrigger(
                Integer sendCompletionAt, Runnable completeAction, Consumer<Integer> shardEndAction) {
            setCompleteTrigger(sendCompletionAt, completeAction);
            this.shardEndAction = shardEndAction;
        }

        public void setErrorTrigger(Integer sendErrorAt, Runnable errorAction) {
            this.sendErrorAt = sendErrorAt;
            this.errorAction = errorAction;
        }
    }

    private Record makeRecord(String sequenceNumber) {
        return makeRecord(Integer.parseInt(sequenceNumber));
    }

    private Record makeRecord(int sequenceNumber) {
        SdkBytes buffer = SdkBytes.fromByteArray(new byte[] {1, 2, 3});
        return Record.builder()
                .data(buffer)
                .approximateArrivalTimestamp(Instant.now())
                .sequenceNumber(Integer.toString(sequenceNumber))
                .partitionKey("A")
                .build();
    }

    private static class KinesisClientRecordMatcher extends TypeSafeDiagnosingMatcher<KinesisClientRecord> {

        private final KinesisClientRecord expected;
        private final Matcher<String> partitionKeyMatcher;
        private final Matcher<String> sequenceNumberMatcher;
        private final Matcher<Instant> approximateArrivalMatcher;
        private final Matcher<ByteBuffer> dataMatcher;

        public KinesisClientRecordMatcher(Record record) {
            expected = KinesisClientRecord.fromRecord(record);
            partitionKeyMatcher = equalTo(expected.partitionKey());
            sequenceNumberMatcher = equalTo(expected.sequenceNumber());
            approximateArrivalMatcher = equalTo(expected.approximateArrivalTimestamp());
            dataMatcher = equalTo(expected.data());
        }

        @Override
        protected boolean matchesSafely(KinesisClientRecord item, Description mismatchDescription) {
            boolean matches =
                    matchAndDescribe(partitionKeyMatcher, item.partitionKey(), "partitionKey", mismatchDescription);
            matches &= matchAndDescribe(
                    sequenceNumberMatcher, item.sequenceNumber(), "sequenceNumber", mismatchDescription);
            matches &= matchAndDescribe(
                    approximateArrivalMatcher,
                    item.approximateArrivalTimestamp(),
                    "approximateArrivalTimestamp",
                    mismatchDescription);
            matches &= matchAndDescribe(dataMatcher, item.data(), "data", mismatchDescription);
            return matches;
        }

        private <T> boolean matchAndDescribe(
                Matcher<T> matcher, T value, String field, Description mismatchDescription) {
            if (!matcher.matches(value)) {
                mismatchDescription.appendText(field).appendText(": ");
                matcher.describeMismatch(value, mismatchDescription);
                return false;
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText("A kinesis client record with: ")
                    .appendText("PartitionKey: ")
                    .appendDescriptionOf(partitionKeyMatcher)
                    .appendText(" SequenceNumber: ")
                    .appendDescriptionOf(sequenceNumberMatcher)
                    .appendText(" Approximate Arrival Time: ")
                    .appendDescriptionOf(approximateArrivalMatcher)
                    .appendText(" Data: ")
                    .appendDescriptionOf(dataMatcher);
        }
    }
}
