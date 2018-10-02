package software.amazon.kinesis.retrieval.fanout;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.netty.handler.timeout.ReadTimeoutException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class FanOutRecordsPublisherTest {

    private static final String SHARD_ID = "Shard-001";
    private static final String CONSUMER_ARN = "arn:consumer";

    @Mock
    private KinesisAsyncClient kinesisClient;
    @Mock
    private SdkPublisher<SubscribeToShardEventStream> publisher;
    @Mock
    private Subscription subscription;
    @Mock
    private Subscriber<ProcessRecordsInput> subscriber;

    private SubscribeToShardEvent batchEvent;

    @Test
    public void simpleTest() throws Exception {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor = ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor = ArgumentCaptor
                .forClass(FanOutRecordsPublisher.RecordFlow.class);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(ExtendedSequenceNumber.LATEST, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        source.subscribe(new Subscriber<ProcessRecordsInput>() {
            Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                subscription.request(1);
            }

            @Override
            public void onNext(ProcessRecordsInput input) {
                receivedInput.add(input);
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
        });

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> matchers = records.stream().map(KinesisClientRecordMatcher::new)
                .collect(Collectors.toList());

        batchEvent = SubscribeToShardEvent.builder().millisBehindLatest(100L).records(records).build();

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
    public void largeRequestTest() throws Exception {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor = ArgumentCaptor.forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor = ArgumentCaptor
                .forClass(FanOutRecordsPublisher.RecordFlow.class);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(ExtendedSequenceNumber.LATEST, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<ProcessRecordsInput> receivedInput = new ArrayList<>();

        source.subscribe(new Subscriber<ProcessRecordsInput>() {
            Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                subscription.request(3);
            }

            @Override
            public void onNext(ProcessRecordsInput input) {
                receivedInput.add(input);
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
        });

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> matchers = records.stream().map(KinesisClientRecordMatcher::new)
                .collect(Collectors.toList());

        batchEvent = SubscribeToShardEvent.builder().millisBehindLatest(100L).records(records).build();

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

        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor = ArgumentCaptor
                .forClass(FanOutRecordsPublisher.RecordFlow.class);
        ArgumentCaptor<ProcessRecordsInput> inputCaptor = ArgumentCaptor.forClass(ProcessRecordsInput.class);

        source.subscribe(subscriber);

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        FanOutRecordsPublisher.RecordFlow recordFlow = flowCaptor.getValue();
        recordFlow.exceptionOccurred(new RuntimeException(ResourceNotFoundException.builder().build()));

        verify(subscriber).onSubscribe(any());
        verify(subscriber, never()).onError(any());
        verify(subscriber).onNext(inputCaptor.capture());
        verify(subscriber).onComplete();

        ProcessRecordsInput input = inputCaptor.getValue();
        assertThat(input.isAtShardEnd(), equalTo(true));
        assertThat(input.records().isEmpty(), equalTo(true));
    }

    @Test
    public void testReadTimeoutExceptionForShard() {
        FanOutRecordsPublisher source = new FanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor = ArgumentCaptor
                .forClass(FanOutRecordsPublisher.RecordFlow.class);

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

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor = ArgumentCaptor
                .forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor = ArgumentCaptor
                .forClass(FanOutRecordsPublisher.RecordFlow.class);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(new ExtendedSequenceNumber("0"),
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        NonFailingSubscriber nonFailingSubscriber = new NonFailingSubscriber();

        source.subscribe(nonFailingSubscriber);

        SubscribeToShardRequest expected = SubscribeToShardRequest.builder().consumerARN(CONSUMER_ARN).shardId(SHARD_ID)
                .startingPosition(StartingPosition.builder().sequenceNumber("0")
                        .type(ShardIteratorType.AT_SEQUENCE_NUMBER).build())
                .build();

        verify(kinesisClient).subscribeToShard(eq(expected), flowCaptor.capture());

        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<Record> records = Stream.of(1, 2, 3).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> matchers = records.stream().map(KinesisClientRecordMatcher::new)
                .collect(Collectors.toList());

        batchEvent = SubscribeToShardEvent.builder().millisBehindLatest(100L).records(records)
                .continuationSequenceNumber("3").build();

        captor.getValue().onNext(batchEvent);
        captor.getValue().onComplete();
        flowCaptor.getValue().complete();

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> nextSubscribeCaptor = ArgumentCaptor
                .forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> nextFlowCaptor = ArgumentCaptor
                .forClass(FanOutRecordsPublisher.RecordFlow.class);


        SubscribeToShardRequest nextExpected = SubscribeToShardRequest.builder().consumerARN(CONSUMER_ARN).shardId(SHARD_ID)
                .startingPosition(StartingPosition.builder().sequenceNumber("3")
                        .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER).build())
                .build();

        verify(kinesisClient).subscribeToShard(eq(nextExpected), nextFlowCaptor.capture());
        reset(publisher);
        doNothing().when(publisher).subscribe(nextSubscribeCaptor.capture());

        nextFlowCaptor.getValue().onEventStream(publisher);
        nextSubscribeCaptor.getValue().onSubscribe(subscription);


        List<Record> nextRecords = Stream.of(4, 5, 6).map(this::makeRecord).collect(Collectors.toList());
        List<KinesisClientRecordMatcher> nextMatchers = nextRecords.stream().map(KinesisClientRecordMatcher::new)
                .collect(Collectors.toList());

        batchEvent = SubscribeToShardEvent.builder().millisBehindLatest(100L).records(nextRecords)
                .continuationSequenceNumber("6").build();
        nextSubscribeCaptor.getValue().onNext(batchEvent);

        verify(subscription, times(4)).request(1);

        assertThat(nonFailingSubscriber.received.size(), equalTo(2));

        verifyRecords(nonFailingSubscriber.received.get(0).records(), matchers);
        verifyRecords(nonFailingSubscriber.received.get(1).records(), nextMatchers);

    }

    private void verifyRecords(List<KinesisClientRecord> clientRecordsList, List<KinesisClientRecordMatcher> matchers) {
        assertThat(clientRecordsList.size(), equalTo(matchers.size()));
        for (int i = 0; i < clientRecordsList.size(); ++i) {
            assertThat(clientRecordsList.get(i), matchers.get(i));
        }
    }

    private static class NonFailingSubscriber implements Subscriber<ProcessRecordsInput> {
        final List<ProcessRecordsInput> received = new ArrayList<>();
        Subscription subscription;

        @Override
        public void onSubscribe(Subscription s) {
            subscription = s;
            subscription.request(1);
        }

        @Override
        public void onNext(ProcessRecordsInput input) {
            received.add(input);
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

    private Record makeRecord(int sequenceNumber) {
        SdkBytes buffer = SdkBytes.fromByteArray(new byte[] { 1, 2, 3 });
        return Record.builder().data(buffer).approximateArrivalTimestamp(Instant.now())
                .sequenceNumber(Integer.toString(sequenceNumber)).partitionKey("A").build();
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
            boolean matches = matchAndDescribe(partitionKeyMatcher, item.partitionKey(), "partitionKey",
                    mismatchDescription);
            matches &= matchAndDescribe(sequenceNumberMatcher, item.sequenceNumber(), "sequenceNumber",
                    mismatchDescription);
            matches &= matchAndDescribe(approximateArrivalMatcher, item.approximateArrivalTimestamp(),
                    "approximateArrivalTimestamp", mismatchDescription);
            matches &= matchAndDescribe(dataMatcher, item.data(), "data", mismatchDescription);
            return matches;
        }

        private <T> boolean matchAndDescribe(Matcher<T> matcher, T value, String field,
                Description mismatchDescription) {
            if (!matcher.matches(value)) {
                mismatchDescription.appendText(field).appendText(": ");
                matcher.describeMismatch(value, mismatchDescription);
                return false;
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("A kinesis client record with: ").appendText("PartitionKey: ")
                    .appendDescriptionOf(partitionKeyMatcher).appendText(" SequenceNumber: ")
                    .appendDescriptionOf(sequenceNumberMatcher).appendText(" Approximate Arrival Time: ")
                    .appendDescriptionOf(approximateArrivalMatcher).appendText(" Data: ")
                    .appendDescriptionOf(dataMatcher);
        }

    }

}