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
package software.amazon.kinesis.lifecycle;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.utils.ProcessRecordsInputMatcher.eqProcessRecordsInput;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.RequestDetails;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RecordsDeliveryAck;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class ShardConsumerSubscriberTest {
    private static final StreamIdentifier TEST_STREAM_IDENTIFIER = StreamIdentifier.singleStreamInstance("streamName");
    private static final InitialPositionInStreamExtended TEST_INITIAL_POSITION_IN_STREAM_EXTENDED =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final StreamConfig TEST_STREAM_CONFIG =
            new StreamConfig(TEST_STREAM_IDENTIFIER, TEST_INITIAL_POSITION_IN_STREAM_EXTENDED);

    private final Object processedNotifier = new Object();

    private static final String TERMINAL_MARKER = "Terminal";

    private static final long DEFAULT_NOTIFIER_TIMEOUT = 5000L;

    private final RequestDetails lastSuccessfulRequestDetails = new RequestDetails();

    @Mock
    private ShardConsumer shardConsumer;
    @Mock
    private RecordsRetrieved recordsRetrieved;

    private ProcessRecordsInput processRecordsInput;
    private TestPublisher recordsPublisher;

    private ExecutorService executorService;
    private int bufferSize = 8;

    private ShardConsumerSubscriber subscriber;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() {
        executorService = Executors.newFixedThreadPool(8, new ThreadFactoryBuilder()
                .setNameFormat("test-" + testName.getMethodName() + "-%04d").setDaemon(true).build());
        recordsPublisher = new TestPublisher();

        final ShardInfo shardInfo = new ShardInfo(
                "shard-001", "", Collections.emptyList(), ExtendedSequenceNumber.TRIM_HORIZON, TEST_STREAM_CONFIG);
        when(shardConsumer.shardInfo()).thenReturn(shardInfo);

        processRecordsInput = ProcessRecordsInput.builder().records(Collections.emptyList())
                .cacheEntryTime(Instant.now()).build();

        subscriber = new ShardConsumerSubscriber(recordsPublisher, executorService, bufferSize, shardConsumer, 0);
        when(recordsRetrieved.processRecordsInput()).thenReturn(processRecordsInput);
    }

    @After
    public void after() {
        executorService.shutdownNow();
    }

    @Test
    public void singleItemTest() throws Exception {
        addItemsToReturn(1);

        setupNotifierAnswer(1);

        startSubscriptionsAndWait();

        verify(shardConsumer).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)), any(Subscription.class));
    }

    @Test
    public void multipleItemTest() throws Exception {
        addItemsToReturn(100);

        setupNotifierAnswer(recordsPublisher.responses.size());

        startSubscriptionsAndWait();

        verify(shardConsumer, times(100)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)), any(Subscription.class));
    }

    @Test
    public void consumerErrorSkipsEntryTest() throws Exception {
        addItemsToReturn(20);

        Throwable testException = new Throwable("ShardConsumerError");

        doAnswer(new Answer() {
            int expectedInvocations = recordsPublisher.responses.size();

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                expectedInvocations--;
                if (expectedInvocations == 10) {
                    throw testException;
                }
                if (expectedInvocations <= 0) {
                    synchronized (processedNotifier) {
                        processedNotifier.notifyAll();
                    }
                }
                return null;
            }
        }).when(shardConsumer).handleInput(any(ProcessRecordsInput.class), any(Subscription.class));

        startSubscriptionsAndWait();

        assertThat(subscriber.getAndResetDispatchFailure(), equalTo(testException));
        assertThat(subscriber.getAndResetDispatchFailure(), nullValue());

        verify(shardConsumer, times(20)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)), any(Subscription.class));
    }

    @Test
    public void onErrorStopsProcessingTest() throws Exception {
        Throwable expected = new Throwable("Wheee");
        addItemsToReturn(10);
        recordsPublisher.add(new ResponseItem(expected));
        addItemsToReturn(10);

        setupNotifierAnswer(10);

        startSubscriptionsAndWait();

        for (int attempts = 0; attempts < 10; attempts++) {
            if (subscriber.retrievalFailure() != null) {
                break;
            }
            Thread.sleep(10);
        }

        verify(shardConsumer, times(10)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)), any(Subscription.class));
        assertThat(subscriber.retrievalFailure(), equalTo(expected));
    }

    @Test
    public void restartAfterErrorTest() throws Exception {
        Throwable expected = new Throwable("whee");
        addItemsToReturn(9);
        RecordsRetrieved edgeRecord = mock(RecordsRetrieved.class);
        when(edgeRecord.processRecordsInput()).thenReturn(processRecordsInput);
        recordsPublisher.add(new ResponseItem(edgeRecord));
        recordsPublisher.add(new ResponseItem(expected));
        addItemsToReturn(10);

        setupNotifierAnswer(10);

        startSubscriptionsAndWait();

        for (int attempts = 0; attempts < 10; attempts++) {
            if (subscriber.retrievalFailure() != null) {
                break;
            }
            Thread.sleep(100);
        }

        setupNotifierAnswer(10);

        synchronized (processedNotifier) {
            assertThat(subscriber.healthCheck(100000), equalTo(expected));
            processedNotifier.wait(DEFAULT_NOTIFIER_TIMEOUT);
        }

        assertThat(recordsPublisher.restartedFrom, equalTo(edgeRecord));
        verify(shardConsumer, times(20)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)), any(Subscription.class));
    }

    @Test
    public void restartAfterRequestTimerExpiresTest() throws Exception {

        executorService = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                .setNameFormat("test-" + testName.getMethodName() + "-%04d").setDaemon(true).build());

        subscriber = new ShardConsumerSubscriber(recordsPublisher, executorService, bufferSize, shardConsumer, 0);
        addUniqueItem(1);
        addTerminalMarker(1);

        CyclicBarrier barrier = new CyclicBarrier(2);

        List<ProcessRecordsInput> received = new ArrayList<>();
        doAnswer(a -> {
            ProcessRecordsInput input = a.getArgumentAt(0, ProcessRecordsInput.class);
            received.add(input);
            if (input.records().stream().anyMatch(r -> StringUtils.startsWith(r.partitionKey(), TERMINAL_MARKER))) {
                synchronized (processedNotifier) {
                    processedNotifier.notifyAll();
                }
            }
            return null;
        }).when(shardConsumer).handleInput(any(ProcessRecordsInput.class), any(Subscription.class));

        startSubscriptionsAndWait();

        synchronized (processedNotifier) {
            executorService.execute(() -> {
                try {
                    //
                    // Notify the test as soon as we have started executing, then wait on the post add
                    // subscriptionBarrier.
                    //
                    synchronized (processedNotifier) {
                        processedNotifier.notifyAll();
                    }
                    barrier.await();
                } catch (Exception e) {
                    log.error("Exception while blocking thread", e);
                }
            });
            //
            // Wait for our blocking thread to control the thread in the executor.
            //
            processedNotifier.wait(DEFAULT_NOTIFIER_TIMEOUT);
        }

        Stream.iterate(2, i -> i + 1).limit(97).forEach(this::addUniqueItem);

        addTerminalMarker(2);

        synchronized (processedNotifier) {
            assertThat(subscriber.healthCheck(1), nullValue());
            barrier.await(500, TimeUnit.MILLISECONDS);

            processedNotifier.wait(DEFAULT_NOTIFIER_TIMEOUT);
        }

        verify(shardConsumer, times(100)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)), any(Subscription.class));

        assertThat(received.size(), equalTo(recordsPublisher.responses.size()));
        Stream.iterate(0, i -> i + 1).limit(received.size()).forEach(i -> assertThat(received.get(i),
                eqProcessRecordsInput(recordsPublisher.responses.get(i).recordsRetrieved.processRecordsInput())));
    }

    @Test
    public void restartAfterRequestTimerExpiresWhenNotGettingRecordsAfterInitialization() throws Exception {
        executorService = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                .setNameFormat("test-" + testName.getMethodName() + "-%04d").setDaemon(true).build());

        // Mock record publisher which doesn't publish any records on first try which simulates any scenario which
        // causes first subscription try to fail.
        recordsPublisher = new RecordPublisherWithInitialFailureSubscription();
        subscriber = new ShardConsumerSubscriber(recordsPublisher, executorService, bufferSize, shardConsumer, 0);
        addUniqueItem(1);

        List<ProcessRecordsInput> received = new ArrayList<>();
        doAnswer(a -> {
            ProcessRecordsInput input = a.getArgumentAt(0, ProcessRecordsInput.class);
            received.add(input);
            if (input.records().stream().anyMatch(r -> StringUtils.startsWith(r.partitionKey(), TERMINAL_MARKER))) {
                synchronized (processedNotifier) {
                    processedNotifier.notifyAll();
                }
            }
            return null;
        }).when(shardConsumer).handleInput(any(ProcessRecordsInput.class), any(Subscription.class));

        // First try to start subscriptions.
        startSubscriptionsAndWait(100);

        // Verifying that there are no interactions with shardConsumer mock indicating no records were sent back and
        // subscription has not started correctly.
        verify(shardConsumer, never()).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)), any(Subscription.class));

        Stream.iterate(2, i -> i + 1).limit(98).forEach(this::addUniqueItem);

        addTerminalMarker(2);

        // Doing the health check to allow the subscription to restart.
        assertThat(subscriber.healthCheck(1), nullValue());

        // Allow time for processing of the records to end in the executor thread which call notifyAll as it gets the
        // terminal record. Keeping the timeout pretty high for avoiding test failures on slow machines.
        synchronized (processedNotifier) {
            processedNotifier.wait(1000);
        }

        // Verify that shardConsumer mock was called 100 times and all 100 input records are processed.
        verify(shardConsumer, times(100)).handleInput(any(ProcessRecordsInput.class), any(Subscription.class));

        // Verify that received records in the subscriber are equal to the ones sent by the record publisher.
        assertThat(received.size(), equalTo(recordsPublisher.responses.size()));
        Stream.iterate(0, i -> i + 1).limit(received.size()).forEach(i -> assertThat(received.get(i),
                eqProcessRecordsInput(recordsPublisher.responses.get(i).recordsRetrieved.processRecordsInput())));
    }

    @Test
    public void restartAfterRequestTimerExpiresWhenInitialTaskExecutionIsRejected() throws Exception {
        executorService = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                .setNameFormat("test-" + testName.getMethodName() + "-%04d").setDaemon(true).build());

        ExecutorService failingService = spy(executorService);

        doAnswer(invocation -> directlyExecuteRunnable(invocation))
                .doThrow(new RejectedExecutionException())
                .doCallRealMethod()
                .when(failingService).execute(any());

        subscriber = new ShardConsumerSubscriber(recordsPublisher, failingService, bufferSize, shardConsumer, 0);
        addUniqueItem(1);

        List<ProcessRecordsInput> received = new ArrayList<>();
        doAnswer(a -> {
            ProcessRecordsInput input = a.getArgumentAt(0, ProcessRecordsInput.class);
            received.add(input);
            if (input.records().stream().anyMatch(r -> StringUtils.startsWith(r.partitionKey(), TERMINAL_MARKER))) {
                synchronized (processedNotifier) {
                    processedNotifier.notifyAll();
                }
            }
            return null;
        }).when(shardConsumer).handleInput(any(ProcessRecordsInput.class), any(Subscription.class));

        // First try to start subscriptions.
        startSubscriptionsAndWait(100);

        // Verifying that there are no interactions with shardConsumer mock indicating no records were sent back and
        // subscription has not started correctly.
        verify(shardConsumer, never()).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)), any(Subscription.class));

        Stream.iterate(2, i -> i + 1).limit(98).forEach(this::addUniqueItem);

        addTerminalMarker(2);

        // Doing the health check to allow the subscription to restart.
        assertThat(subscriber.healthCheck(1), nullValue());

        // Allow time for processing of the records to end in the executor thread which call notifyAll as it gets the
        // terminal record. Keeping the timeout pretty high for avoiding test failures on slow machines.
        synchronized (processedNotifier) {
            processedNotifier.wait(1000);
        }

        // Verify that shardConsumer mock was called 100 times and all 100 input records are processed.
        verify(shardConsumer, times(100)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)), any(Subscription.class));

        // Verify that received records in the subscriber are equal to the ones sent by the record publisher.
        assertThat(received.size(), equalTo(recordsPublisher.responses.size()));
        Stream.iterate(0, i -> i + 1).limit(received.size()).forEach(i -> assertThat(received.get(i),
                eqProcessRecordsInput(recordsPublisher.responses.get(i).recordsRetrieved.processRecordsInput())));
    }

    private Object directlyExecuteRunnable(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        Runnable runnable = (Runnable) args[0];
        runnable.run();
        return null;
    }

    private void addUniqueItem(int id) {
        RecordsRetrieved r = mock(RecordsRetrieved.class, "Record-" + id);
        ProcessRecordsInput input = ProcessRecordsInput.builder().cacheEntryTime(Instant.now())
                .records(Collections.singletonList(KinesisClientRecord.builder().partitionKey("Record-" + id).build()))
                .build();
        when(r.processRecordsInput()).thenReturn(input);
        recordsPublisher.add(new ResponseItem(r));
    }

    private ProcessRecordsInput addTerminalMarker(int id) {
        RecordsRetrieved terminalResponse = mock(RecordsRetrieved.class, TERMINAL_MARKER + "-" + id);
        ProcessRecordsInput terminalInput = ProcessRecordsInput.builder()
                .records(Collections
                        .singletonList(KinesisClientRecord.builder().partitionKey(TERMINAL_MARKER + "-" + id).build()))
                .cacheEntryTime(Instant.now()).build();
        when(terminalResponse.processRecordsInput()).thenReturn(terminalInput);
        recordsPublisher.add(new ResponseItem(terminalResponse));

        return terminalInput;
    }

    private void addItemsToReturn(int count) {
        Stream.iterate(0, i -> i + 1).limit(count)
                .forEach(i -> recordsPublisher.add(new ResponseItem(recordsRetrieved)));
    }

    private void setupNotifierAnswer(int expected) {
        doAnswer(new Answer() {
            int seen = expected;

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                seen--;
                if (seen == 0) {
                    synchronized (processedNotifier) {
                        processedNotifier.notifyAll();
                    }
                }
                return null;
            }
        }).when(shardConsumer).handleInput(any(ProcessRecordsInput.class), any(Subscription.class));
    }

    private void startSubscriptionsAndWait() throws InterruptedException {
        startSubscriptionsAndWait(DEFAULT_NOTIFIER_TIMEOUT);
    }

    private void startSubscriptionsAndWait(long timeout) throws InterruptedException {
        synchronized (processedNotifier) {
            subscriber.startSubscriptions();
            processedNotifier.wait(timeout);
        }
    }

    private class ResponseItem {
        private final RecordsRetrieved recordsRetrieved;
        private final Throwable throwable;
        private int throwCount = 1;

        public ResponseItem(@NonNull RecordsRetrieved recordsRetrieved) {
            this.recordsRetrieved = recordsRetrieved;
            this.throwable = null;
        }

        public ResponseItem(@NonNull Throwable throwable) {
            this.throwable = throwable;
            this.recordsRetrieved = null;
        }
    }

    private class TestPublisher implements RecordsPublisher {

        private final LinkedList<ResponseItem> responses = new LinkedList<>();
        protected volatile long requested = 0;
        private int currentIndex = 0;
        protected Subscriber<? super RecordsRetrieved> subscriber;
        private RecordsRetrieved restartedFrom;

        void add(ResponseItem... toAdd) {
            responses.addAll(Arrays.asList(toAdd));
            send();
        }

        void send() {
            send(0);
        }

        synchronized void send(long toRequest) {
            requested += toRequest;
            while (requested > 0 && currentIndex < responses.size()) {
                ResponseItem item = responses.get(currentIndex);
                currentIndex++;
                if (item.recordsRetrieved != null) {
                    subscriber.onNext(item.recordsRetrieved);
                } else {
                    if (item.throwCount > 0) {
                        item.throwCount--;
                        subscriber.onError(item.throwable);
                    } else {
                        continue;
                    }
                }
                requested--;
            }
        }

        @Override
        public void start(ExtendedSequenceNumber extendedSequenceNumber,
                InitialPositionInStreamExtended initialPositionInStreamExtended) {

        }

        @Override
        public void restartFrom(RecordsRetrieved recordsRetrieved) {
            restartedFrom = recordsRetrieved;
            currentIndex = -1;
            for (int i = 0; i < responses.size(); i++) {
                ResponseItem item = responses.get(i);
                if (recordsRetrieved.equals(item.recordsRetrieved)) {
                    currentIndex = i + 1;
                    break;
                }
            }

        }

        @Override
        public void notify(RecordsDeliveryAck ack) {

        }

        @Override
        public void shutdown() {

        }

        @Override
        public RequestDetails getLastSuccessfulRequestDetails() {
            return lastSuccessfulRequestDetails;
        }

        @Override
        public void subscribe(Subscriber<? super RecordsRetrieved> s) {
            subscriber = s;
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    send(n);
                }

                @Override
                public void cancel() {
                    requested = 0;
                }
            });
        }
    }

    private class RecordPublisherWithInitialFailureSubscription extends TestPublisher {
        private int subscriptionTryCount = 0;

        @Override
        public void subscribe(Subscriber<? super RecordsRetrieved> s) {
            subscriber = s;
            ++subscriptionTryCount;
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    if (subscriptionTryCount != 1) {
                        send(n);
                    }
                }

                @Override
                public void cancel() {
                    requested = 0;
                }
            });
        }
    }

    class TestShardConsumerSubscriber extends ShardConsumerSubscriber {

        private int genericWarningLogged = 0;
        private int readTimeoutWarningLogged = 0;

        TestShardConsumerSubscriber(RecordsPublisher recordsPublisher, ExecutorService executorService, int bufferSize,
                ShardConsumer shardConsumer,
                // Setup test expectations
                int readTimeoutsToIgnoreBeforeWarning) {
            super(recordsPublisher, executorService, bufferSize, shardConsumer, readTimeoutsToIgnoreBeforeWarning);
        }

        @Override
        protected void logOnErrorWarning(Throwable t) {
            genericWarningLogged++;
            super.logOnErrorWarning(t);
        }

        @Override
        protected void logOnErrorReadTimeoutWarning(Throwable t) {
            readTimeoutWarningLogged++;
            super.logOnErrorReadTimeoutWarning(t);
        }
    }

    /**
     * Test to validate the warning message from ShardConsumer is not suppressed with the default configuration of 0
     */
    @Test
    public void noLoggingSuppressionNeededOnHappyPathTest() {
        // All requests are expected to succeed. No logs are expected.

        // Setup test expectations
        int readTimeoutsToIgnore = 0;
        int expectedReadTimeoutLogs = 0;
        int expectedGenericLogs = 0;
        TestShardConsumerSubscriber consumer = new TestShardConsumerSubscriber(mock(RecordsPublisher.class),
                Executors.newFixedThreadPool(1), 8, shardConsumer, readTimeoutsToIgnore);
        consumer.startSubscriptions();
        mimicSuccess(consumer);
        mimicSuccess(consumer);
        mimicSuccess(consumer);
        mimicSuccess(consumer);
        mimicSuccess(consumer);
        assertEquals(expectedGenericLogs, consumer.genericWarningLogged);
        assertEquals(expectedReadTimeoutLogs, consumer.readTimeoutWarningLogged);
    }

    /**
     * Test to validate the warning message from ShardConsumer is not suppressed with the default configuration of 0
     */
    @Test
    public void loggingNotSuppressedAfterTimeoutTest() {
        Exception exceptionToThrow = new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        // The first 2 requests succeed, followed by an exception, a success, and another exception.
        // We are not ignoring any ReadTimeouts, so we expect 1 log on the first failure,
        // and another log on the second failure.

        // Setup test expectations
        int readTimeoutsToIgnore = 0;
        int expectedReadTimeoutLogs = 2;
        int expectedGenericLogs = 0;
        TestShardConsumerSubscriber consumer = new TestShardConsumerSubscriber(mock(RecordsPublisher.class),
                Executors.newFixedThreadPool(1), 8, shardConsumer, readTimeoutsToIgnore);
        consumer.startSubscriptions();
        mimicSuccess(consumer);
        mimicSuccess(consumer);
        mimicException(exceptionToThrow, consumer);
        mimicSuccess(consumer);
        mimicException(exceptionToThrow, consumer);
        assertEquals(expectedGenericLogs, consumer.genericWarningLogged);
        assertEquals(expectedReadTimeoutLogs, consumer.readTimeoutWarningLogged);
    }

    /**
     * Test to validate the warning message from ShardConsumer is successfully supressed if we only have intermittant
     * readTimeouts.
     */
    @Test
    public void loggingSuppressedAfterIntermittentTimeoutTest() {
        Exception exceptionToThrow = new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        // The first 2 requests succeed, followed by an exception, a success, and another exception.
        // We are ignoring a single consecutive ReadTimeout, So we don't expect any logs to be made.

        // Setup test expectations
        int readTimeoutsToIgnore = 1;
        int expectedReadTimeoutLogs = 0;
        int expectedGenericLogs = 0;
        TestShardConsumerSubscriber consumer = new TestShardConsumerSubscriber(mock(RecordsPublisher.class),
                Executors.newFixedThreadPool(1), 8, shardConsumer, readTimeoutsToIgnore);
        consumer.startSubscriptions();
        mimicSuccess(consumer);
        mimicSuccess(consumer);
        mimicException(exceptionToThrow, consumer);
        mimicSuccess(consumer);
        mimicException(exceptionToThrow, consumer);
        assertEquals(expectedGenericLogs, consumer.genericWarningLogged);
        assertEquals(expectedReadTimeoutLogs, consumer.readTimeoutWarningLogged);
    }

    /**
     * Test to validate the warning message from ShardConsumer is successfully logged if multiple sequential timeouts
     * occur.
     */
    @Test
    public void loggingPartiallySuppressedAfterMultipleTimeoutTest() {
        Exception exceptionToThrow = new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        // The first 2 requests are expected to throw an exception, followed by a success, and 2 more exceptions.
        // We are ignoring a single consecutive ReadTimeout, so we expect a single log starting after the second
        // consecutive ReadTimeout (Request 2) and another log after another second consecutive failure (Request 5)

        // Setup test expectations
        int readTimeoutsToIgnore = 1;
        int expectedReadTimeoutLogs = 2;
        int expectedGenericLogs = 0;
        TestShardConsumerSubscriber consumer = new TestShardConsumerSubscriber(mock(RecordsPublisher.class),
                Executors.newFixedThreadPool(1), 8, shardConsumer, readTimeoutsToIgnore);
        consumer.startSubscriptions();
        mimicException(exceptionToThrow, consumer);
        mimicException(exceptionToThrow, consumer);
        mimicSuccess(consumer);
        mimicException(exceptionToThrow, consumer);
        mimicException(exceptionToThrow, consumer);
        assertEquals(expectedGenericLogs, consumer.genericWarningLogged);
        assertEquals(expectedReadTimeoutLogs, consumer.readTimeoutWarningLogged);
    }

    /**
     * Test to validate the warning message from ShardConsumer is successfully logged if sequential timeouts occur.
     */
    @Test
    public void loggingPartiallySuppressedAfterConsecutiveTimeoutTest() {
        Exception exceptionToThrow = new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        // Every request of 5 requests are expected to fail.
        // We are ignoring 2 consecutive ReadTimeout exceptions, so we expect a single log starting after the third
        // consecutive ReadTimeout (Request 3) and another log after each other request since we are still breaching the
        // number of consecutive ReadTimeouts to ignore.

        // Setup test expectations
        int readTimeoutsToIgnore = 2;
        int expectedReadTimeoutLogs = 3;
        int expectedGenericLogs = 0;
        TestShardConsumerSubscriber consumer = new TestShardConsumerSubscriber(mock(RecordsPublisher.class),
                Executors.newFixedThreadPool(1), 8, shardConsumer, readTimeoutsToIgnore);
        consumer.startSubscriptions();
        mimicException(exceptionToThrow, consumer);
        mimicException(exceptionToThrow, consumer);
        mimicException(exceptionToThrow, consumer);
        mimicException(exceptionToThrow, consumer);
        mimicException(exceptionToThrow, consumer);
        assertEquals(expectedGenericLogs, consumer.genericWarningLogged);
        assertEquals(expectedReadTimeoutLogs, consumer.readTimeoutWarningLogged);
    }

    /**
     * Test to validate the non-timeout warning message from ShardConsumer is not suppressed with the default
     * configuration of 0
     */
    @Test
    public void loggingNotSuppressedOnNonReadTimeoutExceptionNotIgnoringReadTimeoutsExceptionTest() {
        // We're not throwing a ReadTimeout, so no suppression is expected.
        // The test expects a non-ReadTimeout exception to be thrown on requests 3 and 5, and we expect logs on
        // each Non-ReadTimeout Exception, no matter what the number of ReadTimeoutsToIgnore we pass in.
        Exception exceptionToThrow = new RuntimeException("Uh oh Not a ReadTimeout");

        // Setup test expectations
        int readTimeoutsToIgnore = 0;
        int expectedReadTimeoutLogs = 0;
        int expectedGenericLogs = 2;
        TestShardConsumerSubscriber consumer = new TestShardConsumerSubscriber(mock(RecordsPublisher.class),
                Executors.newFixedThreadPool(1), 8, shardConsumer, readTimeoutsToIgnore);
        consumer.startSubscriptions();
        mimicSuccess(consumer);
        mimicSuccess(consumer);
        mimicException(exceptionToThrow, consumer);
        mimicSuccess(consumer);
        mimicException(exceptionToThrow, consumer);
        assertEquals(expectedGenericLogs, consumer.genericWarningLogged);
        assertEquals(expectedReadTimeoutLogs, consumer.readTimeoutWarningLogged);
    }

    /**
     * Test to validate the non-timeout warning message from ShardConsumer is not suppressed with 2 ReadTimeouts to
     * ignore
     */
    @Test
    public void loggingNotSuppressedOnNonReadTimeoutExceptionIgnoringReadTimeoutsTest() {
        // We're not throwing a ReadTimeout, so no suppression is expected.
        // The test expects a non-ReadTimeout exception to be thrown on requests 3 and 5, and we expect logs on
        // each Non-ReadTimeout Exception, no matter what the number of ReadTimeoutsToIgnore we pass in,
        // in this case if we had instead thrown ReadTimeouts, we would not have expected any logs with this
        // configuration.
        Exception exceptionToThrow = new RuntimeException("Uh oh Not a ReadTimeout");

        // Setup test expectations
        int readTimeoutsToIgnore = 2;
        int expectedReadTimeoutLogs = 0;
        int expectedGenericLogs = 2;
        TestShardConsumerSubscriber consumer = new TestShardConsumerSubscriber(mock(RecordsPublisher.class),
                Executors.newFixedThreadPool(1), 8, shardConsumer, readTimeoutsToIgnore);
        consumer.startSubscriptions();
        mimicSuccess(consumer);
        mimicSuccess(consumer);
        mimicException(exceptionToThrow, consumer);
        mimicSuccess(consumer);
        mimicException(exceptionToThrow, consumer);
        assertEquals(expectedGenericLogs, consumer.genericWarningLogged);
        assertEquals(expectedReadTimeoutLogs, consumer.readTimeoutWarningLogged);
    }

    private void mimicSuccess(TestShardConsumerSubscriber consumer) {
        // Mimic a successful publishing request
        consumer.onNext(recordsRetrieved);
    }

    private void mimicException(Exception exceptionToThrow, TestShardConsumerSubscriber consumer) {
        // Mimic throwing an exception during publishing,
        consumer.onError(exceptionToThrow);
        // restart subscriptions to allow further requests to be mimiced
        consumer.startSubscriptions();
    }

}
