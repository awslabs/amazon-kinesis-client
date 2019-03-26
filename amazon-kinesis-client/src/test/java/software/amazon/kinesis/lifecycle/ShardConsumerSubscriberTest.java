/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.lifecycle;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class ShardConsumerSubscriberTest {

    private final Object processedNotifier = new Object();

    private static final String TERMINAL_MARKER = "Terminal";

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

        ShardInfo shardInfo = new ShardInfo("shard-001", "", Collections.emptyList(),
                ExtendedSequenceNumber.TRIM_HORIZON);
        when(shardConsumer.shardInfo()).thenReturn(shardInfo);

        processRecordsInput = ProcessRecordsInput.builder().records(Collections.emptyList())
                .cacheEntryTime(Instant.now()).build();

        subscriber = new ShardConsumerSubscriber(recordsPublisher, executorService, bufferSize, shardConsumer,0);
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

        synchronized (processedNotifier) {
            subscriber.startSubscriptions();
            processedNotifier.wait(5000);
        }

        verify(shardConsumer).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)), any(Subscription.class));
    }

    @Test
    public void multipleItemTest() throws Exception {
        addItemsToReturn(100);

        setupNotifierAnswer(recordsPublisher.responses.size());

        synchronized (processedNotifier) {
            subscriber.startSubscriptions();
            processedNotifier.wait(5000);
        }

        verify(shardConsumer, times(100)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)),
                any(Subscription.class));
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

        synchronized (processedNotifier) {
            subscriber.startSubscriptions();
            processedNotifier.wait(5000);
        }

        assertThat(subscriber.getAndResetDispatchFailure(), equalTo(testException));
        assertThat(subscriber.getAndResetDispatchFailure(), nullValue());

        verify(shardConsumer, times(20)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)),
                any(Subscription.class));

    }

    @Test
    public void onErrorStopsProcessingTest() throws Exception {
        Throwable expected = new Throwable("Wheee");
        addItemsToReturn(10);
        recordsPublisher.add(new ResponseItem(expected));
        addItemsToReturn(10);

        setupNotifierAnswer(10);

        synchronized (processedNotifier) {
            subscriber.startSubscriptions();
            processedNotifier.wait(5000);
        }

        for (int attempts = 0; attempts < 10; attempts++) {
            if (subscriber.retrievalFailure() != null) {
                break;
            }
            Thread.sleep(10);
        }

        verify(shardConsumer, times(10)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)),
                any(Subscription.class));
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

        synchronized (processedNotifier) {
            subscriber.startSubscriptions();
            processedNotifier.wait(5000);
        }

        for (int attempts = 0; attempts < 10; attempts++) {
            if (subscriber.retrievalFailure() != null) {
                break;
            }
            Thread.sleep(100);
        }

        setupNotifierAnswer(10);

        synchronized (processedNotifier) {
            assertThat(subscriber.healthCheck(100000), equalTo(expected));
            processedNotifier.wait(5000);
        }

        assertThat(recordsPublisher.restartedFrom, equalTo(edgeRecord));
        verify(shardConsumer, times(20)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)),
                any(Subscription.class));
    }

    @Test
    public void restartAfterRequestTimerExpiresTest() throws Exception {

        executorService = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
                .setNameFormat("test-" + testName.getMethodName() + "-%04d").setDaemon(true).build());

        subscriber = new ShardConsumerSubscriber(recordsPublisher, executorService, bufferSize, shardConsumer,0);
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

        synchronized (processedNotifier) {
            subscriber.startSubscriptions();
            processedNotifier.wait(5000);
        }

        synchronized (processedNotifier) {
            executorService.execute(() -> {
                try {
                    //
                    // Notify the test as soon as we have started executing, then wait on the post add subscriptionBarrier.
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
            processedNotifier.wait(5000);
        }

        Stream.iterate(2, i -> i + 1).limit(97).forEach(this::addUniqueItem);

        addTerminalMarker(2);

        synchronized (processedNotifier) {
            assertThat(subscriber.healthCheck(1), nullValue());
            barrier.await(500, TimeUnit.MILLISECONDS);

            processedNotifier.wait(5000);
        }

        verify(shardConsumer, times(100)).handleInput(argThat(eqProcessRecordsInput(processRecordsInput)),
                any(Subscription.class));

        assertThat(received.size(), equalTo(recordsPublisher.responses.size()));
        Stream.iterate(0, i -> i + 1).limit(received.size()).forEach(i -> assertThat(received.get(i),
                eqProcessRecordsInput(recordsPublisher.responses.get(i).recordsRetrieved.processRecordsInput())));

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
        private volatile long requested = 0;
        private int currentIndex = 0;
        private Subscriber<? super RecordsRetrieved> subscriber;
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
        public void shutdown() {

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

}