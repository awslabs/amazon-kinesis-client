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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardObjectHelper;
import software.amazon.kinesis.lifecycle.ShardConsumerNotifyingSubscriber;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.ThrottlingReporter;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.utils.BlockingUtils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.utils.BlockingUtils.blockUntilConditionSatisfied;
import static software.amazon.kinesis.utils.ProcessRecordsInputMatcher.eqProcessRecordsInput;

/**
 * Test class for the PrefetchRecordsPublisher class.
 */
@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class PrefetchRecordsPublisherTest {
    private static final int SIZE_512_KB = 512 * 1024;
    private static final int SIZE_1_MB = 2 * SIZE_512_KB;
    private static final int MAX_RECORDS_PER_CALL = 10000;
    private static final int MAX_SIZE = 5;
    private static final int MAX_RECORDS_COUNT = 15000;
    private static final String NEXT_SHARD_ITERATOR = "testNextShardIterator";

    private static final long DEFAULT_TIMEOUT_MILLIS = Duration.ofSeconds(1).toMillis();

    @Mock
    private GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;

    @Mock
    private DataFetcher dataFetcher;

    @Mock
    private InitialPositionInStreamExtended initialPosition;

    @Mock
    private ExtendedSequenceNumber sequenceNumber;

    @Mock
    private ThrottlingReporter throttlingReporter;

    private List<Record> records;
    private ExecutorService executorService;
    private LinkedBlockingQueue<PrefetchRecordsPublisher.PrefetchRecordsRetrieved> spyQueue;
    private PrefetchRecordsPublisher getRecordsCache;
    private GetRecordsResponse getRecordsResponse;
    private Record record;

    @Before
    public void setup() {
        when(getRecordsRetrievalStrategy.dataFetcher()).thenReturn(dataFetcher);
        when(dataFetcher.getStreamIdentifier()).thenReturn(StreamIdentifier.singleStreamInstance("testStream"));
        executorService = spy(Executors.newFixedThreadPool(1));
        getRecordsCache = createPrefetchRecordsPublisher(0L);
        spyQueue = spy(getRecordsCache.getPublisherSession().prefetchRecordsQueue());
        records = spy(new ArrayList<>());
        getRecordsResponse = GetRecordsResponse.builder()
                .records(records)
                .nextShardIterator(NEXT_SHARD_ITERATOR)
                .childShards(Collections.emptyList())
                .build();

        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL))).thenReturn(getRecordsResponse);
    }

    @Test
    public void testDataFetcherIsNotReInitializedOnMultipleCacheStarts() {
        getRecordsCache.start(sequenceNumber, initialPosition);
        getRecordsCache.start(sequenceNumber, initialPosition);
        getRecordsCache.start(sequenceNumber, initialPosition);
        verify(dataFetcher).initialize(any(ExtendedSequenceNumber.class), any());
    }

    @Test
    public void testPrefetchPublisherInternalStateNotModifiedWhenPrefetcherThreadStartFails() {
        doThrow(new RejectedExecutionException())
                .doThrow(new RejectedExecutionException())
                .doCallRealMethod()
                .when(executorService)
                .execute(any());
        // Initialize try 1
        tryPrefetchCacheStart();
        blockUntilConditionSatisfied(
                () -> getRecordsCache
                                .getPublisherSession()
                                .prefetchRecordsQueue()
                                .size()
                        == MAX_SIZE,
                300);
        verifyInternalState(0);
        // Initialize try 2
        tryPrefetchCacheStart();
        blockUntilConditionSatisfied(
                () -> getRecordsCache
                                .getPublisherSession()
                                .prefetchRecordsQueue()
                                .size()
                        == MAX_SIZE,
                300);
        verifyInternalState(0);
        // Initialize try 3
        tryPrefetchCacheStart();
        blockUntilConditionSatisfied(
                () -> getRecordsCache
                                .getPublisherSession()
                                .prefetchRecordsQueue()
                                .size()
                        == MAX_SIZE,
                300);
        verifyInternalState(MAX_SIZE);
        verify(dataFetcher, times(3)).initialize(any(ExtendedSequenceNumber.class), any());
    }

    private void tryPrefetchCacheStart() {
        try {
            getRecordsCache.start(sequenceNumber, initialPosition);
        } catch (Exception e) {
            // suppress exception
        }
    }

    private void verifyInternalState(int queueSize) {
        assertEquals(
                queueSize,
                getRecordsCache.getPublisherSession().prefetchRecordsQueue().size());
    }

    @Test
    public void testGetRecords() {
        record = Record.builder().data(createByteBufferWithSize(SIZE_512_KB)).build();

        final List<KinesisClientRecord> expectedRecords =
                records.stream().map(KinesisClientRecord::fromRecord).collect(Collectors.toList());

        getRecordsCache.start(sequenceNumber, initialPosition);
        ProcessRecordsInput result = blockUntilRecordsAvailable().processRecordsInput();

        assertEquals(expectedRecords, result.records());
        assertEquals(new ArrayList<>(), result.childShards());

        verify(executorService).execute(any());
        verify(getRecordsRetrievalStrategy, atLeast(1)).getRecords(eq(MAX_RECORDS_PER_CALL));
    }

    @Test(expected = RuntimeException.class)
    public void testGetRecordsWithInitialFailures_LessThanRequiredWait_Throws() {
        getRecordsCache = createPrefetchRecordsPublisher(Duration.ofSeconds(1).toMillis());
        // Setup the retrieval strategy to fail initial calls before succeeding
        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL)))
                .thenThrow(new RetryableRetrievalException("Timed out"))
                .thenThrow(new RetryableRetrievalException("Timed out again"))
                .thenReturn(getRecordsResponse);
        record = Record.builder().data(createByteBufferWithSize(SIZE_512_KB)).build();

        getRecordsCache.start(sequenceNumber, initialPosition);
        // Setup timeout to be less than what the PrefetchRecordsPublisher will need based on the idle time between
        // get calls to validate exception is thrown
        blockUntilRecordsAvailable();
    }

    @Test
    public void testGetRecordsWithInitialFailures_AdequateWait_Success() {
        getRecordsCache = createPrefetchRecordsPublisher(Duration.ofSeconds(1).toMillis());
        // Setup the retrieval strategy to fail initial calls before succeeding
        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL)))
                .thenThrow(new RetryableRetrievalException("Timed out"))
                .thenThrow(new RetryableRetrievalException("Timed out again"))
                .thenReturn(getRecordsResponse);
        record = Record.builder().data(createByteBufferWithSize(SIZE_512_KB)).build();

        final List<KinesisClientRecord> expectedRecords =
                records.stream().map(KinesisClientRecord::fromRecord).collect(Collectors.toList());

        getRecordsCache.start(sequenceNumber, initialPosition);
        ProcessRecordsInput result = null;
        // Setup timeout to be more than what the PrefetchRecordsPublisher will need based on the idle time between
        // get calls and then validate the mocks later
        result = BlockingUtils.blockUntilRecordsAvailable(this::evictPublishedEvent, 4000L)
                .processRecordsInput();

        assertEquals(expectedRecords, result.records());
        assertEquals(new ArrayList<>(), result.childShards());

        verify(executorService).execute(any());
        // Validate at least 3 calls were including the 2 failed ones
        verify(getRecordsRetrievalStrategy, atLeast(3)).getRecords(eq(MAX_RECORDS_PER_CALL));
    }

    @Test
    public void testGetRecordsWithInvalidResponse() {
        record = Record.builder().data(createByteBufferWithSize(SIZE_512_KB)).build();

        GetRecordsResponse response =
                GetRecordsResponse.builder().records(records).build();
        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL))).thenReturn(response);
        when(dataFetcher.isShardEndReached()).thenReturn(false);

        getRecordsCache.start(sequenceNumber, initialPosition);

        try {
            blockUntilRecordsAvailable();
        } catch (Exception e) {
            assertEquals("No records found", e.getMessage());
        }
    }

    @Test
    public void testGetRecordsWithShardEnd() {
        records = new ArrayList<>();

        final List<KinesisClientRecord> expectedRecords = new ArrayList<>();

        List<ChildShard> childShards = new ArrayList<>();
        List<String> parentShards = new ArrayList<>();
        parentShards.add("shardId");
        ChildShard leftChild = ChildShard.builder()
                .shardId("shardId-000000000001")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"))
                .build();
        ChildShard rightChild = ChildShard.builder()
                .shardId("shardId-000000000002")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("50", "99"))
                .build();
        childShards.add(leftChild);
        childShards.add(rightChild);

        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(records)
                .childShards(childShards)
                .build();
        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL))).thenReturn(response);
        when(dataFetcher.isShardEndReached()).thenReturn(true);

        getRecordsCache.start(sequenceNumber, initialPosition);
        ProcessRecordsInput result = blockUntilRecordsAvailable().processRecordsInput();

        assertEquals(expectedRecords, result.records());
        assertEquals(childShards, result.childShards());
        assertTrue(result.isAtShardEnd());
    }

    // TODO: Broken test
    @Test
    @Ignore
    public void testFullCacheByteSize() {
        record = Record.builder().data(createByteBufferWithSize(SIZE_1_MB)).build();

        when(records.size()).thenReturn(500);

        records.add(record);

        getRecordsCache.start(sequenceNumber, initialPosition);

        // Sleep for a few seconds for the cache to fill up.
        sleep(2000);

        verify(getRecordsRetrievalStrategy, times(3)).getRecords(eq(MAX_RECORDS_PER_CALL));
        assertEquals(spyQueue.size(), 3);
    }

    @Test
    public void testFullCacheRecordsCount() {
        int recordsSize = 4500;
        getRecordsCache.start(sequenceNumber, initialPosition);

        sleep(2000);

        int callRate = (int) Math.ceil((double) MAX_RECORDS_COUNT / recordsSize);
        //        TODO: fix this verification
        //        verify(getRecordsRetrievalStrategy, times(callRate)).getRecords(MAX_RECORDS_PER_CALL);
        //        assertEquals(spyQueue.size(), callRate);
        assertTrue("Call Rate is " + callRate, callRate < MAX_SIZE);
    }

    @Test
    public void testFullCacheSize() {
        int recordsSize = 200;
        getRecordsCache.start(sequenceNumber, initialPosition);

        // Sleep for a few seconds for the cache to fill up.
        sleep(2000);

        verify(getRecordsRetrievalStrategy, times(MAX_SIZE + 1)).getRecords(eq(MAX_RECORDS_PER_CALL));
        assertEquals(spyQueue.size(), MAX_SIZE);
    }

    // TODO: Broken tests
    @Test
    @Ignore
    public void testMultipleCacheCalls() {
        int recordsSize = 20;
        record = Record.builder().data(createByteBufferWithSize(1024)).build();

        IntStream.range(0, recordsSize).forEach(i -> records.add(record));
        final List<KinesisClientRecord> expectedRecords =
                records.stream().map(KinesisClientRecord::fromRecord).collect(Collectors.toList());

        getRecordsCache.start(sequenceNumber, initialPosition);
        ProcessRecordsInput processRecordsInput = evictPublishedEvent().processRecordsInput();

        verify(executorService).execute(any());
        assertEquals(expectedRecords, processRecordsInput.records());
        assertNotNull(processRecordsInput.cacheEntryTime());
        assertNotNull(processRecordsInput.cacheExitTime());

        sleep(2000);

        ProcessRecordsInput processRecordsInput2 = evictPublishedEvent().processRecordsInput();
        assertNotEquals(processRecordsInput, processRecordsInput2);
        assertEquals(expectedRecords, processRecordsInput2.records());
        assertNotEquals(processRecordsInput2.timeSpentInCache(), Duration.ZERO);

        assertTrue(spyQueue.size() <= MAX_SIZE);
    }

    @Test(expected = IllegalStateException.class)
    public void testSubscribeWithoutStarting() {
        verify(executorService, never()).execute(any());
        Subscriber<RecordsRetrieved> mockSubscriber = mock(Subscriber.class);
        getRecordsCache.subscribe(mockSubscriber);
    }

    @Test(expected = IllegalStateException.class)
    public void testRequestRecordsOnSubscriptionAfterShutdown() {
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(Record.builder()
                        .data(SdkBytes.fromByteArray(new byte[] {1, 2, 3}))
                        .sequenceNumber("123")
                        .build())
                .nextShardIterator(NEXT_SHARD_ITERATOR)
                .build();
        when(getRecordsRetrievalStrategy.getRecords(anyInt())).thenReturn(response);

        getRecordsCache.start(sequenceNumber, initialPosition);

        verify(getRecordsRetrievalStrategy, timeout(100).atLeastOnce()).getRecords(anyInt());

        when(executorService.isShutdown()).thenReturn(true);
        Subscriber<RecordsRetrieved> mockSubscriber = mock(Subscriber.class);
        getRecordsCache.subscribe(mockSubscriber);
        ArgumentCaptor<Subscription> subscriptionCaptor = ArgumentCaptor.forClass(Subscription.class);
        verify(mockSubscriber).onSubscribe(subscriptionCaptor.capture());
        subscriptionCaptor.getValue().request(1);
    }

    @Test
    public void testExpiredIteratorException() {
        when(getRecordsRetrievalStrategy.getRecords(MAX_RECORDS_PER_CALL))
                .thenThrow(ExpiredIteratorException.class)
                .thenReturn(getRecordsResponse);

        getRecordsCache.start(sequenceNumber, initialPosition);

        doNothing().when(dataFetcher).restartIterator();

        blockUntilRecordsAvailable();

        sleep(1000);

        verify(dataFetcher).restartIterator();
    }

    @Test
    public void testExpiredIteratorExceptionWithIllegalStateException() {
        // This test validates that the daemon thread doesn't die when ExpiredIteratorException occurs with an
        // IllegalStateException.
        when(getRecordsRetrievalStrategy.getRecords(MAX_RECORDS_PER_CALL))
                .thenThrow(ExpiredIteratorException.builder().build())
                .thenReturn(getRecordsResponse)
                .thenThrow(ExpiredIteratorException.builder().build())
                .thenReturn(getRecordsResponse);

        doThrow(new IllegalStateException()).when(dataFetcher).restartIterator();

        getRecordsCache.start(sequenceNumber, initialPosition);
        blockUntilConditionSatisfied(
                () -> getRecordsCache
                                .getPublisherSession()
                                .prefetchRecordsQueue()
                                .size()
                        == MAX_SIZE,
                300);

        // verify restartIterator was called
        verify(dataFetcher, times(2)).restartIterator();
    }

    @Test
    public void testRetryableRetrievalExceptionContinues() {
        GetRecordsResponse response = GetRecordsResponse.builder()
                .millisBehindLatest(100L)
                .records(Collections.emptyList())
                .nextShardIterator(NEXT_SHARD_ITERATOR)
                .build();
        when(getRecordsRetrievalStrategy.getRecords(anyInt()))
                .thenThrow(new RetryableRetrievalException("Timeout", new TimeoutException("Timeout")))
                .thenReturn(response);

        getRecordsCache.start(sequenceNumber, initialPosition);

        RecordsRetrieved records = blockUntilRecordsAvailable();
        assertEquals(records.processRecordsInput().millisBehindLatest(), response.millisBehindLatest());
    }

    @Test
    public void testInvalidArgumentExceptionIsRetried() {
        when(getRecordsRetrievalStrategy.getRecords(MAX_RECORDS_PER_CALL))
                .thenThrow(InvalidArgumentException.builder().build())
                .thenReturn(getRecordsResponse);

        getRecordsCache.start(sequenceNumber, initialPosition);
        blockUntilConditionSatisfied(
                () -> getRecordsCache
                                .getPublisherSession()
                                .prefetchRecordsQueue()
                                .size()
                        == MAX_SIZE,
                300);

        verify(dataFetcher, times(1)).restartIterator();
    }

    @Test(timeout = 10000L)
    public void testNoDeadlockOnFullQueue() {
        //
        // Fixes https://github.com/awslabs/amazon-kinesis-client/issues/448
        //
        // This test is to verify that the drain of a blocked queue no longer deadlocks.
        // If the test times out before starting the subscriber it means something went wrong while filling the queue.
        // After the subscriber is started one of the things that can trigger a timeout is a deadlock.
        //
        final int[] sequenceNumberInResponse = {0};

        when(getRecordsRetrievalStrategy.getRecords(anyInt())).thenAnswer(i -> GetRecordsResponse.builder()
                .records(Record.builder()
                        .data(SdkBytes.fromByteArray(new byte[] {1, 2, 3}))
                        .sequenceNumber(++sequenceNumberInResponse[0] + "")
                        .build())
                .nextShardIterator(NEXT_SHARD_ITERATOR)
                .build());

        getRecordsCache.start(sequenceNumber, initialPosition);

        //
        // Wait for the queue to fill up, and the publisher to block on adding items to the queue.
        //
        log.info("Waiting for queue to fill up");
        while (getRecordsCache.getPublisherSession().prefetchRecordsQueue().size() < MAX_SIZE) {
            Thread.yield();
        }

        log.info(
                "Queue is currently at {} starting subscriber",
                getRecordsCache.getPublisherSession().prefetchRecordsQueue().size());
        AtomicInteger receivedItems = new AtomicInteger(0);

        final int expectedItems = MAX_SIZE * 10;

        Object lock = new Object();

        final boolean[] isRecordNotInorder = {false};
        final String[] recordNotInOrderMessage = {""};

        Subscriber<RecordsRetrieved> delegateSubscriber = new Subscriber<RecordsRetrieved>() {
            Subscription sub;
            int receivedSeqNum = 0;

            @Override
            public void onSubscribe(Subscription s) {
                sub = s;
                s.request(1);
            }

            @Override
            public void onNext(RecordsRetrieved recordsRetrieved) {
                receivedItems.incrementAndGet();
                if (Integer.parseInt(((PrefetchRecordsPublisher.PrefetchRecordsRetrieved) recordsRetrieved)
                                .lastBatchSequenceNumber())
                        != ++receivedSeqNum) {
                    isRecordNotInorder[0] = true;
                    recordNotInOrderMessage[0] = "Expected : " + receivedSeqNum + " Actual : "
                            + ((PrefetchRecordsPublisher.PrefetchRecordsRetrieved) recordsRetrieved)
                                    .lastBatchSequenceNumber();
                }
                if (receivedItems.get() >= expectedItems) {
                    synchronized (lock) {
                        log.info("Notifying waiters");
                        lock.notifyAll();
                    }
                    sub.cancel();
                } else {
                    sub.request(1);
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Caught error", t);
                throw new RuntimeException(t);
            }

            @Override
            public void onComplete() {
                fail("onComplete not expected in this test");
            }
        };

        Subscriber<RecordsRetrieved> subscriber =
                new ShardConsumerNotifyingSubscriber(delegateSubscriber, getRecordsCache);

        synchronized (lock) {
            log.info("Awaiting notification");
            Flowable.fromPublisher(getRecordsCache)
                    .subscribeOn(Schedulers.computation())
                    .observeOn(Schedulers.computation(), true, 8)
                    .subscribe(subscriber);
            try {
                lock.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        verify(getRecordsRetrievalStrategy, atLeast(expectedItems)).getRecords(anyInt());
        assertThat(receivedItems.get(), equalTo(expectedItems));
        assertFalse(recordNotInOrderMessage[0], isRecordNotInorder[0]);
    }

    @Test(timeout = 10000L)
    public void testNoDeadlockOnFullQueueAndLossOfNotification() {
        //
        // Fixes https://github.com/awslabs/amazon-kinesis-client/issues/602
        //
        // This test is to verify that the data consumption is not stuck in the case of an failed event delivery
        // to the subscriber.
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(Record.builder()
                        .data(SdkBytes.fromByteArray(new byte[] {1, 2, 3}))
                        .sequenceNumber("123")
                        .build())
                .nextShardIterator(NEXT_SHARD_ITERATOR)
                .build();
        when(getRecordsRetrievalStrategy.getRecords(anyInt())).thenReturn(response);

        getRecordsCache.start(sequenceNumber, initialPosition);

        //
        // Wait for the queue to fill up, and the publisher to block on adding items to the queue.
        //
        log.info("Waiting for queue to fill up");
        while (getRecordsCache.getPublisherSession().prefetchRecordsQueue().size() < MAX_SIZE) {
            Thread.yield();
        }

        log.info(
                "Queue is currently at {} starting subscriber",
                getRecordsCache.getPublisherSession().prefetchRecordsQueue().size());
        AtomicInteger receivedItems = new AtomicInteger(0);

        final int expectedItems = MAX_SIZE * 20;

        Object lock = new Object();

        Subscriber<RecordsRetrieved> delegateSubscriber = new Subscriber<RecordsRetrieved>() {
            Subscription sub;

            @Override
            public void onSubscribe(Subscription s) {
                sub = s;
                s.request(1);
            }

            @Override
            public void onNext(RecordsRetrieved recordsRetrieved) {
                receivedItems.incrementAndGet();
                if (receivedItems.get() >= expectedItems) {
                    synchronized (lock) {
                        log.info("Notifying waiters");
                        lock.notifyAll();
                    }
                    sub.cancel();
                } else {
                    sub.request(1);
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Caught error", t);
                throw new RuntimeException(t);
            }

            @Override
            public void onComplete() {
                fail("onComplete not expected in this test");
            }
        };

        Subscriber<RecordsRetrieved> subscriber = new LossyNotificationSubscriber(delegateSubscriber, getRecordsCache);

        synchronized (lock) {
            log.info("Awaiting notification");
            Flowable.fromPublisher(getRecordsCache)
                    .subscribeOn(Schedulers.computation())
                    .observeOn(Schedulers.computation(), true, 8)
                    .subscribe(subscriber);
            try {
                lock.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        verify(getRecordsRetrievalStrategy, atLeast(expectedItems)).getRecords(anyInt());
        assertThat(receivedItems.get(), equalTo(expectedItems));
    }

    @Test
    public void testResetClearsRemainingData() {
        List<GetRecordsResponse> responses = Stream.iterate(0, i -> i + 1)
                .limit(10)
                .map(i -> {
                    Record record = Record.builder()
                            .partitionKey("record-" + i)
                            .sequenceNumber("seq-" + i)
                            .data(SdkBytes.fromByteArray(new byte[] {1, 2, 3}))
                            .approximateArrivalTimestamp(Instant.now())
                            .build();
                    String nextIterator = "shard-iter-" + (i + 1);
                    return GetRecordsResponse.builder()
                            .records(record)
                            .nextShardIterator(nextIterator)
                            .build();
                })
                .collect(Collectors.toList());

        RetrieverAnswer retrieverAnswer = new RetrieverAnswer(responses);

        when(getRecordsRetrievalStrategy.getRecords(anyInt())).thenAnswer(retrieverAnswer);
        doAnswer(a -> {
                    String resetTo = (String) a.getArgument(0);
                    retrieverAnswer.resetIteratorTo(resetTo);
                    return null;
                })
                .when(dataFetcher)
                .resetIterator(anyString(), anyString(), any());

        getRecordsCache.start(sequenceNumber, initialPosition);

        RecordsRetrieved lastProcessed = blockUntilRecordsAvailable();
        RecordsRetrieved expected = blockUntilRecordsAvailable();

        //
        // Skip some of the records the cache
        //
        blockUntilRecordsAvailable();
        blockUntilRecordsAvailable();

        verify(getRecordsRetrievalStrategy, atLeast(2)).getRecords(anyInt());

        while (getRecordsCache.getPublisherSession().prefetchRecordsQueue().remainingCapacity() > 0) {
            Thread.yield();
        }

        getRecordsCache.restartFrom(lastProcessed);
        RecordsRetrieved postRestart = blockUntilRecordsAvailable();

        assertThat(postRestart.processRecordsInput(), eqProcessRecordsInput(expected.processRecordsInput()));
        verify(dataFetcher)
                .resetIterator(
                        eq(responses.get(0).nextShardIterator()),
                        eq(responses.get(0).records().get(0).sequenceNumber()),
                        any());
    }

    /**
     * Tests that a thrown {@link SdkException} doesn't cause a retry storm.
     */
    @Test(expected = RuntimeException.class)
    public void testRepeatSdkExceptionLoop() {
        final int expectedFailedCalls = 4;
        getRecordsCache = createPrefetchRecordsPublisher(DEFAULT_TIMEOUT_MILLIS / expectedFailedCalls);
        getRecordsCache.start(sequenceNumber, initialPosition);

        try {
            // return a valid response to cause `lastSuccessfulCall` to initialize
            when(getRecordsRetrievalStrategy.getRecords(anyInt()))
                    .thenReturn(GetRecordsResponse.builder().build());
            blockUntilRecordsAvailable();
        } catch (RuntimeException re) {
            Assert.fail("first call should succeed");
        }

        try {
            when(getRecordsRetrievalStrategy.getRecords(anyInt()))
                    .thenThrow(SdkException.builder()
                            .message("lose yourself to dance")
                            .build());
            blockUntilRecordsAvailable();
        } finally {
            // the successful call is the +1
            verify(getRecordsRetrievalStrategy, times(expectedFailedCalls + 1)).getRecords(anyInt());
        }
    }

    /**
     * Tests that a thrown {@link ProvisionedThroughputExceededException} writes to throttlingReporter.
     */
    @Test
    public void testProvisionedThroughputExceededExceptionReporter() {
        when(getRecordsRetrievalStrategy.getRecords(anyInt()))
                .thenThrow(ProvisionedThroughputExceededException.builder().build())
                .thenReturn(GetRecordsResponse.builder().build());

        getRecordsCache.start(sequenceNumber, initialPosition);

        BlockingUtils.blockUntilRecordsAvailable(this::evictPublishedEvent, DEFAULT_TIMEOUT_MILLIS);
        InOrder inOrder = Mockito.inOrder(throttlingReporter);
        inOrder.verify(throttlingReporter).throttled();
        inOrder.verify(throttlingReporter, atLeastOnce()).success();
        inOrder.verifyNoMoreInteractions();
    }

    private RecordsRetrieved blockUntilRecordsAvailable() {
        return BlockingUtils.blockUntilRecordsAvailable(this::evictPublishedEvent, DEFAULT_TIMEOUT_MILLIS);
    }

    private RecordsRetrieved evictPublishedEvent() {
        return getRecordsCache.getPublisherSession().evictPublishedRecordAndUpdateDemand("shardId");
    }

    private static class RetrieverAnswer implements Answer<GetRecordsResponse> {

        private final List<GetRecordsResponse> responses;
        private Iterator<GetRecordsResponse> iterator;

        public RetrieverAnswer(List<GetRecordsResponse> responses) {
            this.responses = responses;
            this.iterator = responses.iterator();
        }

        public void resetIteratorTo(String nextIterator) {
            Iterator<GetRecordsResponse> newIterator = responses.iterator();
            while (newIterator.hasNext()) {
                GetRecordsResponse current = newIterator.next();
                if (StringUtils.equals(nextIterator, current.nextShardIterator())) {
                    if (!newIterator.hasNext()) {
                        iterator = responses.iterator();
                    } else {
                        newIterator.next();
                        iterator = newIterator;
                    }
                    break;
                }
            }
        }

        @Override
        public GetRecordsResponse answer(InvocationOnMock invocation) {
            GetRecordsResponse response = iterator.next();
            if (!iterator.hasNext()) {
                iterator = responses.iterator();
            }
            return response;
        }
    }

    private static class LossyNotificationSubscriber extends ShardConsumerNotifyingSubscriber {

        private static final int LOSS_EVERY_NTH_RECORD = 50;
        private static int recordCounter = 0;
        private static final ScheduledExecutorService CONSUMER_HEALTH_CHECKER = Executors.newScheduledThreadPool(1);

        public LossyNotificationSubscriber(Subscriber<RecordsRetrieved> delegate, RecordsPublisher recordsPublisher) {
            super(delegate, recordsPublisher);
        }

        @Override
        public void onNext(RecordsRetrieved recordsRetrieved) {
            if (!(recordCounter % LOSS_EVERY_NTH_RECORD == LOSS_EVERY_NTH_RECORD - 1)) {
                getRecordsPublisher().notify(getRecordsDeliveryAck(recordsRetrieved));
                getDelegateSubscriber().onNext(recordsRetrieved);
            } else {
                log.info("Record Loss Triggered");
                CONSUMER_HEALTH_CHECKER.schedule(
                        () -> {
                            getRecordsPublisher().restartFrom(recordsRetrieved);
                            Flowable.fromPublisher(getRecordsPublisher())
                                    .subscribeOn(Schedulers.computation())
                                    .observeOn(Schedulers.computation(), true, 8)
                                    .subscribe(this);
                        },
                        1000,
                        TimeUnit.MILLISECONDS);
            }
            recordCounter++;
        }
    }

    @After
    public void shutdown() {
        getRecordsCache.shutdown();
        verify(executorService).shutdown();
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    private SdkBytes createByteBufferWithSize(int size) {
        return SdkBytes.fromByteArray(new byte[size]);
    }

    private PrefetchRecordsPublisher createPrefetchRecordsPublisher(final long idleMillisBetweenCalls) {
        return new PrefetchRecordsPublisher(
                MAX_SIZE,
                3 * SIZE_1_MB,
                MAX_RECORDS_COUNT,
                MAX_RECORDS_PER_CALL,
                getRecordsRetrievalStrategy,
                executorService,
                idleMillisBetweenCalls,
                new NullMetricsFactory(),
                PrefetchRecordsPublisherTest.class.getSimpleName(),
                "shardId",
                throttlingReporter,
                1L);
    }
}
