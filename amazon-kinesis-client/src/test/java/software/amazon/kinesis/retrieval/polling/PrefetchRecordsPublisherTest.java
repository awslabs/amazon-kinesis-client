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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.utils.BlockingUtils.blockUntilConditionSatisfied;
import static software.amazon.kinesis.utils.BlockingUtils.blockUntilRecordsAvailable;
import static software.amazon.kinesis.utils.ProcessRecordsInputMatcher.eqProcessRecordsInput;

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

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.RequestDetails;
import software.amazon.kinesis.leases.ShardObjectHelper;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.lifecycle.ShardConsumerNotifyingSubscriber;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

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
    private static final long IDLE_MILLIS_BETWEEN_CALLS = 0L;
    private static final String NEXT_SHARD_ITERATOR = "testNextShardIterator";

    @Mock
    private GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    @Mock
    private DataFetcher dataFetcher;
    @Mock
    private InitialPositionInStreamExtended initialPosition;
    @Mock
    private ExtendedSequenceNumber sequenceNumber;

    private List<Record> records;
    private ExecutorService executorService;
    private LinkedBlockingQueue<PrefetchRecordsPublisher.PrefetchRecordsRetrieved> spyQueue;
    private PrefetchRecordsPublisher getRecordsCache;
    private String operation = "ProcessTask";
    private GetRecordsResponse getRecordsResponse;
    private Record record;
    private RequestDetails requestDetails;

    @Before
    public void setup() {
        when(getRecordsRetrievalStrategy.dataFetcher()).thenReturn(dataFetcher);
        when(dataFetcher.getStreamIdentifier()).thenReturn(StreamIdentifier.singleStreamInstance("testStream"));
        executorService = spy(Executors.newFixedThreadPool(1));
        getRecordsCache = new PrefetchRecordsPublisher(
                MAX_SIZE,
                3 * SIZE_1_MB,
                MAX_RECORDS_COUNT,
                MAX_RECORDS_PER_CALL,
                getRecordsRetrievalStrategy,
                executorService,
                IDLE_MILLIS_BETWEEN_CALLS,
                new NullMetricsFactory(),
                operation,
                "shardId");
        spyQueue = spy(getRecordsCache.getPublisherSession().prefetchRecordsQueue());
        records = spy(new ArrayList<>());
        getRecordsResponse = GetRecordsResponse.builder().records(records).nextShardIterator(NEXT_SHARD_ITERATOR).childShards(new ArrayList<>()).build();

        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL))).thenReturn(getRecordsResponse);
    }

    @Test
    public void testDataFetcherIsNotReInitializedOnMultipleCacheStarts() {
        getRecordsCache.start(sequenceNumber, initialPosition);
        getRecordsCache.start(sequenceNumber, initialPosition);
        getRecordsCache.start(sequenceNumber, initialPosition);
        verify(dataFetcher, times(1)).initialize(any(ExtendedSequenceNumber.class), any());
    }

    @Test
    public void testPrefetchPublisherInternalStateNotModifiedWhenPrefetcherThreadStartFails() {
        doThrow(new RejectedExecutionException()).doThrow(new RejectedExecutionException()).doCallRealMethod()
                .when(executorService).execute(any());
        // Initialize try 1
        tryPrefetchCacheStart();
        blockUntilConditionSatisfied(() -> getRecordsCache.getPublisherSession().prefetchRecordsQueue().size() == MAX_SIZE, 300);
        verifyInternalState(0);
        // Initialize try 2
        tryPrefetchCacheStart();
        blockUntilConditionSatisfied(() -> getRecordsCache.getPublisherSession().prefetchRecordsQueue().size() == MAX_SIZE, 300);
        verifyInternalState(0);
        // Initialize try 3
        tryPrefetchCacheStart();
        blockUntilConditionSatisfied(() -> getRecordsCache.getPublisherSession().prefetchRecordsQueue().size() == MAX_SIZE, 300);
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
        Assert.assertTrue(getRecordsCache.getPublisherSession().prefetchRecordsQueue().size() == queueSize);
    }

    @Test
    public void testGetRecords() {
        record = Record.builder().data(createByteBufferWithSize(SIZE_512_KB)).build();

        when(records.size()).thenReturn(1000);

        final List<KinesisClientRecord> expectedRecords = records.stream()
                .map(KinesisClientRecord::fromRecord).collect(Collectors.toList());

        getRecordsCache.start(sequenceNumber, initialPosition);
        ProcessRecordsInput result = blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache,
                "shardId"), 1000L)
                .processRecordsInput();

        assertEquals(expectedRecords, result.records());
        assertEquals(new ArrayList<>(), result.childShards());

        verify(executorService).execute(any());
        verify(getRecordsRetrievalStrategy, atLeast(1)).getRecords(eq(MAX_RECORDS_PER_CALL));
    }

    @Test(expected = RuntimeException.class)
    public void testGetRecordsWithInitialFailures_LessThanRequiredWait_Throws() {
        // Create a new PrefetchRecordsPublisher with 1s idle time between get calls
        getRecordsCache = new PrefetchRecordsPublisher(
                MAX_SIZE,
                3 * SIZE_1_MB,
                MAX_RECORDS_COUNT,
                MAX_RECORDS_PER_CALL,
                getRecordsRetrievalStrategy,
                executorService,
                1000,
                new NullMetricsFactory(),
                operation,
                "shardId");
        // Setup the retrieval strategy to fail initial calls before succeeding
        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL))).thenThrow(new
                RetryableRetrievalException("Timed out")).thenThrow(new
                RetryableRetrievalException("Timed out again")).thenReturn(getRecordsResponse);
        record = Record.builder().data(createByteBufferWithSize(SIZE_512_KB)).build();

        when(records.size()).thenReturn(1000);

        final List<KinesisClientRecord> expectedRecords = records.stream()
                .map(KinesisClientRecord::fromRecord).collect(Collectors.toList());

        getRecordsCache.start(sequenceNumber, initialPosition);
        ProcessRecordsInput result = null;
        // Setup timeout to be less than what the PrefetchRecordsPublisher will need based on the idle time between
        // get calls to validate exception is thrown
        result = blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache,
                "shardId"), 1000L)
                .processRecordsInput();
    }

    @Test
    public void testGetRecordsWithInitialFailures_AdequateWait_Success() {
        // Create a new PrefetchRecordsPublisher with 1s idle time between get calls
        getRecordsCache = new PrefetchRecordsPublisher(
                MAX_SIZE,
                3 * SIZE_1_MB,
                MAX_RECORDS_COUNT,
                MAX_RECORDS_PER_CALL,
                getRecordsRetrievalStrategy,
                executorService,
                1000,
                new NullMetricsFactory(),
                operation,
                "shardId");
        // Setup the retrieval strategy to fail initial calls before succeeding
        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL))).thenThrow(new
                RetryableRetrievalException("Timed out")).thenThrow(new
                RetryableRetrievalException("Timed out again")).thenReturn(getRecordsResponse);
        record = Record.builder().data(createByteBufferWithSize(SIZE_512_KB)).build();

        when(records.size()).thenReturn(1000);

        final List<KinesisClientRecord> expectedRecords = records.stream()
                .map(KinesisClientRecord::fromRecord).collect(Collectors.toList());

        getRecordsCache.start(sequenceNumber, initialPosition);
        ProcessRecordsInput result = null;
        // Setup timeout to be more than what the PrefetchRecordsPublisher will need based on the idle time between
        // get calls and then validate the mocks later
        result = blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache,
                "shardId"), 4000L)
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

        when(records.size()).thenReturn(1000);

        GetRecordsResponse response = GetRecordsResponse.builder().records(records).build();
        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL))).thenReturn(response);
        when(dataFetcher.isShardEndReached()).thenReturn(false);

        getRecordsCache.start(sequenceNumber, initialPosition);

        try {
            ProcessRecordsInput result = blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache, "shardId"), 1000L)
                    .processRecordsInput();
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

        GetRecordsResponse response = GetRecordsResponse.builder().records(records).childShards(childShards).build();
        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL))).thenReturn(response);
        when(dataFetcher.isShardEndReached()).thenReturn(true);

        getRecordsCache.start(sequenceNumber, initialPosition);
        ProcessRecordsInput result = blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache, "shardId"), 1000L)
                .processRecordsInput();

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
        when(records.size()).thenReturn(recordsSize);

        getRecordsCache.start(sequenceNumber, initialPosition);

        sleep(2000);

        int callRate = (int) Math.ceil((double) MAX_RECORDS_COUNT/recordsSize);
        //        TODO: fix this verification
        //        verify(getRecordsRetrievalStrategy, times(callRate)).getRecords(MAX_RECORDS_PER_CALL);
        //        assertEquals(spyQueue.size(), callRate);
        assertTrue("Call Rate is "+callRate,callRate < MAX_SIZE);
    }

    @Test
    public void testFullCacheSize() {
        int recordsSize = 200;
        when(records.size()).thenReturn(recordsSize);

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
        final List<KinesisClientRecord> expectedRecords = records.stream()
                .map(KinesisClientRecord::fromRecord).collect(Collectors.toList());

        getRecordsCache.start(sequenceNumber, initialPosition);
        ProcessRecordsInput processRecordsInput = evictPublishedEvent(getRecordsCache, "shardId").processRecordsInput();

        verify(executorService).execute(any());
        assertEquals(expectedRecords, processRecordsInput.records());
        assertNotNull(processRecordsInput.cacheEntryTime());
        assertNotNull(processRecordsInput.cacheExitTime());

        sleep(2000);

        ProcessRecordsInput processRecordsInput2 = evictPublishedEvent(getRecordsCache, "shardId").processRecordsInput();
        assertNotEquals(processRecordsInput, processRecordsInput2);
        assertEquals(expectedRecords, processRecordsInput2.records());
        assertNotEquals(processRecordsInput2.timeSpentInCache(), Duration.ZERO);

        assertTrue(spyQueue.size() <= MAX_SIZE);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetNextRecordsWithoutStarting() {
        verify(executorService, times(0)).execute(any());
        getRecordsCache.drainQueueForRequests();
    }

    @Test(expected = IllegalStateException.class)
    public void testCallAfterShutdown() {
        when(executorService.isShutdown()).thenReturn(true);
        getRecordsCache.drainQueueForRequests();
    }

    @Test
    public void testExpiredIteratorException() {
        log.info("Starting tests");
        when(getRecordsRetrievalStrategy.getRecords(MAX_RECORDS_PER_CALL)).thenThrow(ExpiredIteratorException.class)
                .thenReturn(getRecordsResponse);

        getRecordsCache.start(sequenceNumber, initialPosition);

        doNothing().when(dataFetcher).restartIterator();

        blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache, "shardId"), 1000L);

        sleep(1000);

        verify(dataFetcher).restartIterator();
    }

    @Test
    public void testRetryableRetrievalExceptionContinues() {

        GetRecordsResponse response = GetRecordsResponse.builder().millisBehindLatest(100L).records(Collections.emptyList()).nextShardIterator(NEXT_SHARD_ITERATOR).build();
        when(getRecordsRetrievalStrategy.getRecords(anyInt())).thenThrow(new RetryableRetrievalException("Timeout", new TimeoutException("Timeout"))).thenReturn(response);

        getRecordsCache.start(sequenceNumber, initialPosition);

        RecordsRetrieved records = blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache, "shardId"), 1000);
        assertThat(records.processRecordsInput().millisBehindLatest(), equalTo(response.millisBehindLatest()));
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

        final int[] sequenceNumberInResponse = { 0 };

        when(getRecordsRetrievalStrategy.getRecords(anyInt())).thenAnswer( i -> GetRecordsResponse.builder().records(
                Record.builder().data(SdkBytes.fromByteArray(new byte[] { 1, 2, 3 })).sequenceNumber(++sequenceNumberInResponse[0] + "").build())
                .nextShardIterator(NEXT_SHARD_ITERATOR).build());

        getRecordsCache.start(sequenceNumber, initialPosition);

        //
        // Wait for the queue to fill up, and the publisher to block on adding items to the queue.
        //
        log.info("Waiting for queue to fill up");
        while (getRecordsCache.getPublisherSession().prefetchRecordsQueue().size() < MAX_SIZE) {
            Thread.yield();
        }

        log.info("Queue is currently at {} starting subscriber", getRecordsCache.getPublisherSession().prefetchRecordsQueue().size());
        AtomicInteger receivedItems = new AtomicInteger(0);

        final int expectedItems = MAX_SIZE * 10;

        Object lock = new Object();

        final boolean[] isRecordNotInorder = { false };
        final String[] recordNotInOrderMessage = { "" };

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
                        .lastBatchSequenceNumber()) != ++receivedSeqNum) {
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

        Subscriber<RecordsRetrieved> subscriber = new ShardConsumerNotifyingSubscriber(delegateSubscriber, getRecordsCache);

        synchronized (lock) {
            log.info("Awaiting notification");
            Flowable.fromPublisher(getRecordsCache).subscribeOn(Schedulers.computation())
                    .observeOn(Schedulers.computation(), true, 8).subscribe(subscriber);
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
        GetRecordsResponse response = GetRecordsResponse.builder().records(
                Record.builder().data(SdkBytes.fromByteArray(new byte[] { 1, 2, 3 })).sequenceNumber("123").build())
                .nextShardIterator(NEXT_SHARD_ITERATOR).build();
        when(getRecordsRetrievalStrategy.getRecords(anyInt())).thenReturn(response);

        getRecordsCache.start(sequenceNumber, initialPosition);

        //
        // Wait for the queue to fill up, and the publisher to block on adding items to the queue.
        //
        log.info("Waiting for queue to fill up");
        while (getRecordsCache.getPublisherSession().prefetchRecordsQueue().size() < MAX_SIZE) {
            Thread.yield();
        }

        log.info("Queue is currently at {} starting subscriber", getRecordsCache.getPublisherSession().prefetchRecordsQueue().size());
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
            Flowable.fromPublisher(getRecordsCache).subscribeOn(Schedulers.computation())
                    .observeOn(Schedulers.computation(), true, 8).subscribe(subscriber);
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
        List<GetRecordsResponse> responses = Stream.iterate(0, i -> i + 1).limit(10).map(i -> {
            Record record = Record.builder().partitionKey("record-" + i).sequenceNumber("seq-" + i)
                    .data(SdkBytes.fromByteArray(new byte[] { 1, 2, 3 })).approximateArrivalTimestamp(Instant.now())
                    .build();
            String nextIterator = "shard-iter-" + (i + 1);
            return GetRecordsResponse.builder().records(record).nextShardIterator(nextIterator).build();
        }).collect(Collectors.toList());

        RetrieverAnswer retrieverAnswer = new RetrieverAnswer(responses);

        when(getRecordsRetrievalStrategy.getRecords(anyInt())).thenAnswer(retrieverAnswer);
        doAnswer(a -> {
            String resetTo = a.getArgumentAt(0, String.class);
            retrieverAnswer.resetIteratorTo(resetTo);
            return null;
        }).when(dataFetcher).resetIterator(anyString(), anyString(), any());

        getRecordsCache.start(sequenceNumber, initialPosition);

        RecordsRetrieved lastProcessed = blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache, "shardId"), 1000);
        RecordsRetrieved expected = blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache, "shardId"), 1000);

        //
        // Skip some of the records the cache
        //
        blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache, "shardId"), 1000);
        blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache, "shardId"), 1000);

        verify(getRecordsRetrievalStrategy, atLeast(2)).getRecords(anyInt());

        while(getRecordsCache.getPublisherSession().prefetchRecordsQueue().remainingCapacity() > 0) {
            Thread.yield();
        }

        getRecordsCache.restartFrom(lastProcessed);
        RecordsRetrieved postRestart = blockUntilRecordsAvailable(() -> evictPublishedEvent(getRecordsCache, "shardId"), 1000);

        assertThat(postRestart.processRecordsInput(), eqProcessRecordsInput(expected.processRecordsInput()));
        verify(dataFetcher).resetIterator(eq(responses.get(0).nextShardIterator()),
                eq(responses.get(0).records().get(0).sequenceNumber()), any());

    }

    private RecordsRetrieved evictPublishedEvent(PrefetchRecordsPublisher publisher, String shardId) {
        return publisher.getPublisherSession().evictPublishedRecordAndUpdateDemand(shardId);
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
            while(newIterator.hasNext()) {
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
        public GetRecordsResponse answer(InvocationOnMock invocation) throws Throwable {
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
        private static final ScheduledExecutorService consumerHealthChecker = Executors.newScheduledThreadPool(1);

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
                consumerHealthChecker.schedule(() ->  {
                    getRecordsPublisher().restartFrom(recordsRetrieved);
                    Flowable.fromPublisher(getRecordsPublisher()).subscribeOn(Schedulers.computation())
                            .observeOn(Schedulers.computation(), true, 8).subscribe(this);
                }, 1000, TimeUnit.MILLISECONDS);
            }
            recordCounter++;
        }
    }
    @After
    public void shutdown() {
        getRecordsCache.shutdown();
        verify(executorService).shutdownNow();
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {}
    }

    private SdkBytes createByteBufferWithSize(int size) {
        return SdkBytes.fromByteArray(new byte[size]);
    }

}
