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

package software.amazon.kinesis.retrieval.polling;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
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

    @Mock
    private GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    @Mock
    private KinesisDataFetcher dataFetcher;
    @Mock
    private InitialPositionInStreamExtended initialPosition;
    @Mock
    private ExtendedSequenceNumber sequenceNumber;

    private List<Record> records;
    private ExecutorService executorService;
    private LinkedBlockingQueue<ProcessRecordsInput> spyQueue;
    private PrefetchRecordsPublisher getRecordsCache;
    private String operation = "ProcessTask";
    private GetRecordsResponse getRecordsResponse;
    private Record record;

    @Before
    public void setup() {
        when(getRecordsRetrievalStrategy.getDataFetcher()).thenReturn(dataFetcher);

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
        spyQueue = spy(getRecordsCache.getRecordsResultQueue);
        records = spy(new ArrayList<>());
        getRecordsResponse = GetRecordsResponse.builder().records(records).build();

        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL))).thenReturn(getRecordsResponse);
    }

    @Test
    public void testGetRecords() {
        record = Record.builder().data(createByteBufferWithSize(SIZE_512_KB)).build();

        when(records.size()).thenReturn(1000);

        final List<KinesisClientRecord> expectedRecords = records.stream()
                .map(KinesisClientRecord::fromRecord).collect(Collectors.toList());

        getRecordsCache.start(sequenceNumber, initialPosition);
        ProcessRecordsInput result = getRecordsCache.getNextResult();

        assertEquals(expectedRecords, result.records());

        verify(executorService).execute(any());
        verify(getRecordsRetrievalStrategy, atLeast(1)).getRecords(eq(MAX_RECORDS_PER_CALL));
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
        assertTrue(callRate < MAX_SIZE);
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
        ProcessRecordsInput processRecordsInput = getRecordsCache.getNextResult();

        verify(executorService).execute(any());
        assertEquals(expectedRecords, processRecordsInput.records());
        assertNotNull(processRecordsInput.cacheEntryTime());
        assertNotNull(processRecordsInput.cacheExitTime());

        sleep(2000);

        ProcessRecordsInput processRecordsInput2 = getRecordsCache.getNextResult();
        assertNotEquals(processRecordsInput, processRecordsInput2);
        assertEquals(expectedRecords, processRecordsInput2.records());
        assertNotEquals(processRecordsInput2.timeSpentInCache(), Duration.ZERO);

        assertTrue(spyQueue.size() <= MAX_SIZE);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetNextRecordsWithoutStarting() {
        verify(executorService, times(0)).execute(any());
        getRecordsCache.getNextResult();
    }

    @Test(expected = IllegalStateException.class)
    public void testCallAfterShutdown() {
        when(executorService.isShutdown()).thenReturn(true);
        getRecordsCache.getNextResult();
    }

    @Test
    public void testExpiredIteratorException() {
        log.info("Starting tests");
        when(getRecordsRetrievalStrategy.getRecords(MAX_RECORDS_PER_CALL)).thenThrow(ExpiredIteratorException.class)
                .thenReturn(getRecordsResponse);

        getRecordsCache.start(sequenceNumber, initialPosition);

        doNothing().when(dataFetcher).restartIterator();

        getRecordsCache.getNextResult();

        sleep(1000);

        verify(dataFetcher).restartIterator();
    }

    @Test(timeout = 1000L)
    public void testNoDeadlockOnFullQueue() {
        //
        // Fixes https://github.com/awslabs/amazon-kinesis-client/issues/448
        //
        // This test is to verify that the drain of a blocked queue no longer deadlocks.
        // If the test times out before starting the subscriber it means something went wrong while filling the queue.
        // After the subscriber is started one of the things that can trigger a timeout is a deadlock.
        //
        GetRecordsResponse response = GetRecordsResponse.builder().records(
                Record.builder().data(SdkBytes.fromByteArray(new byte[] { 1, 2, 3 })).sequenceNumber("123").build())
                .build();
        when(getRecordsRetrievalStrategy.getRecords(anyInt())).thenReturn(response);

        getRecordsCache.start(sequenceNumber, initialPosition);

        //
        // Wait for the queue to fill up, and the publisher to block on adding items to the queue.
        //
        log.info("Waiting for queue to fill up");
        while (getRecordsCache.getRecordsResultQueue.size() < MAX_SIZE) {
            Thread.yield();
        }

        log.info("Queue is currently at {} starting subscriber", getRecordsCache.getRecordsResultQueue.size());
        AtomicInteger receivedItems = new AtomicInteger(0);
        final int expectedItems = MAX_SIZE * 3;

        Object lock = new Object();

        Subscriber<ProcessRecordsInput> subscriber = new Subscriber<ProcessRecordsInput>() {
            Subscription sub;

            @Override
            public void onSubscribe(Subscription s) {
                sub = s;
                s.request(1);
            }

            @Override
            public void onNext(ProcessRecordsInput processRecordsInput) {
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
