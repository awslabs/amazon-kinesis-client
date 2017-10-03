/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License. 
 */

package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Test class for the PrefetchGetRecordsCache class.
 */
@RunWith(MockitoJUnitRunner.class)
public class PrefetchGetRecordsCacheTest {
    private static final int SIZE_512_KB = 512 * 1024;
    private static final int SIZE_1_MB = 2 * SIZE_512_KB;
    private static final int MAX_RECORDS_PER_CALL = 10000;
    private static final int MAX_SIZE = 5;
    private static final int MAX_RECORDS_COUNT = 15000;
    private static final long IDLE_MILLIS_BETWEEN_CALLS = 0L;

    @Mock
    private GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    @Mock
    private GetRecordsResult getRecordsResult;
    @Mock
    private Record record;

    private List<Record> records;
    private ExecutorService executorService;
    private LinkedBlockingQueue<ProcessRecordsInput> spyQueue;
    private PrefetchGetRecordsCache getRecordsCache;

    @Before
    public void setup() {
        executorService = spy(Executors.newFixedThreadPool(1));
        getRecordsCache = new PrefetchGetRecordsCache(
                MAX_SIZE,
                3 * SIZE_1_MB,
                MAX_RECORDS_COUNT,
                MAX_RECORDS_PER_CALL,
                getRecordsRetrievalStrategy,
                executorService,
                IDLE_MILLIS_BETWEEN_CALLS);
        spyQueue = spy(getRecordsCache.getRecordsResultQueue);
        records = spy(new ArrayList<>());

        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_CALL))).thenReturn(getRecordsResult);
        when(getRecordsResult.getRecords()).thenReturn(records);
    }

    @Test
    public void testGetRecords() {
        when(records.size()).thenReturn(1000);
        when(record.getData()).thenReturn(createByteBufferWithSize(SIZE_512_KB));

        records.add(record);
        records.add(record);
        records.add(record);
        records.add(record);
        records.add(record);

        getRecordsCache.start();
        ProcessRecordsInput result = getRecordsCache.getNextResult();

        assertEquals(result.getRecords(), records);

        verify(executorService).execute(any());
        verify(getRecordsRetrievalStrategy, atLeast(1)).getRecords(eq(MAX_RECORDS_PER_CALL));
    }

    @Test
    public void testFullCacheByteSize() {
        when(records.size()).thenReturn(500);
        when(record.getData()).thenReturn(createByteBufferWithSize(SIZE_1_MB));

        records.add(record);

        getRecordsCache.start();

        // Sleep for a few seconds for the cache to fill up.
        sleep(2000);

        verify(getRecordsRetrievalStrategy, times(3)).getRecords(eq(MAX_RECORDS_PER_CALL));
        assertEquals(spyQueue.size(), 3);
    }

    @Test
    public void testFullCacheRecordsCount() {
        int recordsSize = 4500;
        when(records.size()).thenReturn(recordsSize);

        getRecordsCache.start();

        sleep(2000);

        int callRate = (int) Math.ceil((double) MAX_RECORDS_COUNT/recordsSize);
        verify(getRecordsRetrievalStrategy, times(callRate)).getRecords(MAX_RECORDS_PER_CALL);
        assertEquals(spyQueue.size(), callRate);
        assertTrue(callRate < MAX_SIZE);
    }

    @Test
    public void testFullCacheSize() {
        int recordsSize = 200;
        when(records.size()).thenReturn(recordsSize);

        getRecordsCache.start();

        // Sleep for a few seconds for the cache to fill up.
        sleep(2000);

        verify(getRecordsRetrievalStrategy, times(MAX_SIZE + 1)).getRecords(eq(MAX_RECORDS_PER_CALL));
        assertEquals(spyQueue.size(), MAX_SIZE);
    }

    @Test
    public void testMultipleCacheCalls() {
        int recordsSize = 20;
        when(record.getData()).thenReturn(createByteBufferWithSize(1024));

        IntStream.range(0, recordsSize).forEach(i -> records.add(record));

        getRecordsCache.start();
        ProcessRecordsInput processRecordsInput = getRecordsCache.getNextResult();

        verify(executorService).execute(any());
        assertEquals(processRecordsInput.getRecords(), records);
        assertNotNull(processRecordsInput.getCacheEntryTime());
        assertNotNull(processRecordsInput.getCacheExitTime());

        sleep(2000);

        ProcessRecordsInput processRecordsInput2 = getRecordsCache.getNextResult();
        assertNotEquals(processRecordsInput, processRecordsInput2);
        assertEquals(processRecordsInput2.getRecords(), records);
        assertNotEquals(processRecordsInput2.getTimeSpentInCache(), Duration.ZERO);

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

    private ByteBuffer createByteBufferWithSize(int size) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        byteBuffer.put(new byte[size]);
        return byteBuffer;
    }
}
