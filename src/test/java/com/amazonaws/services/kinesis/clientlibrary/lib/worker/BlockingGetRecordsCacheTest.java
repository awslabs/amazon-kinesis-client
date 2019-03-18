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

package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Test class for the BlockingGetRecordsCache class.
 */
@RunWith(MockitoJUnitRunner.class)
public class BlockingGetRecordsCacheTest {
    private static final int MAX_RECORDS_PER_COUNT = 10_000;

    @Mock
    private GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    @Mock
    private GetRecordsResult getRecordsResult;
    
    private List<Record> records;
    private BlockingGetRecordsCache blockingGetRecordsCache;

    @Before
    public void setup() {
        records = new ArrayList<>();
        blockingGetRecordsCache = new BlockingGetRecordsCache(MAX_RECORDS_PER_COUNT, getRecordsRetrievalStrategy);

        when(getRecordsRetrievalStrategy.getRecords(eq(MAX_RECORDS_PER_COUNT))).thenReturn(getRecordsResult);
        when(getRecordsResult.getRecords()).thenReturn(records);
    }

    @Test
    public void testGetNextRecordsWithNoRecords() {
        ProcessRecordsInput result = blockingGetRecordsCache.getNextResult();

        assertEquals(result.getRecords(), records);
        assertNull(result.getCacheEntryTime());
        assertNull(result.getCacheExitTime());
        assertEquals(result.getTimeSpentInCache(), Duration.ZERO);
    }
    
    @Test
    public void testGetNextRecordsWithRecords() {
        Record record = new Record();
        records.add(record);
        records.add(record);
        records.add(record);
        records.add(record);
        
        ProcessRecordsInput result = blockingGetRecordsCache.getNextResult();
        
        assertEquals(result.getRecords(), records);
    }
}
