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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

/**
 * Unit tests for KinesisDataFetcher.
 */
@RunWith(MockitoJUnitRunner.class)
public class KinesisDataFetcherTest {

    @Mock
    private KinesisProxy kinesisProxy;

    private static final int MAX_RECORDS = 1;
    private static final String SHARD_ID = "shardId-1";
    private static final String AT_SEQUENCE_NUMBER = ShardIteratorType.AT_SEQUENCE_NUMBER.toString();
    private static final ShardInfo SHARD_INFO = new ShardInfo(SHARD_ID, null, null, null);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_AT_TIMESTAMP =
            InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(1000));

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        MetricsHelper.startScope(new NullMetricsFactory(), "KinesisDataFetcherTest");
    }

    /**
     * Test initialize() with the LATEST iterator instruction
     */
    @Test
    public final void testInitializeLatest() throws Exception {
        testInitializeAndFetch(ShardIteratorType.LATEST.toString(),
                ShardIteratorType.LATEST.toString(),
                INITIAL_POSITION_LATEST);
    }

    /**
     * Test initialize() with the TIME_ZERO iterator instruction
     */
    @Test
    public final void testInitializeTimeZero() throws Exception {
        testInitializeAndFetch(ShardIteratorType.TRIM_HORIZON.toString(),
                ShardIteratorType.TRIM_HORIZON.toString(),
                INITIAL_POSITION_TRIM_HORIZON);
    }

    /**
     * Test initialize() with the AT_TIMESTAMP iterator instruction
     */
    @Test
    public final void testInitializeAtTimestamp() throws Exception {
        testInitializeAndFetch(ShardIteratorType.AT_TIMESTAMP.toString(),
                ShardIteratorType.AT_TIMESTAMP.toString(),
                INITIAL_POSITION_AT_TIMESTAMP);
    }


    /**
     * Test initialize() when a flushpoint exists.
     */
    @Test
    public final void testInitializeFlushpoint() throws Exception {
        testInitializeAndFetch("foo", "123", INITIAL_POSITION_LATEST);
    }

    /**
     * Test initialize() with an invalid iterator instruction
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testInitializeInvalid() throws Exception {
        testInitializeAndFetch("foo", null, INITIAL_POSITION_LATEST);
    }

    @Test
    public void testadvanceIteratorTo() throws KinesisClientLibException {
        IKinesisProxy kinesis = mock(IKinesisProxy.class);
        ICheckpoint checkpoint = mock(ICheckpoint.class);

        KinesisDataFetcher fetcher = new KinesisDataFetcher(kinesis, SHARD_INFO);
        GetRecordsRetrievalStrategy getRecordsRetrievalStrategy = new SynchronousGetRecordsRetrievalStrategy(fetcher);

        String iteratorA = "foo";
        String iteratorB = "bar";
        String seqA = "123";
        String seqB = "456";
        GetRecordsResult outputA = new GetRecordsResult();
        List<Record> recordsA = new ArrayList<Record>();
        outputA.setRecords(recordsA);
        GetRecordsResult outputB = new GetRecordsResult();
        List<Record> recordsB = new ArrayList<Record>();
        outputB.setRecords(recordsB);

        when(kinesis.getIterator(SHARD_ID, AT_SEQUENCE_NUMBER, seqA)).thenReturn(iteratorA);
        when(kinesis.getIterator(SHARD_ID, AT_SEQUENCE_NUMBER, seqB)).thenReturn(iteratorB);
        when(kinesis.get(iteratorA, MAX_RECORDS)).thenReturn(outputA);
        when(kinesis.get(iteratorB, MAX_RECORDS)).thenReturn(outputB);

        when(checkpoint.getCheckpoint(SHARD_ID)).thenReturn(new ExtendedSequenceNumber(seqA));
        fetcher.initialize(seqA, null);

        fetcher.advanceIteratorTo(seqA, null);
        Assert.assertEquals(recordsA, getRecordsRetrievalStrategy.getRecords(MAX_RECORDS).getRecords());

        fetcher.advanceIteratorTo(seqB, null);
        Assert.assertEquals(recordsB, getRecordsRetrievalStrategy.getRecords(MAX_RECORDS).getRecords());
    }

    @Test
    public void testadvanceIteratorToTrimHorizonLatestAndAtTimestamp() {
        IKinesisProxy kinesis = mock(IKinesisProxy.class);

        KinesisDataFetcher fetcher = new KinesisDataFetcher(kinesis, SHARD_INFO);

        String iteratorHorizon = "horizon";
        when(kinesis.getIterator(SHARD_ID, ShardIteratorType.TRIM_HORIZON.toString())).thenReturn(iteratorHorizon);
        fetcher.advanceIteratorTo(ShardIteratorType.TRIM_HORIZON.toString(), INITIAL_POSITION_TRIM_HORIZON);
        Assert.assertEquals(iteratorHorizon, fetcher.getNextIterator());

        String iteratorLatest = "latest";
        when(kinesis.getIterator(SHARD_ID, ShardIteratorType.LATEST.toString())).thenReturn(iteratorLatest);
        fetcher.advanceIteratorTo(ShardIteratorType.LATEST.toString(), INITIAL_POSITION_LATEST);
        Assert.assertEquals(iteratorLatest, fetcher.getNextIterator());

        Date timestamp  = new Date(1000L);
        String iteratorAtTimestamp = "AT_TIMESTAMP";
        when(kinesis.getIterator(SHARD_ID, timestamp)).thenReturn(iteratorAtTimestamp);
        fetcher.advanceIteratorTo(ShardIteratorType.AT_TIMESTAMP.toString(), INITIAL_POSITION_AT_TIMESTAMP);
        Assert.assertEquals(iteratorAtTimestamp, fetcher.getNextIterator());
    }

    @Test
    public void testGetRecordsWithResourceNotFoundException() {
        // Set up arguments used by proxy
        String nextIterator = "TestShardIterator";
        int maxRecords = 100;

        // Set up proxy mock methods
        KinesisProxy mockProxy = mock(KinesisProxy.class);
        doReturn(nextIterator).when(mockProxy).getIterator(SHARD_ID, ShardIteratorType.LATEST.toString());
        doThrow(new ResourceNotFoundException("Test Exception")).when(mockProxy).get(nextIterator, maxRecords);

        // Create data fectcher and initialize it with latest type checkpoint
        KinesisDataFetcher dataFetcher = new KinesisDataFetcher(mockProxy, SHARD_INFO);
        dataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);
        GetRecordsRetrievalStrategy getRecordsRetrievalStrategy = new SynchronousGetRecordsRetrievalStrategy(dataFetcher);
        // Call getRecords of dataFetcher which will throw an exception
        getRecordsRetrievalStrategy.getRecords(maxRecords);

        // Test shard has reached the end
        Assert.assertTrue("Shard should reach the end", dataFetcher.isShardEndReached());
    }
    
    @Test
    public void testNonNullGetRecords() {
        String nextIterator = "TestIterator";
        int maxRecords = 100;
        
        KinesisProxy mockProxy = mock(KinesisProxy.class);
        doThrow(new ResourceNotFoundException("Test Exception")).when(mockProxy).get(nextIterator, maxRecords);

        KinesisDataFetcher dataFetcher = new KinesisDataFetcher(mockProxy, SHARD_INFO);
        dataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);
        
        DataFetcherResult dataFetcherResult = dataFetcher.getRecords(maxRecords);
        
        assertThat(dataFetcherResult, notNullValue());
    }

    @Test
    public void testFetcherDoesNotAdvanceWithoutAccept() {
        final String INITIAL_ITERATOR = "InitialIterator";
        final String NEXT_ITERATOR_ONE = "NextIteratorOne";
        final String NEXT_ITERATOR_TWO = "NextIteratorTwo";
        when(kinesisProxy.getIterator(anyString(), anyString())).thenReturn(INITIAL_ITERATOR);
        GetRecordsResult iteratorOneResults = mock(GetRecordsResult.class);
        when(iteratorOneResults.getNextShardIterator()).thenReturn(NEXT_ITERATOR_ONE);
        when(kinesisProxy.get(eq(INITIAL_ITERATOR), anyInt())).thenReturn(iteratorOneResults);

        GetRecordsResult iteratorTwoResults = mock(GetRecordsResult.class);
        when(kinesisProxy.get(eq(NEXT_ITERATOR_ONE), anyInt())).thenReturn(iteratorTwoResults);
        when(iteratorTwoResults.getNextShardIterator()).thenReturn(NEXT_ITERATOR_TWO);

        GetRecordsResult finalResult = mock(GetRecordsResult.class);
        when(kinesisProxy.get(eq(NEXT_ITERATOR_TWO), anyInt())).thenReturn(finalResult);
        when(finalResult.getNextShardIterator()).thenReturn(null);

        KinesisDataFetcher dataFetcher = new KinesisDataFetcher(kinesisProxy, SHARD_INFO);
        dataFetcher.initialize("TRIM_HORIZON",
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));

        assertNoAdvance(dataFetcher, iteratorOneResults, INITIAL_ITERATOR);
        assertAdvanced(dataFetcher, iteratorOneResults, INITIAL_ITERATOR, NEXT_ITERATOR_ONE);

        assertNoAdvance(dataFetcher, iteratorTwoResults, NEXT_ITERATOR_ONE);
        assertAdvanced(dataFetcher, iteratorTwoResults, NEXT_ITERATOR_ONE, NEXT_ITERATOR_TWO);

        assertNoAdvance(dataFetcher, finalResult, NEXT_ITERATOR_TWO);
        assertAdvanced(dataFetcher, finalResult, NEXT_ITERATOR_TWO, null);

        verify(kinesisProxy, times(2)).get(eq(INITIAL_ITERATOR), anyInt());
        verify(kinesisProxy, times(2)).get(eq(NEXT_ITERATOR_ONE), anyInt());
        verify(kinesisProxy, times(2)).get(eq(NEXT_ITERATOR_TWO), anyInt());

        reset(kinesisProxy);

        DataFetcherResult terminal = dataFetcher.getRecords(100);
        assertThat(terminal.isShardEnd(), equalTo(true));
        assertThat(terminal.getResult(), notNullValue());
        GetRecordsResult terminalResult = terminal.getResult();
        assertThat(terminalResult.getRecords(), notNullValue());
        assertThat(terminalResult.getRecords(), empty());
        assertThat(terminalResult.getNextShardIterator(), nullValue());
        assertThat(terminal, equalTo(dataFetcher.TERMINAL_RESULT));

        verify(kinesisProxy, never()).get(anyString(), anyInt());
    }

    private DataFetcherResult assertAdvanced(KinesisDataFetcher dataFetcher, GetRecordsResult expectedResult,
            String previousValue, String nextValue) {
        DataFetcherResult acceptResult = dataFetcher.getRecords(100);
        assertThat(acceptResult.getResult(), equalTo(expectedResult));

        assertThat(dataFetcher.getNextIterator(), equalTo(previousValue));
        assertThat(dataFetcher.isShardEndReached(), equalTo(false));

        assertThat(acceptResult.accept(), equalTo(expectedResult));
        assertThat(dataFetcher.getNextIterator(), equalTo(nextValue));
        if (nextValue == null) {
            assertThat(dataFetcher.isShardEndReached(), equalTo(true));
        }

        verify(kinesisProxy, times(2)).get(eq(previousValue), anyInt());

        return acceptResult;
    }

    private DataFetcherResult assertNoAdvance(KinesisDataFetcher dataFetcher, GetRecordsResult expectedResult,
            String previousValue) {
        assertThat(dataFetcher.getNextIterator(), equalTo(previousValue));
        DataFetcherResult noAcceptResult = dataFetcher.getRecords(100);
        assertThat(noAcceptResult.getResult(), equalTo(expectedResult));

        assertThat(dataFetcher.getNextIterator(), equalTo(previousValue));

        verify(kinesisProxy).get(eq(previousValue), anyInt());

        return noAcceptResult;
    }

    private void testInitializeAndFetch(String iteratorType,
            String seqNo,
            InitialPositionInStreamExtended initialPositionInStream) throws Exception {
        IKinesisProxy kinesis = mock(IKinesisProxy.class);
        String iterator = "foo";
        List<Record> expectedRecords = new ArrayList<Record>();
        GetRecordsResult response = new GetRecordsResult();
        response.setRecords(expectedRecords);

        when(kinesis.getIterator(SHARD_ID, initialPositionInStream.getTimestamp())).thenReturn(iterator);
        when(kinesis.getIterator(SHARD_ID, AT_SEQUENCE_NUMBER, seqNo)).thenReturn(iterator);
        when(kinesis.getIterator(SHARD_ID, iteratorType)).thenReturn(iterator);
        when(kinesis.get(iterator, MAX_RECORDS)).thenReturn(response);

        ICheckpoint checkpoint = mock(ICheckpoint.class);
        when(checkpoint.getCheckpoint(SHARD_ID)).thenReturn(new ExtendedSequenceNumber(seqNo));

        KinesisDataFetcher fetcher = new KinesisDataFetcher(kinesis, SHARD_INFO);
        GetRecordsRetrievalStrategy getRecordsRetrievalStrategy = new SynchronousGetRecordsRetrievalStrategy(fetcher);
        fetcher.initialize(seqNo, initialPositionInStream);
        List<Record> actualRecords = getRecordsRetrievalStrategy.getRecords(MAX_RECORDS).getRecords();

        Assert.assertEquals(expectedRecords, actualRecords);
    }

}
