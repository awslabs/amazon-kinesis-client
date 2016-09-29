/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;

/**
 * Unit tests for KinesisDataFetcher.
 */
public class KinesisDataFetcherTest {

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
        Assert.assertEquals(recordsA, fetcher.getRecords(MAX_RECORDS).getRecords());

        fetcher.advanceIteratorTo(seqB, null);
        Assert.assertEquals(recordsB, fetcher.getRecords(MAX_RECORDS).getRecords());
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
        // Call getRecords of dataFetcher which will throw an exception
        dataFetcher.getRecords(maxRecords);

        // Test shard has reached the end
        Assert.assertTrue("Shard should reach the end", dataFetcher.isShardEndReached());
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
        fetcher.initialize(seqNo, initialPositionInStream);
        List<Record> actualRecords = fetcher.getRecords(MAX_RECORDS).getRecords();

        Assert.assertEquals(expectedRecords, actualRecords);
    }

}
