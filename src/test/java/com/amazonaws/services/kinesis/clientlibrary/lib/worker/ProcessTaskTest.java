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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.Messages.AggregatedRecord;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.google.protobuf.ByteString;

public class ProcessTaskTest {

    @SuppressWarnings("serial")
    private static class RecordSubclass extends Record {}

    private static final byte[] TEST_DATA = new byte[] { 1, 2, 3, 4 };

    private final int maxRecords = 100;
    private final String shardId = "shard-test";
    private final long idleTimeMillis = 1000L;
    private final long taskBackoffTimeMillis = 1L;
    private final boolean callProcessRecordsForEmptyRecordList = true;
    // We don't want any of these tests to run checkpoint validation
    private final boolean skipCheckpointValidationValue = false;
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

    private @Mock KinesisDataFetcher mockDataFetcher;
    private @Mock IRecordProcessor mockRecordProcessor;
    private @Mock RecordProcessorCheckpointer mockCheckpointer;
    @Mock
    private ThrottlingReporter throttlingReporter;
    @Mock
    private GetRecordsCache getRecordsCache;

    private List<Record> processedRecords;
    private ExtendedSequenceNumber newLargestPermittedCheckpointValue;

    private ProcessTask processTask;

    @Before
    public void setUpProcessTask() {
        // Initialize the annotation
        MockitoAnnotations.initMocks(this);
        // Set up process task
        final StreamConfig config =
                new StreamConfig(null, maxRecords, idleTimeMillis, callProcessRecordsForEmptyRecordList,
                        skipCheckpointValidationValue,
                        INITIAL_POSITION_LATEST);
        final ShardInfo shardInfo = new ShardInfo(shardId, null, null, null);
        processTask = new ProcessTask(
                shardInfo,
                config,
                mockRecordProcessor,
                mockCheckpointer,
                mockDataFetcher,
                taskBackoffTimeMillis,
                KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                throttlingReporter,
                getRecordsCache);
    }

    @Test
    public void testProcessTaskWithProvisionedThroughputExceededException() {
        // Set data fetcher to throw exception
        doReturn(false).when(mockDataFetcher).isShardEndReached();
        doThrow(new ProvisionedThroughputExceededException("Test Exception")).when(getRecordsCache)
                .getNextResult();

        TaskResult result = processTask.call();
        verify(throttlingReporter).throttled();
        verify(throttlingReporter, never()).success();
        verify(getRecordsCache).getNextResult();
        assertTrue("Result should contain ProvisionedThroughputExceededException",
                result.getException() instanceof ProvisionedThroughputExceededException);
    }

    @Test
    public void testProcessTaskWithNonExistentStream() {
        // Data fetcher returns a null Result ` the stream does not exist
        doReturn(new ProcessRecordsInput().withRecords(Collections.emptyList()).withMillisBehindLatest((long) 0)).when(getRecordsCache).getNextResult();

        TaskResult result = processTask.call();
        verify(getRecordsCache).getNextResult();
        assertNull("Task should not throw an exception", result.getException());
    }

    @Test
    public void testProcessTaskWithShardEndReached() {
        // Set data fetcher to return true for shard end reached
        doReturn(true).when(mockDataFetcher).isShardEndReached();

        TaskResult result = processTask.call();
        assertTrue("Result should contain shardEndReached true", result.isShardEndReached());
    }

    @Test
    public void testNonAggregatedKinesisRecord() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final String pk = UUID.randomUUID().toString();
        final Date ts = new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(4, TimeUnit.HOURS));
        final Record r = new Record()
                .withPartitionKey(pk)
                .withData(ByteBuffer.wrap(TEST_DATA))
                .withSequenceNumber(sqn)
                .withApproximateArrivalTimestamp(ts);

        testWithRecord(r);

        assertEquals(1, processedRecords.size());

        Record pr = processedRecords.get(0);
        assertEquals(pk, pr.getPartitionKey());
        assertEquals(ts, pr.getApproximateArrivalTimestamp());
        byte[] b = new byte[pr.getData().remaining()];
        pr.getData().get(b);
        assertTrue(Arrays.equals(TEST_DATA, b));

        assertEquals(sqn, newLargestPermittedCheckpointValue.getSequenceNumber());
        assertEquals(0, newLargestPermittedCheckpointValue.getSubSequenceNumber());
    }

    @Test
    public void testDoesNotDeaggregateSubclassOfRecord() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final Record r = new RecordSubclass()
                .withSequenceNumber(sqn)
                .withData(ByteBuffer.wrap(new byte[0]));

        testWithRecord(r);

        assertEquals(1, processedRecords.size(), 1);
        assertSame(r, processedRecords.get(0));

        assertEquals(sqn, newLargestPermittedCheckpointValue.getSequenceNumber());
        assertEquals(0, newLargestPermittedCheckpointValue.getSubSequenceNumber());
    }

    @Test
    public void testDeaggregatesRecord() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final String pk = UUID.randomUUID().toString();
        final Date ts = new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(4, TimeUnit.HOURS));
        final Record r = new Record()
                .withPartitionKey("-")
                .withData(generateAggregatedRecord(pk))
                .withSequenceNumber(sqn)
                .withApproximateArrivalTimestamp(ts);

        testWithRecord(r);

        assertEquals(3, processedRecords.size());
        for (Record pr : processedRecords) {
            assertTrue(pr instanceof UserRecord);
            assertEquals(pk, pr.getPartitionKey());
            assertEquals(ts, pr.getApproximateArrivalTimestamp());
            byte[] b = new byte[pr.getData().remaining()];
            pr.getData().get(b);
            assertTrue(Arrays.equals(TEST_DATA, b));
        }

        assertEquals(sqn, newLargestPermittedCheckpointValue.getSequenceNumber());
        assertEquals(processedRecords.size() - 1, newLargestPermittedCheckpointValue.getSubSequenceNumber());
    }

    @Test
    public void testDeaggregatesRecordWithNoArrivalTimestamp() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final String pk = UUID.randomUUID().toString();
        final Record r = new Record()
                .withPartitionKey("-")
                .withData(generateAggregatedRecord(pk))
                .withSequenceNumber(sqn);

        testWithRecord(r);

        assertEquals(3, processedRecords.size());
        for (Record pr : processedRecords) {
            assertTrue(pr instanceof UserRecord);
            assertEquals(pk, pr.getPartitionKey());
            assertNull(pr.getApproximateArrivalTimestamp());
        }
    }

    @Test
    public void testLargestPermittedCheckpointValue() {
        // Some sequence number value from previous processRecords call to mock.
        final BigInteger previousCheckpointSqn = new BigInteger(128, new Random());

        // Values for this processRecords call.
        final int numberOfRecords = 104;
        // Start these batch of records's sequence number that is greater than previous checkpoint value.
        final BigInteger startingSqn = previousCheckpointSqn.add(BigInteger.valueOf(10));
        final List<Record> records = generateConsecutiveRecords(
                numberOfRecords, "-", ByteBuffer.wrap(TEST_DATA), new Date(), startingSqn);

        testWithRecords(records, new ExtendedSequenceNumber(previousCheckpointSqn.toString()),
                new ExtendedSequenceNumber(previousCheckpointSqn.toString()));

        final ExtendedSequenceNumber expectedLargestPermittedEsqn = new ExtendedSequenceNumber(
                startingSqn.add(BigInteger.valueOf(numberOfRecords - 1)).toString());
        assertEquals(expectedLargestPermittedEsqn, newLargestPermittedCheckpointValue);
    }

    @Test
    public void testLargestPermittedCheckpointValueWithEmptyRecords() {
        // Some sequence number value from previous processRecords call.
        final BigInteger baseSqn = new BigInteger(128, new Random());
        final ExtendedSequenceNumber lastCheckpointEspn = new ExtendedSequenceNumber(baseSqn.toString());
        final ExtendedSequenceNumber largestPermittedEsqn = new ExtendedSequenceNumber(
                baseSqn.add(BigInteger.valueOf(100)).toString());

        testWithRecords(Collections.<Record>emptyList(), lastCheckpointEspn, largestPermittedEsqn);

        // Make sure that even with empty records, largest permitted sequence number does not change.
        assertEquals(largestPermittedEsqn, newLargestPermittedCheckpointValue);
    }

    @Test
    public void testFilterBasedOnLastCheckpointValue() {
        // Explanation of setup:
        // * Assume in previous processRecord call, user got 3 sub-records that all belonged to one
        //   Kinesis record. So sequence number was X, and sub-sequence numbers were 0, 1, 2.
        // * 2nd sub-record was checkpointed (extended sequnce number X.1).
        // * Worker crashed and restarted. So now DDB has checkpoint value of X.1.
        // Test:
        // * Now in the subsequent processRecords call, KCL should filter out X.0 and X.1.
        final BigInteger previousCheckpointSqn = new BigInteger(128, new Random());
        final long previousCheckpointSsqn = 1;

        // Values for this processRecords call.
        final String startingSqn = previousCheckpointSqn.toString();
        final String pk = UUID.randomUUID().toString();
        final Record r = new Record()
                .withPartitionKey("-")
                .withData(generateAggregatedRecord(pk))
                .withSequenceNumber(startingSqn);

        testWithRecords(Collections.singletonList(r),
                new ExtendedSequenceNumber(previousCheckpointSqn.toString(), previousCheckpointSsqn),
                new ExtendedSequenceNumber(previousCheckpointSqn.toString(), previousCheckpointSsqn));

        // First two records should be dropped - and only 1 remaining records should be there.
        assertEquals(1, processedRecords.size());
        assertTrue(processedRecords.get(0) instanceof UserRecord);

        // Verify user record's extended sequence number and other fields.
        final UserRecord pr = (UserRecord)processedRecords.get(0);
        assertEquals(pk, pr.getPartitionKey());
        assertEquals(startingSqn, pr.getSequenceNumber());
        assertEquals(previousCheckpointSsqn + 1, pr.getSubSequenceNumber());
        assertNull(pr.getApproximateArrivalTimestamp());

        // Expected largest permitted sequence number will be last sub-record sequence number.
        final ExtendedSequenceNumber expectedLargestPermittedEsqn = new ExtendedSequenceNumber(
                previousCheckpointSqn.toString(), 2L);
        assertEquals(expectedLargestPermittedEsqn, newLargestPermittedCheckpointValue);
    }

    private void testWithRecord(Record record) {
        testWithRecords(Collections.singletonList(record),
                ExtendedSequenceNumber.TRIM_HORIZON, ExtendedSequenceNumber.TRIM_HORIZON);
    }

    private void testWithRecords(List<Record> records,
            ExtendedSequenceNumber lastCheckpointValue,
            ExtendedSequenceNumber largestPermittedCheckpointValue) {
        when(getRecordsCache.getNextResult()).thenReturn(new ProcessRecordsInput().withRecords(records).withMillisBehindLatest((long) 1000 * 50));
        when(mockCheckpointer.getLastCheckpointValue()).thenReturn(lastCheckpointValue);
        when(mockCheckpointer.getLargestPermittedCheckpointValue()).thenReturn(largestPermittedCheckpointValue);
        processTask.call();
        verify(throttlingReporter).success();
        verify(throttlingReporter, never()).throttled();
        verify(getRecordsCache).getNextResult();
        ArgumentCaptor<ProcessRecordsInput> priCaptor = ArgumentCaptor.forClass(ProcessRecordsInput.class);
        verify(mockRecordProcessor).processRecords(priCaptor.capture());
        processedRecords = priCaptor.getValue().getRecords();

        ArgumentCaptor<ExtendedSequenceNumber> esnCaptor = ArgumentCaptor.forClass(ExtendedSequenceNumber.class);
        verify(mockCheckpointer).setLargestPermittedCheckpointValue(esnCaptor.capture());
        newLargestPermittedCheckpointValue = esnCaptor.getValue();
    }

    /**
     * See the KPL documentation on GitHub for more details about the binary
     * format.
     * 
     * @param pk
     *            Partition key to use. All the records will have the same
     *            partition key.
     * @return ByteBuffer containing the serialized form of the aggregated
     *         record, along with the necessary header and footer.
     */
    private static ByteBuffer generateAggregatedRecord(String pk) {
        ByteBuffer bb = ByteBuffer.allocate(1024);
        bb.put(new byte[] {-13, -119, -102, -62 });

        com.amazonaws.services.kinesis.clientlibrary.types.Messages.Record r =
                com.amazonaws.services.kinesis.clientlibrary.types.Messages.Record.newBuilder()
                        .setData(ByteString.copyFrom(TEST_DATA))
                        .setPartitionKeyIndex(0)
                        .build();

        byte[] payload = AggregatedRecord.newBuilder()
            .addPartitionKeyTable(pk)
            .addRecords(r)
            .addRecords(r)
            .addRecords(r)
            .build()
            .toByteArray();

        bb.put(payload);
        bb.put(md5(payload));
        bb.limit(bb.position());
        bb.rewind();
        return bb;
    }

    private static List<Record> generateConsecutiveRecords(
            int numberOfRecords, String partitionKey, ByteBuffer data,
            Date arrivalTimestamp, BigInteger startSequenceNumber) {
        List<Record> records = new ArrayList<>();
        for (int i = 0 ; i < numberOfRecords ; ++i) {
            records.add(new Record()
                .withPartitionKey(partitionKey)
                .withData(data)
                .withSequenceNumber(startSequenceNumber.add(BigInteger.valueOf(i)).toString())
                .withApproximateArrivalTimestamp(arrivalTimestamp));
        }
        return records;
    }

    private static byte[] md5(byte[] b) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return md.digest(b);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
