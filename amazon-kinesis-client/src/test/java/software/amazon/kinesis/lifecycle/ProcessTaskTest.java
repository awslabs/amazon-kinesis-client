/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
 * http://aws.amazon.com/asl/ or in the "license" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package software.amazon.kinesis.lifecycle;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.model.Record;
import com.google.protobuf.ByteString;

import lombok.Data;
import software.amazon.kinesis.checkpoint.RecordProcessorCheckpointer;
import software.amazon.kinesis.leases.LeaseManagerProxy;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.processor.RecordProcessor;
import software.amazon.kinesis.retrieval.ThrottlingReporter;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.retrieval.kpl.Messages;
import software.amazon.kinesis.retrieval.kpl.Messages.AggregatedRecord;
import software.amazon.kinesis.retrieval.kpl.UserRecord;

@RunWith(MockitoJUnitRunner.class)
public class ProcessTaskTest {
    private static final long IDLE_TIME_IN_MILLISECONDS = 100L;

    private boolean shouldCallProcessRecordsEvenForEmptyRecordList = true;
    private final boolean skipShardSyncAtWorkerInitializationIfLeasesExist = true;
    private ShardInfo shardInfo;

    @Mock
    private ProcessRecordsInput processRecordsInput;
    @Mock
    private LeaseManagerProxy leaseManagerProxy;

    @SuppressWarnings("serial")
    private static class RecordSubclass extends Record {
    }

    private static final byte[] TEST_DATA = new byte[] { 1, 2, 3, 4 };

    private final String shardId = "shard-test";
    private final long taskBackoffTimeMillis = 1L;

    @Mock
    private RecordProcessor recordProcessor;
    @Mock
    private RecordProcessorCheckpointer checkpointer;
    @Mock
    private ThrottlingReporter throttlingReporter;

    private ProcessTask processTask;

    @Before
    public void setUpProcessTask() {
        shardInfo = new ShardInfo(shardId, null, null, null);

    }

    private ProcessTask makeProcessTask(ProcessRecordsInput processRecordsInput) {
        return new ProcessTask(shardInfo, recordProcessor, checkpointer, taskBackoffTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist, leaseManagerProxy, throttlingReporter,
                processRecordsInput, shouldCallProcessRecordsEvenForEmptyRecordList, IDLE_TIME_IN_MILLISECONDS);
    }

    @Test
    public void testProcessTaskWithShardEndReached() {

        processTask = makeProcessTask(processRecordsInput);
        when(processRecordsInput.isAtShardEnd()).thenReturn(true);

        TaskResult result = processTask.call();
        assertThat(result, shardEndTaskResult(true));
    }

    @Test
    public void testNonAggregatedKinesisRecord() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final String pk = UUID.randomUUID().toString();
        final Date ts = new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(4, TimeUnit.HOURS));
        final Record r = new Record().withPartitionKey(pk).withData(ByteBuffer.wrap(TEST_DATA)).withSequenceNumber(sqn)
                .withApproximateArrivalTimestamp(ts);

        RecordProcessorOutcome outcome = testWithRecord(r);

        assertEquals(1, outcome.getProcessRecordsCall().getRecords().size());

        Record pr = outcome.getProcessRecordsCall().getRecords().get(0);
        assertEquals(pk, pr.getPartitionKey());
        assertEquals(ts, pr.getApproximateArrivalTimestamp());
        byte[] b = pr.getData().array();
        assertThat(b, equalTo(TEST_DATA));

        assertEquals(sqn, outcome.getCheckpointCall().getSequenceNumber());
        assertEquals(0, outcome.getCheckpointCall().getSubSequenceNumber());
    }

    @Data
    static class RecordProcessorOutcome {
        final ProcessRecordsInput processRecordsCall;
        final ExtendedSequenceNumber checkpointCall;
    }

    @Test
    public void testDoesNotDeaggregateSubclassOfRecord() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final Record r = new RecordSubclass().withSequenceNumber(sqn).withData(ByteBuffer.wrap(new byte[0]));

        processTask = makeProcessTask(processRecordsInput);
        RecordProcessorOutcome outcome = testWithRecord(r);

        assertEquals(1, outcome.getProcessRecordsCall().getRecords().size(), 1);
        assertSame(r, outcome.getProcessRecordsCall().getRecords().get(0));

        assertEquals(sqn, outcome.getCheckpointCall().getSequenceNumber());
        assertEquals(0, outcome.getCheckpointCall().getSubSequenceNumber());
    }

    @Test
    public void testDeaggregatesRecord() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final String pk = UUID.randomUUID().toString();
        final Date ts = new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(4, TimeUnit.HOURS));
        final Record r = new Record().withPartitionKey("-").withData(generateAggregatedRecord(pk))
                .withSequenceNumber(sqn).withApproximateArrivalTimestamp(ts);

        processTask = makeProcessTask(processRecordsInput);
        RecordProcessorOutcome outcome = testWithRecord(r);

        List<Record> actualRecords = outcome.getProcessRecordsCall().getRecords();

        assertEquals(3, actualRecords.size());
        for (Record pr : actualRecords) {
            assertThat(pr, instanceOf(UserRecord.class));
            assertEquals(pk, pr.getPartitionKey());
            assertEquals(ts, pr.getApproximateArrivalTimestamp());
            byte[] b = pr.getData().array();
            assertThat(b, equalTo(TEST_DATA));
        }

        assertEquals(sqn, outcome.getCheckpointCall().getSequenceNumber());
        assertEquals(actualRecords.size() - 1, outcome.getCheckpointCall().getSubSequenceNumber());
    }

    @Test
    public void testDeaggregatesRecordWithNoArrivalTimestamp() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final String pk = UUID.randomUUID().toString();
        final Record r = new Record().withPartitionKey("-").withData(generateAggregatedRecord(pk))
                .withSequenceNumber(sqn);

        processTask = makeProcessTask(processRecordsInput);
        RecordProcessorOutcome outcome = testWithRecord(r);

        List<Record> actualRecords = outcome.getProcessRecordsCall().getRecords();

        assertEquals(3, actualRecords.size());
        for (Record pr : actualRecords) {
            assertThat(pr, instanceOf(UserRecord.class));
            assertEquals(pk, pr.getPartitionKey());
            assertThat(pr.getApproximateArrivalTimestamp(), nullValue());
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
        final List<Record> records = generateConsecutiveRecords(numberOfRecords, "-", ByteBuffer.wrap(TEST_DATA),
                new Date(), startingSqn);

        processTask = makeProcessTask(processRecordsInput);
        RecordProcessorOutcome outcome = testWithRecords(records,
                new ExtendedSequenceNumber(previousCheckpointSqn.toString()),
                new ExtendedSequenceNumber(previousCheckpointSqn.toString()));

        final ExtendedSequenceNumber expectedLargestPermittedEsqn = new ExtendedSequenceNumber(
                startingSqn.add(BigInteger.valueOf(numberOfRecords - 1)).toString());
        assertEquals(expectedLargestPermittedEsqn, outcome.getCheckpointCall());
    }

    @Test
    public void testLargestPermittedCheckpointValueWithEmptyRecords() {
        // Some sequence number value from previous processRecords call.
        final BigInteger baseSqn = new BigInteger(128, new Random());
        final ExtendedSequenceNumber lastCheckpointEspn = new ExtendedSequenceNumber(baseSqn.toString());
        final ExtendedSequenceNumber largestPermittedEsqn = new ExtendedSequenceNumber(
                baseSqn.add(BigInteger.valueOf(100)).toString());

        processTask = makeProcessTask(processRecordsInput);
        RecordProcessorOutcome outcome = testWithRecords(Collections.emptyList(), lastCheckpointEspn,
                largestPermittedEsqn);

        // Make sure that even with empty records, largest permitted sequence number does not change.
        assertEquals(largestPermittedEsqn, outcome.getCheckpointCall());
    }

    @Test
    public void testFilterBasedOnLastCheckpointValue() {
        // Explanation of setup:
        // * Assume in previous processRecord call, user got 3 sub-records that all belonged to one
        // Kinesis record. So sequence number was X, and sub-sequence numbers were 0, 1, 2.
        // * 2nd sub-record was checkpointed (extended sequnce number X.1).
        // * Worker crashed and restarted. So now DDB has checkpoint value of X.1.
        // Test:
        // * Now in the subsequent processRecords call, KCL should filter out X.0 and X.1.
        final BigInteger previousCheckpointSqn = new BigInteger(128, new Random());
        final long previousCheckpointSsqn = 1;

        // Values for this processRecords call.
        final String startingSqn = previousCheckpointSqn.toString();
        final String pk = UUID.randomUUID().toString();
        final Record r = new Record().withPartitionKey("-").withData(generateAggregatedRecord(pk))
                .withSequenceNumber(startingSqn);

        processTask = makeProcessTask(processRecordsInput);
        RecordProcessorOutcome outcome = testWithRecords(Collections.singletonList(r),
                new ExtendedSequenceNumber(previousCheckpointSqn.toString(), previousCheckpointSsqn),
                new ExtendedSequenceNumber(previousCheckpointSqn.toString(), previousCheckpointSsqn));

        List<Record> actualRecords = outcome.getProcessRecordsCall().getRecords();

        // First two records should be dropped - and only 1 remaining records should be there.
        assertEquals(1, actualRecords.size());
        assertThat(actualRecords.get(0), instanceOf(UserRecord.class));

        // Verify user record's extended sequence number and other fields.
        final UserRecord pr = (UserRecord) actualRecords.get(0);
        assertEquals(pk, pr.getPartitionKey());
        assertEquals(startingSqn, pr.getSequenceNumber());
        assertEquals(previousCheckpointSsqn + 1, pr.getSubSequenceNumber());
        assertNull(pr.getApproximateArrivalTimestamp());

        // Expected largest permitted sequence number will be last sub-record sequence number.
        final ExtendedSequenceNumber expectedLargestPermittedEsqn = new ExtendedSequenceNumber(
                previousCheckpointSqn.toString(), 2L);
        assertEquals(expectedLargestPermittedEsqn, outcome.getCheckpointCall());
    }

    private RecordProcessorOutcome testWithRecord(Record record) {
        return testWithRecords(Collections.singletonList(record), ExtendedSequenceNumber.TRIM_HORIZON,
                ExtendedSequenceNumber.TRIM_HORIZON);
    }

    private RecordProcessorOutcome testWithRecords(List<Record> records, ExtendedSequenceNumber lastCheckpointValue,
            ExtendedSequenceNumber largestPermittedCheckpointValue) {
        when(checkpointer.lastCheckpointValue()).thenReturn(lastCheckpointValue);
        when(checkpointer.largestPermittedCheckpointValue()).thenReturn(largestPermittedCheckpointValue);
        when(processRecordsInput.getRecords()).thenReturn(records);
        processTask = makeProcessTask(processRecordsInput);
        processTask.call();
        verify(throttlingReporter).success();
        verify(throttlingReporter, never()).throttled();
        ArgumentCaptor<ProcessRecordsInput> recordsCaptor = ArgumentCaptor.forClass(ProcessRecordsInput.class);
        verify(recordProcessor).processRecords(recordsCaptor.capture());

        ArgumentCaptor<ExtendedSequenceNumber> esnCaptor = ArgumentCaptor.forClass(ExtendedSequenceNumber.class);
        verify(checkpointer).largestPermittedCheckpointValue(esnCaptor.capture());

        return new RecordProcessorOutcome(recordsCaptor.getValue(), esnCaptor.getValue());

    }

    /**
     * See the KPL documentation on GitHub for more details about the binary format.
     * 
     * @param pk
     *            Partition key to use. All the records will have the same partition key.
     * @return ByteBuffer containing the serialized form of the aggregated record, along with the necessary header and
     *         footer.
     */
    private static ByteBuffer generateAggregatedRecord(String pk) {
        ByteBuffer bb = ByteBuffer.allocate(1024);
        bb.put(new byte[] { -13, -119, -102, -62 });

        Messages.Record r = Messages.Record.newBuilder().setData(ByteString.copyFrom(TEST_DATA)).setPartitionKeyIndex(0)
                .build();

        byte[] payload = AggregatedRecord.newBuilder().addPartitionKeyTable(pk).addRecords(r).addRecords(r)
                .addRecords(r).build().toByteArray();

        bb.put(payload);
        bb.put(md5(payload));
        bb.limit(bb.position());
        bb.rewind();
        return bb;
    }

    private static List<Record> generateConsecutiveRecords(int numberOfRecords, String partitionKey, ByteBuffer data,
            Date arrivalTimestamp, BigInteger startSequenceNumber) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; ++i) {
            records.add(new Record().withPartitionKey(partitionKey).withData(data)
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

    private static TaskResultMatcher shardEndTaskResult(boolean isAtShardEnd) {
        TaskResult expected = new TaskResult(null, isAtShardEnd);
        return taskResult(expected);
    }

    private static TaskResultMatcher exceptionTaskResult(Exception ex) {
        TaskResult expected = new TaskResult(ex, false);
        return taskResult(expected);
    }

    private static TaskResultMatcher taskResult(TaskResult expected) {
        return new TaskResultMatcher(expected);
    }

    private static class TaskResultMatcher extends TypeSafeDiagnosingMatcher<TaskResult> {

        Matcher<TaskResult> matchers;

        TaskResultMatcher(TaskResult expected) {
            if (expected == null) {
                matchers = nullValue(TaskResult.class);
            } else {
                matchers = allOf(notNullValue(TaskResult.class),
                        hasProperty("shardEndReached", equalTo(expected.isShardEndReached())),
                        hasProperty("exception", equalTo(expected.getException())));
            }

        }

        @Override
        protected boolean matchesSafely(TaskResult item, Description mismatchDescription) {
            if (!matchers.matches(item)) {
                matchers.describeMismatch(item, mismatchDescription);
                return false;
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendDescriptionOf(matchers);
        }
    }
}
