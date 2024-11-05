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
package software.amazon.kinesis.lifecycle;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import lombok.Data;
import lombok.Getter;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.leases.LeaseStatsRecorder;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.ThrottlingReporter;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.retrieval.kpl.Messages;
import software.amazon.kinesis.retrieval.kpl.Messages.AggregatedRecord;
import software.amazon.kinesis.schemaregistry.SchemaRegistryDecoder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProcessTaskTest {
    private static final long IDLE_TIME_IN_MILLISECONDS = 100L;
    private static final Schema SCHEMA_REGISTRY_SCHEMA = new Schema("{}", "AVRO", "demoSchema");
    private static final byte[] SCHEMA_REGISTRY_PAYLOAD = new byte[] {01, 05, 03, 05};

    private boolean shouldCallProcessRecordsEvenForEmptyRecordList = true;
    private boolean skipShardSyncAtWorkerInitializationIfLeasesExist = true;
    private ShardInfo shardInfo;

    @Mock
    private ProcessRecordsInput processRecordsInput;

    @Mock
    private ShardDetector shardDetector;

    @Mock
    private GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer;

    private static final byte[] TEST_DATA = new byte[] {1, 2, 3, 4};

    private final String shardId = "shard-test";
    private final long taskBackoffTimeMillis = 1L;

    @Mock
    private ShardRecordProcessor shardRecordProcessor;

    @Mock
    private ShardRecordProcessorCheckpointer checkpointer;

    @Mock
    private ThrottlingReporter throttlingReporter;

    @Mock
    private LeaseStatsRecorder leaseStatsRecorder;

    private ProcessTask processTask;

    @Before
    public void setUpProcessTask() {
        when(checkpointer.checkpointer()).thenReturn(mock(Checkpointer.class));

        shardInfo = new ShardInfo(shardId, null, null, null);
    }

    private ProcessTask makeProcessTask(ProcessRecordsInput processRecordsInput) {
        return makeProcessTask(
                processRecordsInput, new AggregatorUtil(), skipShardSyncAtWorkerInitializationIfLeasesExist);
    }

    private ProcessTask makeProcessTask(
            ProcessRecordsInput processRecordsInput, GlueSchemaRegistryDeserializer deserializer) {
        return makeProcessTask(
                processRecordsInput,
                new AggregatorUtil(),
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                new SchemaRegistryDecoder(deserializer));
    }

    private ProcessTask makeProcessTask(
            ProcessRecordsInput processRecordsInput, AggregatorUtil aggregatorUtil, boolean skipShardSync) {
        return makeProcessTask(processRecordsInput, aggregatorUtil, skipShardSync, null);
    }

    private ProcessTask makeProcessTask(
            ProcessRecordsInput processRecordsInput,
            AggregatorUtil aggregatorUtil,
            boolean skipShardSync,
            SchemaRegistryDecoder schemaRegistryDecoder) {
        return new ProcessTask(
                shardInfo,
                shardRecordProcessor,
                checkpointer,
                taskBackoffTimeMillis,
                skipShardSync,
                shardDetector,
                throttlingReporter,
                processRecordsInput,
                shouldCallProcessRecordsEvenForEmptyRecordList,
                IDLE_TIME_IN_MILLISECONDS,
                aggregatorUtil,
                new NullMetricsFactory(),
                schemaRegistryDecoder,
                leaseStatsRecorder);
    }

    @Test
    public void testProcessTaskWithShardEndReached() {
        processTask = makeProcessTask(processRecordsInput);
        when(processRecordsInput.isAtShardEnd()).thenReturn(true);

        TaskResult result = processTask.call();
        assertThat(result, shardEndTaskResult(true));
    }

    private KinesisClientRecord makeKinesisClientRecord(String partitionKey, String sequenceNumber, Instant arrival) {
        return KinesisClientRecord.builder()
                .partitionKey(partitionKey)
                .sequenceNumber(sequenceNumber)
                .approximateArrivalTimestamp(arrival)
                .data(ByteBuffer.wrap(TEST_DATA))
                .build();
    }

    private KinesisClientRecord makeKinesisClientRecord(
            String partitionKey, String sequenceNumber, Instant arrival, ByteBuffer data, Schema schema) {
        return KinesisClientRecord.builder()
                .partitionKey(partitionKey)
                .sequenceNumber(sequenceNumber)
                .approximateArrivalTimestamp(arrival)
                .data(data)
                .schema(schema)
                .build();
    }

    @Test
    public void testNonAggregatedKinesisRecord() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final String pk = UUID.randomUUID().toString();
        final Date ts = new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(4, TimeUnit.HOURS));
        final KinesisClientRecord r = makeKinesisClientRecord(pk, sqn, ts.toInstant());

        ShardRecordProcessorOutcome outcome = testWithRecord(r);

        assertEquals(1, outcome.getProcessRecordsCall().records().size());

        KinesisClientRecord pr = outcome.getProcessRecordsCall().records().get(0);
        assertEquals(pk, pr.partitionKey());
        assertEquals(ts.toInstant(), pr.approximateArrivalTimestamp());
        byte[] b = pr.data().array();
        assertThat(b, equalTo(TEST_DATA));

        assertEquals(sqn, outcome.getCheckpointCall().sequenceNumber());
        assertEquals(0, outcome.getCheckpointCall().subSequenceNumber());
    }

    @Data
    static class ShardRecordProcessorOutcome {
        final ProcessRecordsInput processRecordsCall;
        final ExtendedSequenceNumber checkpointCall;
    }

    @Test
    public void testDeaggregatesRecord() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final String pk = UUID.randomUUID().toString();
        final Instant ts = Instant.now().minus(4, ChronoUnit.HOURS);
        KinesisClientRecord record = KinesisClientRecord.builder()
                .partitionKey("-")
                .data(generateAggregatedRecord(pk))
                .sequenceNumber(sqn)
                .approximateArrivalTimestamp(ts)
                .build();

        processTask = makeProcessTask(processRecordsInput);
        ShardRecordProcessorOutcome outcome = testWithRecord(record);

        List<KinesisClientRecord> actualRecords =
                outcome.getProcessRecordsCall().records();

        assertEquals(3, actualRecords.size());
        for (KinesisClientRecord pr : actualRecords) {
            assertThat(pr, instanceOf(KinesisClientRecord.class));
            assertEquals(pk, pr.partitionKey());
            assertEquals(ts, pr.approximateArrivalTimestamp());

            byte[] actualData = new byte[pr.data().limit()];
            pr.data().get(actualData);
            assertThat(actualData, equalTo(TEST_DATA));
        }

        assertEquals(sqn, outcome.getCheckpointCall().sequenceNumber());
        assertEquals(actualRecords.size() - 1, outcome.getCheckpointCall().subSequenceNumber());
    }

    @Test
    public void testDeaggregatesRecordWithNoArrivalTimestamp() {
        final String sqn = new BigInteger(128, new Random()).toString();
        final String pk = UUID.randomUUID().toString();

        KinesisClientRecord record = KinesisClientRecord.builder()
                .partitionKey("-")
                .data(generateAggregatedRecord(pk))
                .sequenceNumber(sqn)
                .build();

        processTask = makeProcessTask(processRecordsInput);
        ShardRecordProcessorOutcome outcome = testWithRecord(record);

        List<KinesisClientRecord> actualRecords =
                outcome.getProcessRecordsCall().records();

        assertEquals(3, actualRecords.size());
        for (KinesisClientRecord actualRecord : actualRecords) {
            assertThat(actualRecord.partitionKey(), equalTo(pk));
            assertThat(actualRecord.approximateArrivalTimestamp(), nullValue());
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
        final List<KinesisClientRecord> records =
                generateConsecutiveRecords(numberOfRecords, "-", ByteBuffer.wrap(TEST_DATA), new Date(), startingSqn);

        processTask = makeProcessTask(processRecordsInput);
        ShardRecordProcessorOutcome outcome = testWithRecords(
                records,
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
        final ExtendedSequenceNumber largestPermittedEsqn =
                new ExtendedSequenceNumber(baseSqn.add(BigInteger.valueOf(100)).toString());

        processTask = makeProcessTask(processRecordsInput);
        ShardRecordProcessorOutcome outcome =
                testWithRecords(Collections.emptyList(), lastCheckpointEspn, largestPermittedEsqn);

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
        BigInteger previousCheckpointSqn = new BigInteger(128, new Random());
        long previousCheckpointSsqn = 1;

        // Values for this processRecords call.
        String startingSqn = previousCheckpointSqn.toString();
        String pk = UUID.randomUUID().toString();
        KinesisClientRecord record = KinesisClientRecord.builder()
                .partitionKey("-")
                .data(generateAggregatedRecord(pk))
                .sequenceNumber(startingSqn)
                .build();

        processTask = makeProcessTask(processRecordsInput);
        ShardRecordProcessorOutcome outcome = testWithRecords(
                Collections.singletonList(record),
                new ExtendedSequenceNumber(previousCheckpointSqn.toString(), previousCheckpointSsqn),
                new ExtendedSequenceNumber(previousCheckpointSqn.toString(), previousCheckpointSsqn));

        List<KinesisClientRecord> actualRecords =
                outcome.getProcessRecordsCall().records();

        // First two records should be dropped - and only 1 remaining records should be there.
        assertThat(actualRecords.size(), equalTo(1));

        // Verify user record's extended sequence number and other fields.
        KinesisClientRecord actualRecord = actualRecords.get(0);
        assertThat(actualRecord.partitionKey(), equalTo(pk));
        assertThat(actualRecord.sequenceNumber(), equalTo(startingSqn));
        assertThat(actualRecord.subSequenceNumber(), equalTo(previousCheckpointSsqn + 1));
        assertThat(actualRecord.approximateArrivalTimestamp(), nullValue());

        // Expected largest permitted sequence number will be last sub-record sequence number.
        final ExtendedSequenceNumber expectedLargestPermittedEsqn =
                new ExtendedSequenceNumber(previousCheckpointSqn.toString(), 2L);
        assertEquals(expectedLargestPermittedEsqn, outcome.getCheckpointCall());
    }

    @Test
    public void testDiscardReshardedKplData() throws Exception {
        BigInteger sequenceNumber = new BigInteger(120, ThreadLocalRandom.current());

        String lowHashKey = BigInteger.ONE.shiftLeft(60).toString();
        String highHashKey = BigInteger.ONE.shiftLeft(68).toString();

        ControlledHashAggregatorUtil aggregatorUtil = new ControlledHashAggregatorUtil(lowHashKey, highHashKey);
        AggregatedRecord.Builder aggregatedRecord = AggregatedRecord.newBuilder();
        Instant approximateArrivalTime = Instant.now();
        int recordIndex = 0;
        sequenceNumber = sequenceNumber.add(BigInteger.ONE);
        for (int i = 0; i < 5; ++i) {
            KinesisClientRecord expectedRecord = createAndRegisterAggregatedRecord(
                    sequenceNumber, aggregatedRecord, recordIndex, approximateArrivalTime);
            aggregatorUtil.addInRange(expectedRecord);
            recordIndex++;
        }

        sequenceNumber = sequenceNumber.add(BigInteger.ONE);
        for (int i = 0; i < 5; ++i) {
            KinesisClientRecord expectedRecord = createAndRegisterAggregatedRecord(
                    sequenceNumber, aggregatedRecord, recordIndex, approximateArrivalTime);
            aggregatorUtil.addBelowRange(expectedRecord);
            recordIndex++;
        }

        sequenceNumber = sequenceNumber.add(BigInteger.ONE);
        for (int i = 0; i < 5; ++i) {
            KinesisClientRecord expectedRecord = createAndRegisterAggregatedRecord(
                    sequenceNumber, aggregatedRecord, recordIndex, approximateArrivalTime);
            aggregatorUtil.addAboveRange(expectedRecord);
            recordIndex++;
        }

        byte[] payload = aggregatedRecord.build().toByteArray();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(new byte[] {-13, -119, -102, -62});
        bos.write(payload);
        bos.write(md5(payload));

        ByteBuffer rawRecordData = ByteBuffer.wrap(bos.toByteArray());

        KinesisClientRecord rawRecord = KinesisClientRecord.builder()
                .data(rawRecordData)
                .approximateArrivalTimestamp(approximateArrivalTime)
                .partitionKey("p-01")
                .sequenceNumber(sequenceNumber.toString())
                .build();

        when(shardDetector.shard(any()))
                .thenReturn(Shard.builder()
                        .shardId("Shard-01")
                        .hashKeyRange(HashKeyRange.builder()
                                .startingHashKey(lowHashKey)
                                .endingHashKey(highHashKey)
                                .build())
                        .build());

        when(processRecordsInput.records()).thenReturn(Collections.singletonList(rawRecord));
        ProcessTask processTask = makeProcessTask(processRecordsInput, aggregatorUtil, false);
        ShardRecordProcessorOutcome outcome = testWithRecords(
                processTask,
                new ExtendedSequenceNumber(
                        sequenceNumber.subtract(BigInteger.valueOf(100)).toString(), 0L),
                new ExtendedSequenceNumber(sequenceNumber.toString(), recordIndex + 1L));

        assertThat(outcome.processRecordsCall.records().size(), equalTo(0));
    }

    @Test
    public void testAllInShardKplData() throws Exception {
        BigInteger sequenceNumber = new BigInteger(120, ThreadLocalRandom.current());

        String lowHashKey = BigInteger.ONE.shiftLeft(60).toString();
        String highHashKey = BigInteger.ONE.shiftLeft(68).toString();

        ControlledHashAggregatorUtil aggregatorUtil = new ControlledHashAggregatorUtil(lowHashKey, highHashKey);

        List<KinesisClientRecord> expectedRecords = new ArrayList<>();
        List<KinesisClientRecord> rawRecords = new ArrayList<>();

        for (int i = 0; i < 3; ++i) {
            AggregatedRecord.Builder aggregatedRecord = AggregatedRecord.newBuilder();
            Instant approximateArrivalTime = Instant.now().minus(i + 4, ChronoUnit.SECONDS);
            sequenceNumber = sequenceNumber.add(BigInteger.ONE);
            for (int j = 0; j < 2; ++j) {
                KinesisClientRecord expectedRecord =
                        createAndRegisterAggregatedRecord(sequenceNumber, aggregatedRecord, j, approximateArrivalTime);
                aggregatorUtil.addInRange(expectedRecord);
                expectedRecords.add(expectedRecord);
            }

            byte[] payload = aggregatedRecord.build().toByteArray();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(AggregatorUtil.AGGREGATED_RECORD_MAGIC);
            bos.write(payload);
            bos.write(md5(payload));

            ByteBuffer rawRecordData = ByteBuffer.wrap(bos.toByteArray());

            KinesisClientRecord rawRecord = KinesisClientRecord.builder()
                    .data(rawRecordData)
                    .approximateArrivalTimestamp(approximateArrivalTime)
                    .partitionKey("pa-" + i)
                    .sequenceNumber(sequenceNumber.toString())
                    .build();

            rawRecords.add(rawRecord);
        }

        when(shardDetector.shard(any()))
                .thenReturn(Shard.builder()
                        .shardId("Shard-01")
                        .hashKeyRange(HashKeyRange.builder()
                                .startingHashKey(lowHashKey)
                                .endingHashKey(highHashKey)
                                .build())
                        .build());

        when(processRecordsInput.records()).thenReturn(rawRecords);
        ProcessTask processTask = makeProcessTask(processRecordsInput, aggregatorUtil, false);
        ShardRecordProcessorOutcome outcome = testWithRecords(
                processTask,
                new ExtendedSequenceNumber(
                        sequenceNumber.subtract(BigInteger.valueOf(100)).toString(), 0L),
                new ExtendedSequenceNumber(sequenceNumber.toString(), 0L));

        assertThat(outcome.processRecordsCall.records(), equalTo(expectedRecords));
    }

    @Test
    public void testProcessTask_WhenSchemaRegistryRecordsAreSent_ProcessesThemSuccessfully() {
        processTask = makeProcessTask(processRecordsInput, glueSchemaRegistryDeserializer);
        final BigInteger sqn = new BigInteger(128, new Random());
        final BigInteger previousCheckpointSqn = BigInteger.valueOf(1);
        final String pk = UUID.randomUUID().toString();
        final Date ts = new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(4, TimeUnit.HOURS));

        // Payload set to SchemaRegistry encoded data and schema to null
        // to mimic Schema Registry encoded message from Kinesis stream.
        final KinesisClientRecord schemaRegistryRecord = makeKinesisClientRecord(
                pk, sqn.toString(), ts.toInstant(), ByteBuffer.wrap(SCHEMA_REGISTRY_PAYLOAD), null);

        final KinesisClientRecord nonSchemaRegistryRecord = makeKinesisClientRecord(pk, sqn.toString(), ts.toInstant());

        when(processRecordsInput.records()).thenReturn(ImmutableList.of(schemaRegistryRecord, nonSchemaRegistryRecord));

        doReturn(true).when(glueSchemaRegistryDeserializer).canDeserialize(SCHEMA_REGISTRY_PAYLOAD);
        doReturn(TEST_DATA).when(glueSchemaRegistryDeserializer).getData(SCHEMA_REGISTRY_PAYLOAD);
        doReturn(SCHEMA_REGISTRY_SCHEMA).when(glueSchemaRegistryDeserializer).getSchema(SCHEMA_REGISTRY_PAYLOAD);

        ShardRecordProcessorOutcome outcome = testWithRecords(
                processTask,
                new ExtendedSequenceNumber(previousCheckpointSqn.toString(), 0L),
                new ExtendedSequenceNumber(
                        previousCheckpointSqn.add(previousCheckpointSqn).toString(), 1L));

        KinesisClientRecord decodedSchemaRegistryRecord = makeKinesisClientRecord(
                pk, sqn.toString(), ts.toInstant(), ByteBuffer.wrap(TEST_DATA), SCHEMA_REGISTRY_SCHEMA);
        List<KinesisClientRecord> expectedRecords =
                ImmutableList.of(decodedSchemaRegistryRecord, nonSchemaRegistryRecord);

        List<KinesisClientRecord> actualRecords =
                outcome.getProcessRecordsCall().records();

        assertEquals(expectedRecords, actualRecords);

        verify(glueSchemaRegistryDeserializer, times(1)).canDeserialize(SCHEMA_REGISTRY_PAYLOAD);
        verify(glueSchemaRegistryDeserializer, times(1)).getSchema(SCHEMA_REGISTRY_PAYLOAD);
        verify(glueSchemaRegistryDeserializer, times(1)).getData(SCHEMA_REGISTRY_PAYLOAD);
    }

    @Test
    public void testProcessTask_WhenSchemaRegistryDecodeCheckFails_IgnoresRecord() {
        processTask = makeProcessTask(processRecordsInput, glueSchemaRegistryDeserializer);
        final BigInteger sqn = new BigInteger(128, new Random());
        final BigInteger previousCheckpointSqn = BigInteger.valueOf(1);
        final String pk = UUID.randomUUID().toString();
        final Date ts = new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(4, TimeUnit.HOURS));

        // Payload set to SchemaRegistry encoded data and schema to null
        // to mimic Schema Registry encoded message from Kinesis stream.
        final KinesisClientRecord schemaRegistryRecord = makeKinesisClientRecord(
                pk, sqn.toString(), ts.toInstant(), ByteBuffer.wrap(SCHEMA_REGISTRY_PAYLOAD), null);

        final KinesisClientRecord nonSchemaRegistryRecord = makeKinesisClientRecord(pk, sqn.toString(), ts.toInstant());

        when(processRecordsInput.records()).thenReturn(ImmutableList.of(schemaRegistryRecord, nonSchemaRegistryRecord));

        doThrow(new RuntimeException("Invalid data"))
                .when(glueSchemaRegistryDeserializer)
                .canDeserialize(SCHEMA_REGISTRY_PAYLOAD);

        ShardRecordProcessorOutcome outcome = testWithRecords(
                processTask,
                new ExtendedSequenceNumber(previousCheckpointSqn.toString(), 0L),
                new ExtendedSequenceNumber(
                        previousCheckpointSqn.add(previousCheckpointSqn).toString(), 1L));

        List<KinesisClientRecord> expectedRecords = ImmutableList.of(schemaRegistryRecord, nonSchemaRegistryRecord);

        List<KinesisClientRecord> actualRecords =
                outcome.getProcessRecordsCall().records();

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void testProcessTask_WhenSchemaRegistryDecodingFails_IgnoresRecord() {
        processTask = makeProcessTask(processRecordsInput, glueSchemaRegistryDeserializer);
        final BigInteger sqn = new BigInteger(128, new Random());
        final BigInteger previousCheckpointSqn = BigInteger.valueOf(1);
        final String pk = UUID.randomUUID().toString();
        final Date ts = new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(4, TimeUnit.HOURS));

        // Payload set to SchemaRegistry encoded data and schema to null
        // to mimic Schema Registry encoded message from Kinesis stream.
        final KinesisClientRecord schemaRegistryRecord = makeKinesisClientRecord(
                pk, sqn.toString(), ts.toInstant(), ByteBuffer.wrap(SCHEMA_REGISTRY_PAYLOAD), null);

        final KinesisClientRecord nonSchemaRegistryRecord = makeKinesisClientRecord(pk, sqn.toString(), ts.toInstant());

        when(processRecordsInput.records()).thenReturn(ImmutableList.of(schemaRegistryRecord, nonSchemaRegistryRecord));

        doReturn(true).when(glueSchemaRegistryDeserializer).canDeserialize(SCHEMA_REGISTRY_PAYLOAD);

        doThrow(new RuntimeException("Cannot decode data"))
                .when(glueSchemaRegistryDeserializer)
                .getData(SCHEMA_REGISTRY_PAYLOAD);

        ShardRecordProcessorOutcome outcome = testWithRecords(
                processTask,
                new ExtendedSequenceNumber(previousCheckpointSqn.toString(), 0L),
                new ExtendedSequenceNumber(
                        previousCheckpointSqn.add(previousCheckpointSqn).toString(), 1L));

        List<KinesisClientRecord> expectedRecords = ImmutableList.of(schemaRegistryRecord, nonSchemaRegistryRecord);

        List<KinesisClientRecord> actualRecords =
                outcome.getProcessRecordsCall().records();

        assertEquals(expectedRecords, actualRecords);
    }

    private KinesisClientRecord createAndRegisterAggregatedRecord(
            BigInteger sequenceNumber,
            AggregatedRecord.Builder aggregatedRecord,
            int i,
            Instant approximateArrivalTime) {
        byte[] dataArray = new byte[1024];
        ThreadLocalRandom.current().nextBytes(dataArray);
        ByteBuffer data = ByteBuffer.wrap(dataArray);

        KinesisClientRecord expectedRecord = KinesisClientRecord.builder()
                .partitionKey("p-" + i)
                .sequenceNumber(sequenceNumber.toString())
                .approximateArrivalTimestamp(approximateArrivalTime)
                .data(data)
                .subSequenceNumber(i)
                .aggregated(true)
                .build();

        Messages.Record kplRecord = Messages.Record.newBuilder()
                .setData(ByteString.copyFrom(dataArray))
                .setPartitionKeyIndex(i)
                .build();
        aggregatedRecord.addPartitionKeyTable(expectedRecord.partitionKey()).addRecords(kplRecord);

        return expectedRecord;
    }

    private enum RecordRangeState {
        BELOW_RANGE,
        IN_RANGE,
        ABOVE_RANGE
    }

    @Getter
    private static class ControlledHashAggregatorUtil extends AggregatorUtil {

        private final BigInteger lowHashKey;
        private final BigInteger highHashKey;
        private final long width;
        private final Map<String, RecordRangeState> recordRanges = new HashMap<>();

        ControlledHashAggregatorUtil(String lowHashKey, String highHashKey) {
            this.lowHashKey = new BigInteger(lowHashKey);
            this.highHashKey = new BigInteger(highHashKey);
            this.width = this.highHashKey
                            .subtract(this.lowHashKey)
                            .mod(BigInteger.valueOf(Long.MAX_VALUE))
                            .longValue()
                    - 1;
        }

        void add(KinesisClientRecord record, RecordRangeState recordRangeState) {
            recordRanges.put(record.partitionKey(), recordRangeState);
        }

        void addInRange(KinesisClientRecord record) {
            add(record, RecordRangeState.IN_RANGE);
        }

        void addBelowRange(KinesisClientRecord record) {
            add(record, RecordRangeState.BELOW_RANGE);
        }

        void addAboveRange(KinesisClientRecord record) {
            add(record, RecordRangeState.ABOVE_RANGE);
        }

        @Override
        protected BigInteger effectiveHashKey(String partitionKey, String explicitHashKey) {
            RecordRangeState rangeState = recordRanges.get(partitionKey);
            assertThat(rangeState, not(nullValue()));

            switch (rangeState) {
                case BELOW_RANGE:
                    return lowHashKey.subtract(
                            BigInteger.valueOf(ThreadLocalRandom.current().nextInt())
                                    .abs());
                case IN_RANGE:
                    return lowHashKey.add(
                            BigInteger.valueOf(ThreadLocalRandom.current().nextLong(width)));
                case ABOVE_RANGE:
                    return highHashKey
                            .add(BigInteger.ONE)
                            .add(BigInteger.valueOf(ThreadLocalRandom.current().nextInt())
                                    .abs());
                default:
                    throw new IllegalStateException("Unknown range state: " + rangeState);
            }
        }
    }

    private ShardRecordProcessorOutcome testWithRecord(KinesisClientRecord record) {
        return testWithRecords(
                Collections.singletonList(record),
                ExtendedSequenceNumber.TRIM_HORIZON,
                ExtendedSequenceNumber.TRIM_HORIZON);
    }

    private ShardRecordProcessorOutcome testWithRecords(
            List<KinesisClientRecord> records,
            ExtendedSequenceNumber lastCheckpointValue,
            ExtendedSequenceNumber largestPermittedCheckpointValue) {
        return testWithRecords(records, lastCheckpointValue, largestPermittedCheckpointValue, new AggregatorUtil());
    }

    private ShardRecordProcessorOutcome testWithRecords(
            List<KinesisClientRecord> records,
            ExtendedSequenceNumber lastCheckpointValue,
            ExtendedSequenceNumber largestPermittedCheckpointValue,
            AggregatorUtil aggregatorUtil) {
        when(processRecordsInput.records()).thenReturn(records);
        return testWithRecords(
                makeProcessTask(processRecordsInput, aggregatorUtil, skipShardSyncAtWorkerInitializationIfLeasesExist),
                lastCheckpointValue,
                largestPermittedCheckpointValue);
    }

    private ShardRecordProcessorOutcome testWithRecords(
            ProcessTask processTask,
            ExtendedSequenceNumber lastCheckpointValue,
            ExtendedSequenceNumber largestPermittedCheckpointValue) {
        when(checkpointer.lastCheckpointValue()).thenReturn(lastCheckpointValue);
        when(checkpointer.largestPermittedCheckpointValue()).thenReturn(largestPermittedCheckpointValue);
        processTask.call();
        verify(throttlingReporter).success();
        verify(throttlingReporter, never()).throttled();
        ArgumentCaptor<ProcessRecordsInput> recordsCaptor = ArgumentCaptor.forClass(ProcessRecordsInput.class);
        verify(shardRecordProcessor).processRecords(recordsCaptor.capture());

        ArgumentCaptor<ExtendedSequenceNumber> esnCaptor = ArgumentCaptor.forClass(ExtendedSequenceNumber.class);
        verify(checkpointer).largestPermittedCheckpointValue(esnCaptor.capture());

        return new ShardRecordProcessorOutcome(recordsCaptor.getValue(), esnCaptor.getValue());
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
        bb.put(new byte[] {-13, -119, -102, -62});

        Messages.Record r = Messages.Record.newBuilder()
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

    private static List<KinesisClientRecord> generateConsecutiveRecords(
            int numberOfRecords,
            String partitionKey,
            ByteBuffer data,
            Date arrivalTimestamp,
            BigInteger startSequenceNumber) {
        List<KinesisClientRecord> records = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; ++i) {
            String seqNum = startSequenceNumber.add(BigInteger.valueOf(i)).toString();
            KinesisClientRecord record = KinesisClientRecord.builder()
                    .partitionKey(partitionKey)
                    .data(data)
                    .sequenceNumber(seqNum)
                    .approximateArrivalTimestamp(arrivalTimestamp.toInstant())
                    .build();
            records.add(record);
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
                matchers = Matchers.allOf(
                        notNullValue(TaskResult.class),
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
