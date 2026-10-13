package software.amazon.kinesis.retrieval;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.kinesis.retrieval.kpl.Messages;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static software.amazon.kinesis.retrieval.AggregatorUtil.AGGREGATED_RECORD_MAGIC;

public class AggregatorUtilTest {

    private static final AggregatorUtil AGGREGATOR_UTIL = new AggregatorUtil();

    @Test
    public void testErrorInProtobufMessagesParseFrom() {
        try (MockedStatic<Messages.AggregatedRecord> mockedStatic =
                Mockito.mockStatic(Messages.AggregatedRecord.class)) {
            mockedStatic
                    .when(() -> Messages.AggregatedRecord.parseFrom(any(byte[].class)))
                    .thenThrow(new NoClassDefFoundError("Test error"));

            final AggregatorUtil aggregatorUtil = new AggregatorUtil();
            assertThrows(
                    NoClassDefFoundError.class,
                    () -> aggregatorUtil.deaggregate(
                            getKinesisClientRecords(), new BigInteger("0"), new BigInteger("1")));
        }
    }

    @Test
    public void testDeaggregateDoesNotConsumeInputBuffers() {
        final Messages.AggregatedRecord aggregated = Messages.AggregatedRecord.newBuilder()
                .addPartitionKeyTable("pk")
                .addRecords(Messages.Record.newBuilder().setPartitionKeyIndex(0).setData(ByteString.copyFromUtf8("a")))
                .addRecords(Messages.Record.newBuilder().setPartitionKeyIndex(0).setData(ByteString.copyFromUtf8("b")))
                .build();
        final byte[] payload = aggregated.toByteArray();
        final ByteBuffer data = ByteBuffer.allocate(AGGREGATED_RECORD_MAGIC.length + payload.length + 16);
        data.put(AGGREGATED_RECORD_MAGIC).put(payload).put(AGGREGATOR_UTIL.calculateTailCheck(payload));
        data.flip();

        final List<KinesisClientRecord> records = Arrays.asList(
                KinesisClientRecord.builder()
                        .data(data)
                        .partitionKey("pk")
                        .sequenceNumber("1")
                        .build(),
                KinesisClientRecord.builder()
                        .data(ByteBuffer.wrap("raw".getBytes()))
                        .partitionKey("pk")
                        .sequenceNumber("2")
                        .build());

        // Deaggregating the same batch twice (e.g. on retry) must produce the same result.
        final List<KinesisClientRecord> first = AGGREGATOR_UTIL.deaggregate(records);
        final List<KinesisClientRecord> second = AGGREGATOR_UTIL.deaggregate(records);

        assertEquals(3, first.size());
        assertEquals(first, second);
        assertEquals(0, records.get(0).data().position());
        assertEquals(0, records.get(1).data().position());
    }

    private List<KinesisClientRecord> getKinesisClientRecords() {
        return Collections.singletonList(KinesisClientRecord.builder()
                .data(constructKplAggregatedRecord())
                .partitionKey("cat")
                .sequenceNumber("555")
                .build());
    }

    private ByteBuffer constructKplAggregatedRecord() {
        // Create message data
        byte[] messageData = "test message".getBytes();

        // Calculate digest for the message data
        byte[] calculatedDigest = AGGREGATOR_UTIL.calculateTailCheck(messageData);

        // Create buffer: magic + messageData + digest (size 16)
        ByteBuffer bb = ByteBuffer.allocate(AGGREGATED_RECORD_MAGIC.length + messageData.length + 16);
        bb.put(AGGREGATED_RECORD_MAGIC);
        bb.put(messageData);
        bb.put(calculatedDigest);
        bb.flip(); // Reset position to 0 for reading

        return bb;
    }
}
