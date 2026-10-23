package software.amazon.kinesis.retrieval;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.Test;
import software.amazon.kinesis.retrieval.kpl.Messages;

import static org.junit.Assert.assertEquals;
import static software.amazon.kinesis.retrieval.AggregatorUtil.AGGREGATED_RECORD_MAGIC;

public class AggregatorUtilTest {

    private static final AggregatorUtil AGGREGATOR_UTIL = new AggregatorUtil();

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
}
