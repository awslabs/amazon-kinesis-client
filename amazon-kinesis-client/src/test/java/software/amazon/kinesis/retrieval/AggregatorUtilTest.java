package software.amazon.kinesis.retrieval;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.kinesis.retrieval.kpl.Messages;

import static org.junit.Assert.assertThrows;
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
