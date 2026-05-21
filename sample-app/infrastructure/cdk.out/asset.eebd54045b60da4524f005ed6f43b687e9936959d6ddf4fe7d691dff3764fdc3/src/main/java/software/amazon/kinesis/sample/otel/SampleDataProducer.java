package software.amazon.kinesis.sample.otel;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

/**
 * Standalone utility that produces sample records to the Kinesis stream.
 * Run separately from the consumer to generate test data.
 */
public class SampleDataProducer {
    private static final Logger log = LoggerFactory.getLogger(SampleDataProducer.class);
    private static final String[] EVENT_TYPES = {"ORDER_PLACED", "ITEM_SHIPPED", "PAYMENT_RECEIVED"};

    public static void main(String[] args) throws InterruptedException {
        SampleAppConfig config = SampleAppConfig.fromEnvironment();
        log.info("Starting data producer: stream={}, rate={}/s, duration={}s",
                config.getStreamName(), config.getProducerRecordsPerSecond(), config.getProducerDurationSeconds());

        KinesisClient kinesisClient = KinesisClient.builder()
                .region(Region.of(config.getRegion()))
                .build();

        Random random = new Random();
        int totalRecords = config.getProducerRecordsPerSecond() * config.getProducerDurationSeconds();
        long sleepMs = 1000L / config.getProducerRecordsPerSecond();

        for (int i = 0; i < totalRecords; i++) {
            String payload = String.format(
                    "{\"eventId\":\"%s\",\"eventType\":\"%s\",\"timestamp\":\"%s\",\"amount\":%.2f,\"customerId\":\"customer-%d\"}",
                    UUID.randomUUID(),
                    EVENT_TYPES[random.nextInt(EVENT_TYPES.length)],
                    Instant.now().toString(),
                    random.nextDouble() * 100,
                    random.nextInt(1000));

            kinesisClient.putRecord(PutRecordRequest.builder()
                    .streamName(config.getStreamName())
                    .partitionKey("partition-" + random.nextInt(100))
                    .data(SdkBytes.fromUtf8String(payload))
                    .build());

            if (i % 100 == 0) {
                log.info("Produced {} / {} records", i, totalRecords);
            }
            Thread.sleep(sleepMs);
        }

        log.info("Data producer complete: {} records sent", totalRecords);
        kinesisClient.close();
    }
}
