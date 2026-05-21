package software.amazon.kinesis.sample.otel;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * Simple record processor that logs records and checkpoints after each batch.
 */
public class SampleRecordProcessor implements ShardRecordProcessor {
    private static final Logger log = LoggerFactory.getLogger(SampleRecordProcessor.class);

    private String shardId;

    @Override
    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.shardId();
        log.info("Initialized processor for shard: {}", shardId);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        var records = processRecordsInput.records();
        log.info("Processing {} records from shard {}", records.size(), shardId);

        for (KinesisClientRecord record : records) {
            String data = StandardCharsets.UTF_8.decode(record.data()).toString();
            log.debug("Record [partitionKey={}, seq={}]: {}",
                    record.partitionKey(), record.sequenceNumber(), data);
        }

        try {
            processRecordsInput.checkpointer().checkpoint();
        } catch (Exception e) {
            log.error("Failed to checkpoint shard {}", shardId, e);
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.info("Lease lost for shard: {}", shardId);
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        log.info("Shard ended: {}", shardId);
        try {
            shardEndedInput.checkpointer().checkpoint();
        } catch (Exception e) {
            log.error("Failed to checkpoint at shard end for {}", shardId, e);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Shutdown requested for shard: {}", shardId);
        try {
            shutdownRequestedInput.checkpointer().checkpoint();
        } catch (Exception e) {
            log.error("Failed to checkpoint during shutdown for {}", shardId, e);
        }
    }
}
