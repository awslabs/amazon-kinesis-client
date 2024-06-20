package com.amazonaws.services.kinesis.clientlibrary.utils;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.ByteBuffer;

/**
 * Implements initalization and shutdown of shards as well as shard record processing
 */
@Slf4j
public class TestRecordProcessor implements IRecordProcessor {

    private String shardId;
    private static final String SHARD_ID_MDC_KEY = "ShardId";
    private final RecordValidatorQueue recordValidator;

    public TestRecordProcessor(RecordValidatorQueue recordValidator) {
        this.recordValidator = recordValidator;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.getShardId();
        MDC.put(SHARD_ID_MDC_KEY, shardId);
        try {
            log.info("Initializing @ Sequence: {}", initializationInput.getExtendedSequenceNumber());
        } finally {
            MDC.remove(SHARD_ID_MDC_KEY);
        }
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        MDC.put(SHARD_ID_MDC_KEY, shardId);
        try {
            log.info("Processing {} record(s)", processRecordsInput.getRecords().size());

            for (Record kinesisRecord : processRecordsInput.getRecords()) {
                final String data = new String(asByteArray(kinesisRecord.getData()));
                log.info("Processing record pk: {}", data);
                recordValidator.add(shardId, data);
            }
            checkpoint(processRecordsInput.getCheckpointer(), "ProcessRecords");
        } catch (Throwable t) {
            log.error("Caught throwable while processing records. Aborting.", t);
            Runtime.getRuntime().halt(1);
        } finally {
            MDC.remove(SHARD_ID_MDC_KEY);
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        MDC.put(SHARD_ID_MDC_KEY, shardId);

        try {
            log.info("Scheduler is shutting down, checkpointing.");
            shutdownInput.getCheckpointer().checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            log.error("Exception while checkpointing at requested shutdown. Giving up.", e);
        } finally {
            MDC.remove(SHARD_ID_MDC_KEY);
        }
    }

    private static byte[] asByteArray(ByteBuffer buf) {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer, String operation) {
        try {
            checkpointer.checkpoint();
        } catch (com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException |
                 com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException e) {
            log.error("{}: Error while checkpointing, called from {}", shardId, operation, e);
        }
    }
}
