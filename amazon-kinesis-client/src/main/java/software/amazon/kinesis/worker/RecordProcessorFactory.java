package software.amazon.kinesis.worker;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class RecordProcessorFactory implements ShardRecordProcessorFactory {
    public ShardRecordProcessor shardRecordProcessor() {
        return new RecordProcessor();
    }
}
