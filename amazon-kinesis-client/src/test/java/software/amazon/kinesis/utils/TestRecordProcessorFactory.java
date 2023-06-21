package software.amazon.kinesis.utils;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class TestRecordProcessorFactory implements ShardRecordProcessorFactory {

    private final RecordValidatorQueue recordValidator;

    public TestRecordProcessorFactory(RecordValidatorQueue recordValidator) {
        this.recordValidator = recordValidator;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new TestRecordProcessor(this.recordValidator);
    }

}
