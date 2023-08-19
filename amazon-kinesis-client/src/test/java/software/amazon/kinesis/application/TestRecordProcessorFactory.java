package software.amazon.kinesis.application;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.utils.RecordValidatorQueue;

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
