package software.amazon.kinesis.integration_tests;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.utils.RecordValidatorQueue;

public class TestRecordProcessorFactoryV2 implements ShardRecordProcessorFactory {

    RecordValidatorQueue recordValidator;

    public TestRecordProcessorFactoryV2( RecordValidatorQueue recordValidator ) {
        this.recordValidator = recordValidator;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new TestRecordProcessorV2( this.recordValidator );
    }

}
