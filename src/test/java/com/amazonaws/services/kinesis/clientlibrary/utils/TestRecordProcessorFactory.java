package com.amazonaws.services.kinesis.clientlibrary.utils;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class TestRecordProcessorFactory implements IRecordProcessorFactory {

    private final RecordValidatorQueue recordValidator;

    public TestRecordProcessorFactory(RecordValidatorQueue recordValidator) {
        this.recordValidator = recordValidator;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new TestRecordProcessor(this.recordValidator);
    }
}
