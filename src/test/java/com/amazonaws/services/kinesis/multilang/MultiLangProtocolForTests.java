package com.amazonaws.services.kinesis.multilang;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;

/**
 *
 */
public class MultiLangProtocolForTests extends MultiLangProtocol {
    /**
     * Constructor.
     *
     * @param messageReader       A message reader.
     * @param messageWriter       A message writer.
     * @param initializationInput
     * @param configuration
     */
    MultiLangProtocolForTests(final MessageReader messageReader,
                              final MessageWriter messageWriter,
                              final InitializationInput initializationInput,
                              final KinesisClientLibConfiguration configuration) {
        super(messageReader, messageWriter, initializationInput, configuration);
    }

    @Override
    protected void haltJvm(final int exitStatus) {
        throw new RuntimeException("Halt called");
    }
}
