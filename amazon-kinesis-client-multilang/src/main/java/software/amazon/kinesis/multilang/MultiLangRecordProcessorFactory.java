/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.kinesis.multilang;

import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.multilang.config.MultiLangDaemonConfiguration;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

/**
 * Creates {@link MultiLangShardRecordProcessor}'s.
 */
@Slf4j
public class MultiLangRecordProcessorFactory implements ShardRecordProcessorFactory {
    private static final String COMMAND_DELIMETER_REGEX = " +";

    private final String command;
    private final String[] commandArray;

    private final ObjectMapper objectMapper;

    private final ExecutorService executorService;

    private final MultiLangDaemonConfiguration configuration;

    /**
     * @param command The command that will do processing for this factory's record processors.
     * @param executorService An executor service to use while processing inputs and outputs of the child process.
     */
    public MultiLangRecordProcessorFactory(
            String command, ExecutorService executorService, MultiLangDaemonConfiguration configuration) {
        this(command, executorService, new ObjectMapper(), configuration);
    }

    /**
     * @param command The command that will do processing for this factory's record processors.
     * @param executorService An executor service to use while processing inputs and outputs of the child process.
     * @param objectMapper An object mapper used to convert messages to json to be written to the child process
     */
    public MultiLangRecordProcessorFactory(
            String command,
            ExecutorService executorService,
            ObjectMapper objectMapper,
            MultiLangDaemonConfiguration configuration) {
        this.command = command;
        this.commandArray = command.split(COMMAND_DELIMETER_REGEX);
        this.executorService = executorService;
        this.objectMapper = objectMapper;
        this.configuration = configuration;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        log.debug("Creating new record processor for client executable: {}", command);
        /*
         * Giving ProcessBuilder the command as an array of Strings allows users to specify command line arguments.
         */
        return new MultiLangShardRecordProcessor(
                new ProcessBuilder(commandArray), executorService, this.objectMapper, this.configuration);
    }

    String[] getCommandArray() {
        return commandArray;
    }
}
