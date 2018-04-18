/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.multilang;

import java.util.concurrent.ExecutorService;

import software.amazon.kinesis.processor.RecordProcessor;
import software.amazon.kinesis.processor.RecordProcessorFactory;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * Creates {@link MultiLangRecordProcessor}'s.
 */
@Slf4j
public class MultiLangRecordProcessorFactory implements RecordProcessorFactory {
    private static final String COMMAND_DELIMETER_REGEX = " +";

    private final String command;
    private final String[] commandArray;

    private final ObjectMapper objectMapper;

    private final ExecutorService executorService;

    private final KinesisClientLibConfiguration configuration;

    /**
     * @param command The command that will do processing for this factory's record processors.
     * @param executorService An executor service to use while processing inputs and outputs of the child process.
     */
    public MultiLangRecordProcessorFactory(String command, ExecutorService executorService,
                                           KinesisClientLibConfiguration configuration) {
        this(command, executorService, new ObjectMapper(), configuration);
    }

    /**
     * @param command The command that will do processing for this factory's record processors.
     * @param executorService An executor service to use while processing inputs and outputs of the child process.
     * @param objectMapper An object mapper used to convert messages to json to be written to the child process
     */
    public MultiLangRecordProcessorFactory(String command, ExecutorService executorService, ObjectMapper objectMapper,
                                           KinesisClientLibConfiguration configuration) {
        this.command = command;
        this.commandArray = command.split(COMMAND_DELIMETER_REGEX);
        this.executorService = executorService;
        this.objectMapper = objectMapper;
        this.configuration = configuration;
    }

    @Override
    public RecordProcessor createProcessor() {
        log.debug("Creating new record processor for client executable: {}", command);
        /*
         * Giving ProcessBuilder the command as an array of Strings allows users to specify command line arguments.
         */
        return new MultiLangRecordProcessor(new ProcessBuilder(commandArray), executorService, this.objectMapper,
                this.configuration);
    }

    String[] getCommandArray() {
        return commandArray;
    }
}
