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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import software.amazon.kinesis.processor.IRecordProcessorCheckpointer;
import software.amazon.kinesis.processor.RecordProcessor;
import software.amazon.kinesis.processor.ShutdownNotificationAware;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import software.amazon.kinesis.lifecycle.InitializationInput;
import software.amazon.kinesis.lifecycle.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.ShutdownInput;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;


/**
 * A record processor that manages creating a child process that implements the multi language protocol and connecting
 * that child process's input and outputs to a {@link MultiLangProtocol} object and calling the appropriate methods on
 * that object when its corresponding {@link #initialize}, {@link #processRecords}, and {@link #shutdown} methods are
 * called.
 */
@Slf4j
public class MultiLangRecordProcessor implements RecordProcessor, ShutdownNotificationAware {
    private static final int EXIT_VALUE = 1;

    /** Whether or not record processor initialization is successful. Defaults to false. */
    private volatile boolean initialized;

    private String shardId;

    private Future<?> stderrReadTask;

    private MessageWriter messageWriter;
    private MessageReader messageReader;
    private DrainChildSTDERRTask readSTDERRTask;

    private ProcessBuilder processBuilder;
    private Process process;
    private ExecutorService executorService;
    private ProcessState state;

    private ObjectMapper objectMapper;

    private MultiLangProtocol protocol;

    private KinesisClientLibConfiguration configuration;

    @Override
    public void initialize(InitializationInput initializationInput) {
        try {
            this.shardId = initializationInput.getShardId();
            try {
                this.process = startProcess();
            } catch (IOException e) {
                /*
                 * The process builder has thrown an exception while starting the child process so we would like to shut
                 * down
                 */
                throw new IOException("Failed to start client executable", e);
            }
            // Initialize all of our utility objects that will handle interacting with the process over
            // STDIN/STDOUT/STDERR
            messageWriter.initialize(process.getOutputStream(), shardId, objectMapper, executorService);
            messageReader.initialize(process.getInputStream(), shardId, objectMapper, executorService);
            readSTDERRTask.initialize(process.getErrorStream(), shardId, "Reading STDERR for " + shardId);

            // Submit the error reader for execution
            stderrReadTask = executorService.submit(readSTDERRTask);

            protocol = new MultiLangProtocol(messageReader, messageWriter, initializationInput, configuration);
            if (!protocol.initialize()) {
                throw new RuntimeException("Failed to initialize child process");
            }

            initialized = true;
        } catch (Throwable t) {
            // Any exception in initialize results in MultiLangDaemon shutdown.
            stopProcessing("Encountered an error while trying to initialize record processor", t);
        }
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        try {
            if (!protocol.processRecords(processRecordsInput)) {
                throw new RuntimeException("Child process failed to process records");
            }
        } catch (Throwable t) {
            stopProcessing("Encountered an error while trying to process records", t);
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        // In cases where KCL loses lease for the shard after creating record processor instance but before
        // record processor initialize() is called, then shutdown() may be called directly before initialize().
        if (!initialized) {
            log.info("Record processor was not initialized and will not have a child process, "
                    + "so not invoking child process shutdown.");
            this.state = ProcessState.SHUTDOWN;
            return;
        }

        try {
            if (ProcessState.ACTIVE.equals(this.state)) {
                if (!protocol.shutdown(shutdownInput.checkpointer(), shutdownInput.shutdownReason())) {
                    throw new RuntimeException("Child process failed to shutdown");
                }

                childProcessShutdownSequence();
            } else {
                log.warn("Shutdown was called but this processor is already shutdown. Not doing anything.");
            }
        } catch (Throwable t) {
            if (ProcessState.ACTIVE.equals(this.state)) {
                stopProcessing("Encountered an error while trying to shutdown child process", t);
            } else {
                stopProcessing("Encountered an error during shutdown,"
                        + " but it appears the processor has already been shutdown", t);
            }
        }
    }

    @Override
    public void shutdownRequested(IRecordProcessorCheckpointer checkpointer) {
        log.info("Shutdown is requested.");
        if (!initialized) {
            log.info("Record processor was not initialized so no need to initiate a final checkpoint.");
            return;
        }
        log.info("Requesting a checkpoint on shutdown notification.");
        if (!protocol.shutdownRequested(checkpointer)) {
            log.error("Child process failed to complete shutdown notification.");
        }
    }


    /**
     * Used to tell whether the processor has been shutdown already.
     */
    private enum ProcessState {
        ACTIVE, SHUTDOWN
    }

    /**
     * Constructor.
     * 
     * @param processBuilder
     *            Provides process builder functionality.
     * @param executorService
     *            An executor
     * @param objectMapper
     *            An obejct mapper.
     */
    MultiLangRecordProcessor(ProcessBuilder processBuilder, ExecutorService executorService,
            ObjectMapper objectMapper, KinesisClientLibConfiguration configuration) {
        this(processBuilder, executorService, objectMapper, new MessageWriter(), new MessageReader(),
                new DrainChildSTDERRTask(), configuration);
    }

    /**
     * Note: This constructor has package level access solely for testing purposes.
     * 
     * @param processBuilder
     *            Provides the child process for this record processor
     * @param executorService
     *            The executor service which is provided by the {@link MultiLangRecordProcessorFactory}
     * @param objectMapper
     *            Object mapper
     * @param messageWriter
     *            Message write to write to child process's stdin
     * @param messageReader
     *            Message reader to read from child process's stdout
     * @param readSTDERRTask
     *            Error reader to read from child process's stderr
     */
    MultiLangRecordProcessor(ProcessBuilder processBuilder, ExecutorService executorService, ObjectMapper objectMapper,
                             MessageWriter messageWriter, MessageReader messageReader, DrainChildSTDERRTask readSTDERRTask,
                             KinesisClientLibConfiguration configuration) {
        this.executorService = executorService;
        this.processBuilder = processBuilder;
        this.objectMapper = objectMapper;
        this.messageWriter = messageWriter;
        this.messageReader = messageReader;
        this.readSTDERRTask = readSTDERRTask;
        this.configuration = configuration;


        this.state = ProcessState.ACTIVE;
    }

    /**
     * Performs the necessary shutdown actions for the child process, e.g. stopping all the handlers after they have
     * drained their streams. Attempts to wait for child process to completely finish before returning.
     */
    private void childProcessShutdownSequence() {
        try {
            /*
             * Close output stream to the child process. The child process should be reading off its stdin until it
             * receives EOF, closing the output stream should signal this and allow the child process to terminate. We
             * expect it to terminate immediately, but there is the possibility that the child process then begins to
             * write to its STDOUT and STDERR.
             */
            if (messageWriter.isOpen()) {
                messageWriter.close();
            }
        } catch (IOException e) {
            log.error("Encountered exception while trying to close output stream.", e);
        }

        // We should drain the STDOUT and STDERR of the child process. If we don't, the child process might remain
        // blocked writing to a full pipe buffer.
        safelyWaitOnFuture(messageReader.drainSTDOUT(), "draining STDOUT");
        safelyWaitOnFuture(stderrReadTask, "draining STDERR");

        safelyCloseInputStream(process.getErrorStream(), "STDERR");
        safelyCloseInputStream(process.getInputStream(), "STDOUT");

        /*
         * By this point the threads handling reading off input streams are done, we do one last thing just to make sure
         * we don't leave the child process running. The process is expected to have exited by now, but we still make
         * sure that it exits before we finish.
         */
        try {
            log.info("Child process exited with value: {}", process.waitFor());
        } catch (InterruptedException e) {
            log.error("Interrupted before process finished exiting. Attempting to kill process.");
            process.destroy();
        }

        state = ProcessState.SHUTDOWN;
    }

    private void safelyCloseInputStream(InputStream inputStream, String name) {
        try {
            inputStream.close();
        } catch (IOException e) {
            log.error("Encountered exception while trying to close {} stream.", name, e);
        }
    }

    /**
     * Convenience method used by {@link #childProcessShutdownSequence()} to drain the STDIN and STDERR of the child
     * process.
     * 
     * @param future A future to wait on.
     * @param whatThisFutureIsDoing What that future is doing while we wait.
     */
    private void safelyWaitOnFuture(Future<?> future, String whatThisFutureIsDoing) {
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Encountered error while {} for shard {}", whatThisFutureIsDoing, shardId, e);
        }
    }

    /**
     * Convenience method for logging and safely shutting down so that we don't throw an exception up to the KCL on
     * accident.
     * 
     * @param message The reason we are stopping processing.
     * @param reason An exception that caused us to want to stop processing.
     */
    private void stopProcessing(String message, Throwable reason) {
        try {
            log.error(message, reason);
            if (!state.equals(ProcessState.SHUTDOWN)) {
                childProcessShutdownSequence();
            }
        } catch (Throwable t) {
            log.error("Encountered error while trying to shutdown", t);
        }
        exit();
    }

    /**
     * We provide a package level method for unit testing this call to exit.
     * 
     * @param val exit value
     */
    void exit() {
        System.exit(EXIT_VALUE);
    }

    /**
     * The {@link ProcessBuilder} class is final so not easily mocked. We wrap the only interaction we have with it in
     * this package level method to permit unit testing.
     * 
     * @return The process started by processBuilder
     * @throws IOException If the process can't be started.
     */
    Process startProcess() throws IOException {
        return this.processBuilder.start();
    }
}
