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

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import software.amazon.kinesis.lifecycle.ShutdownReason;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import com.amazonaws.services.kinesis.multilang.messages.CheckpointMessage;
import com.amazonaws.services.kinesis.multilang.messages.InitializeMessage;
import com.amazonaws.services.kinesis.multilang.messages.Message;
import com.amazonaws.services.kinesis.multilang.messages.ProcessRecordsMessage;
import com.amazonaws.services.kinesis.multilang.messages.ShutdownMessage;
import com.amazonaws.services.kinesis.multilang.messages.ShutdownRequestedMessage;
import com.amazonaws.services.kinesis.multilang.messages.StatusMessage;

import lombok.extern.slf4j.Slf4j;

/**
 * An implementation of the multi language protocol.
 */
@Slf4j
class MultiLangProtocol {

    private MessageReader messageReader;
    private MessageWriter messageWriter;
    private final InitializationInput initializationInput;
    private KinesisClientLibConfiguration configuration;

    /**
     * Constructor.
     *
     * @param messageReader
     *            A message reader.
     * @param messageWriter
     *            A message writer.
     * @param initializationInput
     *            information about the shard this processor is starting to process
     */
    MultiLangProtocol(MessageReader messageReader, MessageWriter messageWriter,
            InitializationInput initializationInput, KinesisClientLibConfiguration configuration) {
        this.messageReader = messageReader;
        this.messageWriter = messageWriter;
        this.initializationInput = initializationInput;
        this.configuration = configuration;
    }

    /**
     * Writes an {@link InitializeMessage} to the child process's STDIN and waits for the child process to respond with
     * a {@link StatusMessage} on its STDOUT.
     *
     * @return Whether or not this operation succeeded.
     */
    boolean initialize() {
        /*
         * Call and response to child process.
         */
        Future<Boolean> writeFuture = messageWriter.writeInitializeMessage(initializationInput);
        return waitForStatusMessage(InitializeMessage.ACTION, null, writeFuture);

    }

    /**
     * Writes a {@link ProcessRecordsMessage} to the child process's STDIN and waits for the child process to respond
     * with a {@link StatusMessage} on its STDOUT.
     *
     * @param processRecordsInput
     *            The records, and associated metadata, to process.
     * @return Whether or not this operation succeeded.
     */
    boolean processRecords(ProcessRecordsInput processRecordsInput) {
        Future<Boolean> writeFuture = messageWriter.writeProcessRecordsMessage(processRecordsInput);
        return waitForStatusMessage(ProcessRecordsMessage.ACTION, processRecordsInput.checkpointer(), writeFuture);
    }

    /**
     * Writes a {@link ShutdownMessage} to the child process's STDIN and waits for the child process to respond with a
     * {@link StatusMessage} on its STDOUT.
     *
     * @param checkpointer A checkpointer.
     * @param reason Why this processor is being shutdown.
     * @return Whether or not this operation succeeded.
     */
    boolean shutdown(RecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        Future<Boolean> writeFuture = messageWriter.writeShutdownMessage(reason);
        return waitForStatusMessage(ShutdownMessage.ACTION, checkpointer, writeFuture);
    }

    /**
     * Writes a {@link ShutdownRequestedMessage} to the child process's STDIN and waits for the child process to respond with a
     * {@link StatusMessage} on its STDOUT.
     *
     * @param checkpointer A checkpointer.
     * @return Whether or not this operation succeeded.
     */
    boolean shutdownRequested(RecordProcessorCheckpointer checkpointer) {
        Future<Boolean> writeFuture = messageWriter.writeShutdownRequestedMessage();
        return waitForStatusMessage(ShutdownRequestedMessage.ACTION, checkpointer, writeFuture);
    }

    /**
     * Waits for a {@link StatusMessage} for a particular action. If a {@link CheckpointMessage} is received, then this
     * method will attempt to checkpoint with the provided {@link RecordProcessorCheckpointer}. This method returns
     * true if writing to the child process succeeds and the status message received back was for the correct action and
     * all communications with the child process regarding checkpointing were successful. Note that whether or not the
     * checkpointing itself was successful is not the concern of this method. This method simply cares whether it was
     * able to successfully communicate the results of its attempts to checkpoint.
     *
     * @param action
     *            What action is being waited on.
     * @param checkpointer
     *            the checkpointer from the process records, or shutdown request
     * @param writeFuture
     *            The writing task.
     * @return Whether or not this operation succeeded.
     */
    private boolean waitForStatusMessage(String action, RecordProcessorCheckpointer checkpointer,
            Future<Boolean> writeFuture) {
        boolean statusWasCorrect = waitForStatusMessage(action, checkpointer);

        // Examine whether or not we failed somewhere along the line.
        try {
            boolean writerIsStillOpen = writeFuture.get();
            return statusWasCorrect && writerIsStillOpen;
        } catch (InterruptedException e) {
            log.error("Interrupted while writing {} message for shard {}", action, initializationInput.shardId());
            return false;
        } catch (ExecutionException e) {
            log.error("Failed to write {} message for shard {}", action, initializationInput.shardId(), e);
            return false;
        }
    }

    /**
     * Waits for status message and verifies it against the expectation
     *
     * @param action
     *            What action is being waited on.
     * @param checkpointer
     *            the original process records request
     * @return Whether or not this operation succeeded.
     */
    boolean waitForStatusMessage(String action, RecordProcessorCheckpointer checkpointer) {
        Optional<StatusMessage> statusMessage = Optional.empty();
        while (!statusMessage.isPresent()) {
            Future<Message> future = this.messageReader.getNextMessageFromSTDOUT();
            Optional<Message> message = configuration.getTimeoutInSeconds()
            .map(second -> futureMethod(() -> future.get(second, TimeUnit.SECONDS), action))
            .orElse(futureMethod(future::get, action));

            if (!message.isPresent()) {
                return false;
            }

            Optional<Boolean> checkpointFailed = message.filter(m -> m instanceof CheckpointMessage )
            .map(m -> (CheckpointMessage) m)
            .flatMap(m -> futureMethod(() -> checkpoint(m, checkpointer).get(), "Checkpoint"))
            .map(checkpointSuccess -> !checkpointSuccess);

            if (checkpointFailed.orElse(false)) {
                return false;
            }

            statusMessage = message.filter(m -> m instanceof StatusMessage).map(m -> (StatusMessage) m );
        }
        return this.validateStatusMessage(statusMessage.get(), action);
    }

    private interface FutureMethod<T> {
        T get() throws InterruptedException, TimeoutException, ExecutionException;
    }

    private <T> Optional<T> futureMethod(FutureMethod<T> fm, String action) {
        try {
            return Optional.of(fm.get());
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for {} message for shard {}", action,
                    initializationInput.shardId(), e);
        } catch (ExecutionException e) {
            log.error("Failed to get status message for {} action for shard {}", action,
                    initializationInput.shardId(), e);
        } catch (TimeoutException e) {
            log.error("Timedout to get status message for {} action for shard {}. Terminating...",
                    action,
                    initializationInput.shardId(),
                    e);
            haltJvm(1);
        }
        return Optional.empty();
    }

    /**
     * This method is used to halt the JVM. Use this method with utmost caution, since this method will kill the JVM
     * without calling the Shutdown hooks.
     *
     * @param exitStatus The exit status with which the JVM is to be halted.
     */
    protected void haltJvm(int exitStatus) {
        Runtime.getRuntime().halt(exitStatus);
    }

    /**
     * Utility for confirming that the status message is for the provided action.
     *
     * @param statusMessage The status of the child process.
     * @param action The action that was being waited on.
     * @return Whether or not this operation succeeded.
     */
    private boolean validateStatusMessage(StatusMessage statusMessage, String action) {
        log.info("Received response {} from subprocess while waiting for {}"
                + " while processing shard {}", statusMessage, action,  initializationInput.shardId());
        return !(statusMessage == null || statusMessage.getResponseFor() == null || !statusMessage.getResponseFor()
                .equals(action));

    }

    /**
     * Attempts to checkpoint with the provided {@link RecordProcessorCheckpointer} at the sequence number in the
     * provided {@link CheckpointMessage}. If no sequence number is provided, i.e. the sequence number is null, then
     * this method will call {@link RecordProcessorCheckpointer#checkpoint()}. The method returns a future representing
     * the attempt to write the result of this checkpoint attempt to the child process.
     *
     * @param checkpointMessage A checkpoint message.
     * @param checkpointer A checkpointer.
     * @return Whether or not this operation succeeded.
     */
    private Future<Boolean> checkpoint(CheckpointMessage checkpointMessage, RecordProcessorCheckpointer checkpointer) {
        String sequenceNumber = checkpointMessage.getSequenceNumber();
        Long subSequenceNumber = checkpointMessage.getSubSequenceNumber();
        try {
            if (checkpointer != null) {
                log.debug(logCheckpointMessage(sequenceNumber, subSequenceNumber));
                if (sequenceNumber != null) {
                    if (subSequenceNumber != null) {
                        checkpointer.checkpoint(sequenceNumber, subSequenceNumber);
                    } else {
                        checkpointer.checkpoint(sequenceNumber);
                    }
                } else {
                    checkpointer.checkpoint();
                }
                return this.messageWriter.writeCheckpointMessageWithError(sequenceNumber, subSequenceNumber, null);
            } else {
                String message =
                        String.format("Was asked to checkpoint at %s but no checkpointer was provided for shard %s",
                                sequenceNumber, initializationInput.shardId());
                log.error(message);
                return this.messageWriter.writeCheckpointMessageWithError(sequenceNumber, subSequenceNumber,
                        new InvalidStateException(
                        message));
            }
        } catch (Throwable t) {
            return this.messageWriter.writeCheckpointMessageWithError(sequenceNumber, subSequenceNumber, t);
        }
    }

    private String logCheckpointMessage(String sequenceNumber, Long subSequenceNumber) {
        return String.format("Attempting to checkpoint shard %s @ sequence number %s, and sub sequence number %s",
                initializationInput.shardId(), sequenceNumber, subSequenceNumber);
    }

}
