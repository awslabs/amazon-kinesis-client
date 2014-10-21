/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.multilang;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.multilang.messages.CheckpointMessage;
import com.amazonaws.services.kinesis.multilang.messages.InitializeMessage;
import com.amazonaws.services.kinesis.multilang.messages.Message;
import com.amazonaws.services.kinesis.multilang.messages.ProcessRecordsMessage;
import com.amazonaws.services.kinesis.multilang.messages.ShutdownMessage;
import com.amazonaws.services.kinesis.multilang.messages.StatusMessage;

/**
 * An implementation of the multi language protocol.
 */
class MultiLangProtocol {

    private static final Log LOG = LogFactory.getLog(MultiLangProtocol.class);

    private MessageReader messageReader;
    private MessageWriter messageWriter;
    private String shardId;

    /**
     * Constructor.
     * 
     * @param messageReader A message reader.
     * @param messageWriter A message writer.
     * @param shardId The shard id this processor is associated with.
     */
    MultiLangProtocol(MessageReader messageReader, MessageWriter messageWriter, String shardId) {
        this.messageReader = messageReader;
        this.messageWriter = messageWriter;
        this.shardId = shardId;
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
        Future<Boolean> writeFuture = messageWriter.writeInitializeMessage(shardId);
        return waitForStatusMessage(InitializeMessage.ACTION, null, writeFuture);

    }

    /**
     * Writes a {@link ProcessRecordsMessage} to the child process's STDIN and waits for the child process to respond
     * with a {@link StatusMessage} on its STDOUT.
     * 
     * @param records The records to process.
     * @param checkpointer A checkpointer.
     * @return Whether or not this operation succeeded.
     */
    boolean processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        Future<Boolean> writeFuture = messageWriter.writeProcessRecordsMessage(records);
        return waitForStatusMessage(ProcessRecordsMessage.ACTION, checkpointer, writeFuture);
    }

    /**
     * Writes a {@link ShutdownMessage} to the child process's STDIN and waits for the child process to respond with a
     * {@link StatusMessage} on its STDOUT.
     * 
     * @param checkpointer A checkpointer.
     * @param reason Why this processor is being shutdown.
     * @return Whether or not this operation succeeded.
     */
    boolean shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        Future<Boolean> writeFuture = messageWriter.writeShutdownMessage(reason);
        return waitForStatusMessage(ShutdownMessage.ACTION, checkpointer, writeFuture);
    }

    /**
     * Waits for a {@link StatusMessage} for a particular action. If a {@link CheckpointMessage} is received, then this
     * method will attempt to checkpoint with the provided {@link IRecordProcessorCheckpointer}. This method returns
     * true if writing to the child process succeeds and the status message received back was for the correct action and
     * all communications with the child process regarding checkpointing were successful. Note that whether or not the
     * checkpointing itself was successful is not the concern of this method. This method simply cares whether it was
     * able to successfully communicate the results of its attempts to checkpoint.
     * 
     * @param action What action is being waited on.
     * @param checkpointer A checkpointer.
     * @param writeFuture The writing task.
     * @return Whether or not this operation succeeded.
     */
    private boolean waitForStatusMessage(String action,
                                         IRecordProcessorCheckpointer checkpointer,
                                         Future<Boolean> writeFuture) {
        boolean statusWasCorrect = waitForStatusMessage(action, checkpointer);

        // Examine whether or not we failed somewhere along the line.
        try {
            boolean writerIsStillOpen = Boolean.valueOf(writeFuture.get());
            return statusWasCorrect && writerIsStillOpen;
        } catch (InterruptedException e) {
            LOG.error(String.format("Interrupted while writing %s message for shard %s", action, shardId));
            return false;
        } catch (ExecutionException e) {
            LOG.error(String.format("Failed to write %s message for shard %s", action, shardId), e);
            return false;
        }
    }

    /**
     * @param action What action is being waited on.
     * @param checkpointer A checkpointer.
     * @return Whether or not this operation succeeded.
     */
    private boolean waitForStatusMessage(String action, IRecordProcessorCheckpointer checkpointer) {
        StatusMessage statusMessage = null;
        while (statusMessage == null) {
            Future<Message> future = this.messageReader.getNextMessageFromSTDOUT();
            try {
                Message message = future.get();
                // Note that instanceof doubles as a check against a value being null
                if (message instanceof CheckpointMessage) {
                    boolean checkpointWriteSucceeded =
                            Boolean.valueOf(checkpoint((CheckpointMessage) message, checkpointer).get());
                    if (!checkpointWriteSucceeded) {
                        return false;
                    }
                } else if (message instanceof StatusMessage) {
                    statusMessage = (StatusMessage) message;
                }
            } catch (InterruptedException e) {
                LOG.error(String.format("Interrupted while waiting for %s message for shard %s", action, shardId));
                return false;
            } catch (ExecutionException e) {
                LOG.error(String.format("Failed to get status message for %s action for shard %s", action, shardId), e);
                return false;
            }
        }
        return this.validateStatusMessage(statusMessage, action);
    }

    /**
     * Utility for confirming that the status message is for the provided action.
     * 
     * @param statusMessage The status of the child process.
     * @param action The action that was being waited on.
     * @return Whether or not this operation succeeded.
     */
    private boolean validateStatusMessage(StatusMessage statusMessage, String action) {
        LOG.info("Received response " + statusMessage + " from subprocess while waiting for " + action
                + " while processing shard " + shardId);
        return !(statusMessage == null || statusMessage.getResponseFor() == null || !statusMessage.getResponseFor()
                .equals(action));

    }

    /**
     * Attempts to checkpoint with the provided {@link IRecordProcessorCheckpointer} at the sequence number in the
     * provided {@link CheckpointMessage}. If no sequence number is provided, i.e. the sequence number is null, then
     * this method will call {@link IRecordProcessorCheckpointer#checkpoint()}. The method returns a future representing
     * the attempt to write the result of this checkpoint attempt to the child process.
     * 
     * @param checkpointMessage A checkpoint message.
     * @param checkpointer A checkpointer.
     * @return Whether or not this operation succeeded.
     */
    private Future<Boolean> checkpoint(CheckpointMessage checkpointMessage, IRecordProcessorCheckpointer checkpointer) {
        String sequenceNumber = checkpointMessage.getCheckpoint();
        try {
            if (checkpointer != null) {
                if (sequenceNumber == null) {
                    LOG.info(String.format("Attempting to checkpoint for shard %s", shardId));
                    checkpointer.checkpoint();
                } else {
                    LOG.info(String.format("Attempting to checkpoint at sequence number %s for shard %s",
                            sequenceNumber, shardId));
                    checkpointer.checkpoint(sequenceNumber);
                }
                return this.messageWriter.writeCheckpointMessageWithError(sequenceNumber, null);
            } else {
                String message =
                        String.format("Was asked to checkpoint at %s but no checkpointer was provided for shard %s",
                                sequenceNumber, shardId);
                LOG.error(message);
                return this.messageWriter.writeCheckpointMessageWithError(sequenceNumber, new InvalidStateException(
                        message));
            }
        } catch (Throwable t) {
            return this.messageWriter.writeCheckpointMessageWithError(sequenceNumber, t);
        }
    }
}
