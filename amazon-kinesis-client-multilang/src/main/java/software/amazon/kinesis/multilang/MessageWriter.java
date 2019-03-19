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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.multilang.messages.CheckpointMessage;
import software.amazon.kinesis.multilang.messages.InitializeMessage;
import software.amazon.kinesis.multilang.messages.LeaseLostMessage;
import software.amazon.kinesis.multilang.messages.Message;
import software.amazon.kinesis.multilang.messages.ProcessRecordsMessage;
import software.amazon.kinesis.multilang.messages.ShardEndedMessage;
import software.amazon.kinesis.multilang.messages.ShutdownRequestedMessage;

/**
 * Defines methods for writing {@link Message} objects to the child process's STDIN.
 */
@Slf4j
class MessageWriter {
    private BufferedWriter writer;

    private volatile boolean open = true;

    private String shardId;

    private ObjectMapper objectMapper;

    private ExecutorService executorService;

    /**
     * Use initialize method after construction.
     */
    MessageWriter() {
    }

    /**
     * Writes the message then writes the line separator provided by the system. Flushes each message to guarantee it
     * is delivered as soon as possible to the subprocess.
     * 
     * @param message A message to be written to the subprocess.
     * @return
     * @throws IOException
     */
    private Future<Boolean> writeMessageToOutput(final String message) throws IOException {
        Callable<Boolean> writeMessageToOutputTask = new Callable<Boolean>() {
            public Boolean call() throws Exception {
                try {
                    /*
                     * If the message size exceeds the size of the buffer, the write won't be guaranteed to be atomic,
                     * so we synchronize on the writer to avoid interlaced lines from different calls to this method.
                     */
                    synchronized (writer) {
                        writer.write(message, 0, message.length());
                        writer.write(System.lineSeparator(), 0, System.lineSeparator().length());
                        writer.flush();
                    }
                    log.info("Message size == {} bytes for shard {}", message.getBytes().length, shardId);
                } catch (IOException e) {
                    open = false;
                }
                return open;
            }
        };

        if (open) {
            return this.executorService.submit(writeMessageToOutputTask);
        } else {
            String errorMessage = "Cannot write message " + message + " because writer is closed for shard " + shardId;
            log.info(errorMessage);
            throw new IllegalStateException(errorMessage);
        }
    }

    /**
     * Converts the message to a JSON string and writes it to the subprocess.
     * 
     * @param message A message to be written to the subprocess.
     * @return
     */
    private Future<Boolean> writeMessage(Message message) {
        log.info("Writing {} to child process for shard {}", message.getClass().getSimpleName(), shardId);
        try {
            String jsonText = objectMapper.writeValueAsString(message);
            return writeMessageToOutput(jsonText);
        } catch (IOException e) {
            String errorMessage =
                    String.format("Encountered I/O error while writing %s action to subprocess", message.getClass()
                            .getSimpleName());
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    /**
     * Writes an {@link InitializeMessage} to the subprocess.
     * 
     * @param initializationInput
     *            contains information about the shard being initialized
     */
    Future<Boolean> writeInitializeMessage(InitializationInput initializationInput) {
        return writeMessage(new InitializeMessage(initializationInput));
    }

    /**
     * Writes a {@link ProcessRecordsMessage} message to the subprocess.
     * 
     * @param processRecordsInput
     *            the records, and associated metadata to be processed.
     */
    Future<Boolean> writeProcessRecordsMessage(ProcessRecordsInput processRecordsInput) {
        return writeMessage(new ProcessRecordsMessage(processRecordsInput));
    }

    /**
     * Writes the lease lost message to the sub process.
     * 
     * @param leaseLostInput
     *            the lease lost input. This is currently unused as lease loss doesn't actually have anything in it
     * @return A future that is set when the message has been written.
     */
    Future<Boolean> writeLeaseLossMessage(@SuppressWarnings("unused") LeaseLostInput leaseLostInput) {
        return writeMessage(new LeaseLostMessage());
    }

    /**
     * Writes a message to the sub process indicating that the shard has ended
     * 
     * @param shardEndedInput
     *            the shard end input. This is currently unused as the checkpoint is extracted, and used by the caller.
     * @return A future that is set when the message has been written.
     */
    Future<Boolean> writeShardEndedMessage(@SuppressWarnings("unused") ShardEndedInput shardEndedInput) {
        return writeMessage(new ShardEndedMessage());
    }

    /**
     * Writes a {@link ShutdownRequestedMessage} to the subprocess.
     */
    Future<Boolean> writeShutdownRequestedMessage() {
        return writeMessage(new ShutdownRequestedMessage());
    }

    /**
     * Writes a {@link CheckpointMessage} to the subprocess.
     * 
     * @param sequenceNumber
     *            The sequence number that was checkpointed.
     * @param subSequenceNumber
     *            the sub sequence number to checkpoint at.
     * @param throwable
     *            The exception that was thrown by a checkpoint attempt. Null if one didn't occur.
     */
    Future<Boolean> writeCheckpointMessageWithError(String sequenceNumber, Long subSequenceNumber,
            Throwable throwable) {
        return writeMessage(new CheckpointMessage(sequenceNumber, subSequenceNumber, throwable));
    }

    /**
     * Closes the output stream and prevents further attempts to write.
     * 
     * @throws IOException Thrown when closing the writer fails
     */
    void close() throws IOException {
        open = false;
        this.writer.close();
    }

    boolean isOpen() {
        return this.open;
    }

    /**
     * An initialization method allows us to delay setting the attributes of this class. Some of the attributes,
     * stream and shardId, are not known to the {@link MultiLangRecordProcessorFactory} when it constructs a
     * {@link MultiLangShardRecordProcessor} but are later determined when
     * {@link MultiLangShardRecordProcessor (String)} is called. So we follow a pattern where the attributes are
     * set inside this method instead of the constructor so that this object will be initialized when all its attributes
     * are known to the record processor.
     * 
     * @param stream Used to write messages to the subprocess.
     * @param shardId The shard we're working on.
     * @param objectMapper The object mapper to encode messages.
     * @param executorService An executor service to run tasks in.
     */
    MessageWriter initialize(OutputStream stream,
            String shardId,
            ObjectMapper objectMapper,
            ExecutorService executorService) {
        return this.initialize(new BufferedWriter(new OutputStreamWriter(stream)), shardId, objectMapper,
                executorService);
    }

    /**
     * @param writer Used to write messages to the subprocess.
     * @param shardId The shard we're working on.
     * @param objectMapper The object mapper to encode messages.
     * @param executorService An executor service to run tasks in.
     */
    MessageWriter initialize(BufferedWriter writer,
            String shardId,
            ObjectMapper objectMapper,
            ExecutorService executorService) {
        this.writer = writer;
        this.shardId = shardId;
        this.objectMapper = objectMapper;
        this.executorService = executorService;
        return this;
    }

}
