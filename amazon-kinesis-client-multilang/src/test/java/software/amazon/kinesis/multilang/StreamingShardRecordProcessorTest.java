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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.multilang.config.MultiLangDaemonConfiguration;
import software.amazon.kinesis.multilang.messages.InitializeMessage;
import software.amazon.kinesis.multilang.messages.Message;
import software.amazon.kinesis.multilang.messages.ProcessRecordsMessage;
import software.amazon.kinesis.multilang.messages.ShutdownMessage;
import software.amazon.kinesis.multilang.messages.StatusMessage;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.PreparedCheckpointer;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

@RunWith(MockitoJUnitRunner.class)
public class StreamingShardRecordProcessorTest {

    private static final String SHARD_ID = "shard-123";

    private int systemExitCount = 0;

    @Mock
    private Future<Message> messageFuture;
    @Mock
    private Future<Boolean> trueFuture;

    private RecordProcessorCheckpointer unimplementedCheckpointer = new RecordProcessorCheckpointer() {

        @Override
        public void checkpoint() throws KinesisClientLibDependencyException, ThrottlingException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkpoint(String sequenceNumber) throws KinesisClientLibDependencyException,
                ThrottlingException, IllegalArgumentException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkpoint(Record record)
                throws KinesisClientLibDependencyException, ThrottlingException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkpoint(String sequenceNumber, long subSequenceNumber)
                throws KinesisClientLibDependencyException, ThrottlingException, IllegalArgumentException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint()
                throws KinesisClientLibDependencyException, ThrottlingException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(byte[] applicationState)
                throws KinesisClientLibDependencyException, ThrottlingException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(Record record)
                throws KinesisClientLibDependencyException, ThrottlingException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(Record record, byte[] applicationState)
                throws KinesisClientLibDependencyException, ThrottlingException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(String sequenceNumber)
                throws KinesisClientLibDependencyException, ThrottlingException, IllegalArgumentException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(String sequenceNumber, byte[] applicationState)
                throws KinesisClientLibDependencyException, ThrottlingException, IllegalArgumentException {
            return null;
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(String sequenceNumber, long subSequenceNumber)
                throws KinesisClientLibDependencyException, ThrottlingException, IllegalArgumentException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(String sequenceNumber, long subSequenceNumber, byte[] applicationState)
                throws KinesisClientLibDependencyException, ThrottlingException, IllegalArgumentException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Checkpointer checkpointer() {
            throw new UnsupportedOperationException();
        }
    };

    private MessageWriter messageWriter;

    private DrainChildSTDERRTask errorReader;

    private MessageReader messageReader;

    private MultiLangShardRecordProcessor recordProcessor;

    @Mock
    private MultiLangDaemonConfiguration configuration;

    @Before
    public void prepare() throws InterruptedException, ExecutionException {
        // Fake command
        systemExitCount = 0;

        // Mocks
        ExecutorService executor = Executors.newFixedThreadPool(3);
        final Process process = Mockito.mock(Process.class);

        messageWriter = Mockito.mock(MessageWriter.class);
        messageReader = Mockito.mock(MessageReader.class);
        errorReader = Mockito.mock(DrainChildSTDERRTask.class);
        when(configuration.getTimeoutInSeconds()).thenReturn(null);

        recordProcessor =
                new MultiLangShardRecordProcessor(new ProcessBuilder(), executor, new ObjectMapper(), messageWriter,
                        messageReader, errorReader, configuration) {

                    // Just don't do anything when we exit.
                    void exit() {
                        systemExitCount += 1;
                    }

                    // Inject our mock process
                    Process startProcess() {
                        return process;
                    }
                };

        // Our process will return mock streams
        InputStream inputStream = Mockito.mock(InputStream.class);
        InputStream errorStream = Mockito.mock(InputStream.class);
        OutputStream outputStream = Mockito.mock(OutputStream.class);
        Mockito.doReturn(inputStream).when(process).getInputStream();
        Mockito.doReturn(errorStream).when(process).getErrorStream();
        Mockito.doReturn(outputStream).when(process).getOutputStream();

        Mockito.doReturn(Mockito.mock(Future.class)).when(messageReader).drainSTDOUT();
        Mockito.doReturn(true).when(trueFuture).get();

        when(messageWriter.writeInitializeMessage(any(InitializationInput.class))).thenReturn(trueFuture);
        when(messageWriter.writeCheckpointMessageWithError(anyString(), anyLong(), any(Throwable.class))).thenReturn(trueFuture);
        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class))).thenReturn(trueFuture);
        when(messageWriter.writeLeaseLossMessage(any(LeaseLostInput.class))).thenReturn(trueFuture);
    }

    private void phases(Answer<StatusMessage> answer) throws InterruptedException, ExecutionException {
        /*
         * Return a status message for each call
         * Plan is:
         * initialize
         * processRecords
         * processRecords
         * shutdown
         */
        when(messageFuture.get()).thenAnswer(answer);
        when(messageReader.getNextMessageFromSTDOUT()).thenReturn(messageFuture);

        List<KinesisClientRecord> testRecords = Collections.emptyList();

        recordProcessor.initialize(InitializationInput.builder().shardId(SHARD_ID).build());
        recordProcessor.processRecords(ProcessRecordsInput.builder().records(testRecords)
                .checkpointer(unimplementedCheckpointer).build());
        recordProcessor.processRecords(ProcessRecordsInput.builder().records(testRecords)
                .checkpointer(unimplementedCheckpointer).build());
        recordProcessor.leaseLost(LeaseLostInput.builder().build());
    }

    @Test
    public void processorPhasesTest() throws InterruptedException, ExecutionException {
        Answer<StatusMessage> answer = new Answer<StatusMessage>() {

            StatusMessage[] answers = new StatusMessage[] { new StatusMessage(InitializeMessage.ACTION),
                    new StatusMessage(ProcessRecordsMessage.ACTION), new StatusMessage(ProcessRecordsMessage.ACTION),
                    new StatusMessage(ShutdownMessage.ACTION) };

            int callCount = 0;

            @Override
            public StatusMessage answer(InvocationOnMock invocation) throws Throwable {
                if (callCount < answers.length) {
                    return answers[callCount++];
                } else {
                    throw new Throwable("Too many calls to getNextStatusMessage");
                }
            }
        };

        phases(answer);

        verify(messageWriter)
                .writeInitializeMessage(argThat(Matchers.withInit(
                        InitializationInput.builder().shardId(SHARD_ID).build())));
        verify(messageWriter, times(2)).writeProcessRecordsMessage(any(ProcessRecordsInput.class));
        verify(messageWriter).writeLeaseLossMessage(any(LeaseLostInput.class));
    }

    @Test
    public void initFailsTest() throws InterruptedException, ExecutionException {
        Answer<StatusMessage> answer = new Answer<StatusMessage>() {

            /*
             * This bad message will cause shutdown to not attempt to send a message. i.e. avoid encountering an
             * exception.
             */
            StatusMessage[] answers = new StatusMessage[] { new StatusMessage("Bad"),
                    new StatusMessage(ProcessRecordsMessage.ACTION), new StatusMessage(ProcessRecordsMessage.ACTION),
                    new StatusMessage(ShutdownMessage.ACTION) };

            int callCount = 0;

            @Override
            public StatusMessage answer(InvocationOnMock invocation) throws Throwable {
                if (callCount < answers.length) {
                    return answers[callCount++];
                } else {
                    throw new Throwable("Too many calls to getNextStatusMessage");
                }
            }
        };

        phases(answer);

        verify(messageWriter).writeInitializeMessage(argThat(Matchers.withInit(InitializationInput.builder()
                .shardId(SHARD_ID).build())));
        verify(messageWriter, times(2)).writeProcessRecordsMessage(any(ProcessRecordsInput.class));
        verify(messageWriter, never()).writeLeaseLossMessage(any(LeaseLostInput.class));
        Assert.assertEquals(1, systemExitCount);
    }
}
