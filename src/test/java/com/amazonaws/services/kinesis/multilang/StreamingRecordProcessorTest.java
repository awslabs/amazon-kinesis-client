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

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IPreparedCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.multilang.messages.InitializeMessage;
import com.amazonaws.services.kinesis.multilang.messages.Message;
import com.amazonaws.services.kinesis.multilang.messages.ProcessRecordsMessage;
import com.amazonaws.services.kinesis.multilang.messages.ShutdownMessage;
import com.amazonaws.services.kinesis.multilang.messages.StatusMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StreamingRecordProcessorTest {

    private static final String shardId = "shard-123";

    private int systemExitCount = 0;

    @Mock
    private Future<Message> messageFuture;

    private IRecordProcessorCheckpointer unimplementedCheckpointer = new IRecordProcessorCheckpointer() {

        @Override
        public void checkpoint() throws KinesisClientLibDependencyException, InvalidStateException,
            ThrottlingException, ShutdownException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkpoint(String sequenceNumber) throws KinesisClientLibDependencyException,
            InvalidStateException, ThrottlingException, ShutdownException, IllegalArgumentException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkpoint(Record record)
                throws KinesisClientLibDependencyException,
                InvalidStateException, ThrottlingException, ShutdownException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkpoint(String sequenceNumber, long subSequenceNumber)
                throws KinesisClientLibDependencyException,
                InvalidStateException, ThrottlingException, ShutdownException,
                IllegalArgumentException {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPreparedCheckpointer prepareCheckpoint()
                throws KinesisClientLibDependencyException,
                InvalidStateException, ThrottlingException, ShutdownException {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPreparedCheckpointer prepareCheckpoint(Record record)
                throws KinesisClientLibDependencyException,
                InvalidStateException, ThrottlingException, ShutdownException {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPreparedCheckpointer prepareCheckpoint(String sequenceNumber)
                throws KinesisClientLibDependencyException,
                InvalidStateException, ThrottlingException, ShutdownException, IllegalArgumentException {
            throw new UnsupportedOperationException();
        }

        @Override
        public IPreparedCheckpointer prepareCheckpoint(String sequenceNumber, long subSequenceNumber)
                throws KinesisClientLibDependencyException,
                InvalidStateException, ThrottlingException, ShutdownException, IllegalArgumentException {
            throw new UnsupportedOperationException();
        }
    };

    private MessageWriter messageWriter;

    private DrainChildSTDERRTask errorReader;

    private MessageReader messageReader;

    private MultiLangRecordProcessor recordProcessor;

    @Mock
    private KinesisClientLibConfiguration configuration;

    @Before
    public void prepare() throws IOException, InterruptedException, ExecutionException {
        // Fake command
        String command = "derp";
        systemExitCount = 0;

        // Mocks
        ExecutorService executor = Executors.newFixedThreadPool(3);
        final Process process = Mockito.mock(Process.class);

        messageWriter = Mockito.mock(MessageWriter.class);
        messageReader = Mockito.mock(MessageReader.class);
        errorReader = Mockito.mock(DrainChildSTDERRTask.class);
        when(configuration.getTimeoutInSeconds()).thenReturn(Optional.empty());

        recordProcessor =
                new MultiLangRecordProcessor(new ProcessBuilder(), executor, new ObjectMapper(), messageWriter,
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
        Future<Boolean> trueFuture = Mockito.mock(Future.class);
        Mockito.doReturn(true).when(trueFuture).get();

        when(messageWriter.writeInitializeMessage(any(InitializationInput.class))).thenReturn(trueFuture);
        when(messageWriter.writeCheckpointMessageWithError(anyString(), anyLong(), any(Throwable.class))).thenReturn(trueFuture);
        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class))).thenReturn(trueFuture);
        when(messageWriter.writeShutdownMessage(any(ShutdownReason.class))).thenReturn(trueFuture);
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

        List<Record> testRecords = new ArrayList<Record>();

        recordProcessor.initialize(new InitializationInput().withShardId(shardId));
        recordProcessor.processRecords(new ProcessRecordsInput().withRecords(testRecords).withCheckpointer(unimplementedCheckpointer));
        recordProcessor.processRecords(new ProcessRecordsInput().withRecords(testRecords).withCheckpointer(unimplementedCheckpointer));
        recordProcessor.shutdown(new ShutdownInput().withCheckpointer(unimplementedCheckpointer).withShutdownReason(ShutdownReason.ZOMBIE));
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
                .writeInitializeMessage(argThat(Matchers.withInit(new InitializationInput().withShardId(shardId))));
        verify(messageWriter, times(2)).writeProcessRecordsMessage(any(ProcessRecordsInput.class));
        verify(messageWriter).writeShutdownMessage(ShutdownReason.ZOMBIE);
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

        verify(messageWriter).writeInitializeMessage(argThat(Matchers.withInit(new InitializationInput()
                .withShardId(shardId))));
        verify(messageWriter, times(2)).writeProcessRecordsMessage(any(ProcessRecordsInput.class));
        verify(messageWriter, never()).writeShutdownMessage(ShutdownReason.ZOMBIE);
        Assert.assertEquals(1, systemExitCount);
    }
}
