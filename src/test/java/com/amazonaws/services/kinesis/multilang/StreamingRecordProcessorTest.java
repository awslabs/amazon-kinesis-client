/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.multilang.messages.InitializeMessage;
import com.amazonaws.services.kinesis.multilang.messages.ProcessRecordsMessage;
import com.amazonaws.services.kinesis.multilang.messages.ShutdownMessage;
import com.amazonaws.services.kinesis.multilang.messages.StatusMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StreamingRecordProcessorTest {

    private static final String shardId = "shard-123";

    private int systemExitCount = 0;

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
    };

    private MessageWriter messageWriter;

    private DrainChildSTDERRTask errorReader;

    private MessageReader messageReader;

    private MultiLangRecordProcessor recordProcessor;

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

        recordProcessor =
                new MultiLangRecordProcessor(new ProcessBuilder(), executor, new ObjectMapper(), messageWriter,
                        messageReader, errorReader) {

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

        Mockito.doReturn(trueFuture).when(messageWriter).writeInitializeMessage(Mockito.anyString());
        Mockito.doReturn(trueFuture).when(messageWriter)
                .writeCheckpointMessageWithError(Mockito.anyString(), Mockito.any(Throwable.class));
        Mockito.doReturn(trueFuture).when(messageWriter).writeProcessRecordsMessage(Mockito.anyList());
        Mockito.doReturn(trueFuture).when(messageWriter).writeShutdownMessage(Mockito.any(ShutdownReason.class));
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
        Future<StatusMessage> future = Mockito.mock(Future.class);
        Mockito.doAnswer(answer).when(future).get();
        Mockito.doReturn(future).when(messageReader).getNextMessageFromSTDOUT();

        List<Record> testRecords = new ArrayList<Record>();

        recordProcessor.initialize(shardId);
        recordProcessor.processRecords(testRecords, unimplementedCheckpointer);
        recordProcessor.processRecords(testRecords, unimplementedCheckpointer);
        recordProcessor.shutdown(unimplementedCheckpointer, ShutdownReason.ZOMBIE);
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

        Mockito.verify(messageWriter, Mockito.times(1)).writeInitializeMessage(shardId);
        Mockito.verify(messageWriter, Mockito.times(2)).writeProcessRecordsMessage(Mockito.anyList());
        Mockito.verify(messageWriter, Mockito.times(1)).writeShutdownMessage(ShutdownReason.ZOMBIE);
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

        Mockito.verify(messageWriter, Mockito.times(1)).writeInitializeMessage(shardId);
        Mockito.verify(messageWriter, Mockito.times(2)).writeProcessRecordsMessage(Mockito.anyList());
        Mockito.verify(messageWriter, Mockito.times(0)).writeShutdownMessage(ShutdownReason.ZOMBIE);
        Assert.assertEquals(1, systemExitCount);
    }
}
