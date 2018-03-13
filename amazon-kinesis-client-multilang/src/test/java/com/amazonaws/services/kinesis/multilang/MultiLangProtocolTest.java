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
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.multilang.messages.CheckpointMessage;
import com.amazonaws.services.kinesis.multilang.messages.Message;
import com.amazonaws.services.kinesis.multilang.messages.ProcessRecordsMessage;
import com.amazonaws.services.kinesis.multilang.messages.StatusMessage;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MultiLangProtocolTest {

    private static final List<Record> EMPTY_RECORD_LIST = Collections.emptyList();
    @Mock
    private MultiLangProtocol protocol;
    @Mock
    private MessageWriter messageWriter;
    @Mock
    private MessageReader messageReader;
    private String shardId;
    @Mock
    private IRecordProcessorCheckpointer checkpointer;
    @Mock
    private KinesisClientLibConfiguration configuration;



    @Before
    public void setup() {
        this.shardId = "shard-id-123";
        protocol = new MultiLangProtocolForTesting(messageReader, messageWriter,
         new InitializationInput().withShardId(shardId), configuration);

        when(configuration.getTimeoutInSeconds()).thenReturn(Optional.empty());
    }

    private <T> Future<T> buildFuture(T value) {
        SettableFuture<T> future = SettableFuture.create();
        future.set(value);
        return future;
    }

    private <T> Future<T> buildFuture(T value, Class<T> clazz) {
        SettableFuture<T> future = SettableFuture.create();
        future.set(value);
        return future;
    }

    @Test
    public void initializeTest() throws InterruptedException, ExecutionException {
        when(messageWriter
                .writeInitializeMessage(argThat(Matchers.withInit(new InitializationInput().withShardId(shardId)))))
                        .thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT()).thenReturn(buildFuture(new StatusMessage("initialize"), Message.class));
        assertThat(protocol.initialize(), equalTo(true));
    }

    @Test
    public void processRecordsTest() throws InterruptedException, ExecutionException {
        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class))).thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT()).thenReturn(buildFuture(new StatusMessage("processRecords"), Message.class));

        assertThat(protocol.processRecords(new ProcessRecordsInput().withRecords(EMPTY_RECORD_LIST)), equalTo(true));
    }

    @Test
    public void shutdownTest() throws InterruptedException, ExecutionException {
        when(messageWriter.writeShutdownMessage(any(ShutdownReason.class))).thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT()).thenReturn(buildFuture(new StatusMessage("shutdown"), Message.class));

        Mockito.doReturn(buildFuture(true)).when(messageWriter)
                .writeShutdownMessage(any(ShutdownReason.class));
        Mockito.doReturn(buildFuture(new StatusMessage("shutdown"))).when(messageReader).getNextMessageFromSTDOUT();
        assertThat(protocol.shutdown(null, ShutdownReason.ZOMBIE), equalTo(true));
    }

    @Test
    public void shutdownRequestedTest() {
        when(messageWriter.writeShutdownRequestedMessage()).thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT()).thenReturn(buildFuture(new StatusMessage("shutdownRequested"), Message.class));
        Mockito.doReturn(buildFuture(true)).when(messageWriter)
                .writeShutdownRequestedMessage();
        Mockito.doReturn(buildFuture(new StatusMessage("shutdownRequested"))).when(messageReader).getNextMessageFromSTDOUT();
        assertThat(protocol.shutdownRequested(null), equalTo(true));
    }

    private Answer<Future<Message>> buildMessageAnswers(List<Message> messages) {
        return new Answer<Future<Message>>() {

            Iterator<Message> messageIterator;
            Message message;

            Answer<Future<Message>> init(List<Message> messages) {
                messageIterator = messages.iterator();
                return this;
            }

            @Override
            public Future<Message> answer(InvocationOnMock invocation) throws Throwable {
                if (this.messageIterator.hasNext()) {
                    message = this.messageIterator.next();
                }
                return buildFuture(message);
            }

        }.init(messages);
    }

    @Test
    public void processRecordsWithCheckpointsTest() throws InterruptedException, ExecutionException,
        KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {

        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class))).thenReturn(buildFuture(true));
        when(messageWriter.writeCheckpointMessageWithError(anyString(), anyLong(), any(Throwable.class))).thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT()).thenAnswer(buildMessageAnswers(new ArrayList<Message>() {
            {
                this.add(new CheckpointMessage("123", 0L, null));
                this.add(new CheckpointMessage(null, 0L, null));
                /*
                 * This procesRecords message will be ignored by the read loop which only cares about status and
                 * checkpoint messages. All other lines and message types are ignored. By inserting it here, we check
                 * that this test succeeds even with unexpected messaging.
                 */
                this.add(new ProcessRecordsMessage());
                this.add(new StatusMessage("processRecords"));
            }
        }));

        boolean result = protocol.processRecords(new ProcessRecordsInput().withRecords(EMPTY_RECORD_LIST).withCheckpointer(checkpointer));

        assertThat(result, equalTo(true));

        verify(checkpointer, timeout(1)).checkpoint();
        verify(checkpointer, timeout(1)).checkpoint("123", 0L);
    }

    @Test
    public void processRecordsWithABadCheckpointTest() throws InterruptedException, ExecutionException {
        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class))).thenReturn(buildFuture(true));
        when(messageWriter.writeCheckpointMessageWithError(anyString(), anyLong(), any(Throwable.class))).thenReturn(buildFuture(false));
        when(messageReader.getNextMessageFromSTDOUT()).thenAnswer(buildMessageAnswers(new ArrayList<Message>() {
            {
                this.add(new CheckpointMessage("456", 0L, null));
                this.add(new StatusMessage("processRecords"));
            }
        }));
        assertThat(protocol.processRecords(new ProcessRecordsInput().withRecords(EMPTY_RECORD_LIST).withCheckpointer(checkpointer)), equalTo(false));
    }

    @Test(expected = NullPointerException.class)
    public void waitForStatusMessageTimeoutTest() throws InterruptedException, TimeoutException, ExecutionException {
        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class))).thenReturn(buildFuture(true));
        Future<Message> future = Mockito.mock(Future.class);
        when(messageReader.getNextMessageFromSTDOUT()).thenReturn(future);
        when(configuration.getTimeoutInSeconds()).thenReturn(Optional.of(5));
        when(future.get(anyInt(), eq(TimeUnit.SECONDS))).thenThrow(TimeoutException.class);
        protocol = new MultiLangProtocolForTesting(messageReader,
                messageWriter,
                new InitializationInput().withShardId(shardId),
                configuration);

        protocol.processRecords(new ProcessRecordsInput().withRecords(EMPTY_RECORD_LIST));
    }

    @Test
    public void waitForStatusMessageSuccessTest() {
        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class))).thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT()).thenReturn(buildFuture(new StatusMessage("processRecords"), Message.class));
        when(configuration.getTimeoutInSeconds()).thenReturn(Optional.of(5));

        assertTrue(protocol.processRecords(new ProcessRecordsInput().withRecords(EMPTY_RECORD_LIST)));
    }

    private class MultiLangProtocolForTesting extends MultiLangProtocol {
        /**
         * Constructor.
         *
         * @param messageReader       A message reader.
         * @param messageWriter       A message writer.
         * @param initializationInput
         * @param configuration
         */
        MultiLangProtocolForTesting(final MessageReader messageReader,
                                  final MessageWriter messageWriter,
                                  final InitializationInput initializationInput,
                                  final KinesisClientLibConfiguration configuration) {
            super(messageReader, messageWriter, initializationInput, configuration);
        }

        @Override
        protected void haltJvm(final int exitStatus) {
            throw new NullPointerException();
        }
    }
}
