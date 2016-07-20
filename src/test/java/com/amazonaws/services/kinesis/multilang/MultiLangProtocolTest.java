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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
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
import com.amazonaws.services.kinesis.multilang.messages.CheckpointMessage;
import com.amazonaws.services.kinesis.multilang.messages.Message;
import com.amazonaws.services.kinesis.multilang.messages.ProcessRecordsMessage;
import com.amazonaws.services.kinesis.multilang.messages.StatusMessage;

public class MultiLangProtocolTest {

    private MultiLangProtocol protocol;
    private MessageWriter messageWriter;
    private MessageReader messageReader;
    private String shardId;
    private IRecordProcessorCheckpointer checkpointer;

    @Before
    public void setup() {
        this.shardId = "shard-id-123";
        messageWriter = Mockito.mock(MessageWriter.class);
        messageReader = Mockito.mock(MessageReader.class);
        protocol = new MultiLangProtocol(messageReader, messageWriter, shardId);
        checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
    }

    private Future<Boolean> buildBooleanFuture(boolean val) throws InterruptedException, ExecutionException {
        Future<Boolean> successFuture = Mockito.mock(Future.class);
        Mockito.doReturn(val).when(successFuture).get();
        return successFuture;
    }

    private Future<Message> buildMessageFuture(Message message) throws InterruptedException, ExecutionException {
        Future<Message> messageFuture = Mockito.mock(Future.class);
        Mockito.doReturn(message).when(messageFuture).get();
        return messageFuture;
    }

    @Test
    public void initializeTest() throws InterruptedException, ExecutionException {
        Mockito.doReturn(buildBooleanFuture(true)).when(messageWriter).writeInitializeMessage(shardId);
        Mockito.doReturn(buildMessageFuture(new StatusMessage("initialize"))).when(messageReader).getNextMessageFromSTDOUT();
        Assert.assertTrue(protocol.initialize());
    }

    @Test
    public void processRecordsTest() throws InterruptedException, ExecutionException {
        Mockito.doReturn(buildBooleanFuture(true)).when(messageWriter).writeProcessRecordsMessage(Mockito.anyList());
        Mockito.doReturn(buildMessageFuture(new StatusMessage("processRecords"))).when(messageReader).getNextMessageFromSTDOUT();
        Assert.assertTrue(protocol.processRecords(new ArrayList<Record>(), null));
    }

    @Test
    public void shutdownTest() throws InterruptedException, ExecutionException {
        Mockito.doReturn(buildBooleanFuture(true)).when(messageWriter)
                .writeShutdownMessage(Mockito.any(ShutdownReason.class));
        Mockito.doReturn(buildMessageFuture(new StatusMessage("shutdown"))).when(messageReader).getNextMessageFromSTDOUT();
        Assert.assertTrue(protocol.shutdown(null, ShutdownReason.ZOMBIE));
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
                return buildMessageFuture(message);
            }

        }.init(messages);
    }

    @Test
    public void processRecordsWithCheckpointsTest() throws InterruptedException, ExecutionException,
        KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {

        Mockito.doReturn(buildBooleanFuture(true)).when(messageWriter).writeProcessRecordsMessage(Mockito.anyList());
        Mockito.doReturn(buildBooleanFuture(true)).when(messageWriter)
                .writeCheckpointMessageWithError(Mockito.anyString(), Mockito.any(Throwable.class));
        Mockito.doAnswer(buildMessageAnswers(new ArrayList<Message>() {
            {
                this.add(new CheckpointMessage("123", null));
                this.add(new CheckpointMessage(null, null));
                /*
                 * This procesRecords message will be ignored by the read loop which only cares about status and
                 * checkpoint messages. All other lines and message types are ignored. By inserting it here, we check
                 * that this test succeeds even with unexpected messaging.
                 */
                this.add(new ProcessRecordsMessage());
                this.add(new StatusMessage("processRecords"));
            }
        })).when(messageReader).getNextMessageFromSTDOUT();
        Assert.assertTrue(protocol.processRecords(new ArrayList<Record>(), checkpointer));

        Mockito.verify(checkpointer, Mockito.timeout(1)).checkpoint();
        Mockito.verify(checkpointer, Mockito.timeout(1)).checkpoint("123");
    }

    @Test
    public void processRecordsWithABadCheckpointTest() throws InterruptedException, ExecutionException {
        Mockito.doReturn(buildBooleanFuture(true)).when(messageWriter).writeProcessRecordsMessage(Mockito.anyList());
        Mockito.doReturn(buildBooleanFuture(false)).when(messageWriter)
                .writeCheckpointMessageWithError(Mockito.anyString(), Mockito.any(Throwable.class));
        Mockito.doAnswer(buildMessageAnswers(new ArrayList<Message>() {
            {
                this.add(new CheckpointMessage("456", null));
                this.add(new StatusMessage("processRecords"));
            }
        })).when(messageReader).getNextMessageFromSTDOUT();
        Assert.assertFalse(protocol.processRecords(new ArrayList<Record>(), checkpointer));
    }
}
