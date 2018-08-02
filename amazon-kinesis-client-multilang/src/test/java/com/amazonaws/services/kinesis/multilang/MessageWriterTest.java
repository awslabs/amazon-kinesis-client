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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.kinesis.multilang.messages.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.ShutdownReason;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class MessageWriterTest {

    private static final String shardId = "shard-123";
    MessageWriter messageWriter;
    OutputStream stream;

    // ExecutorService executor;

    @Before
    public void setup() {
        stream = Mockito.mock(OutputStream.class);
        messageWriter =
                new MessageWriter().initialize(stream, shardId, new ObjectMapper(), Executors.newCachedThreadPool());
    }

    /*
     * Here we are just testing that calling write causes bytes to get written to the stream.
     */
    @Test
    public void writeCheckpointMessageNoErrorTest() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = this.messageWriter.writeCheckpointMessageWithError("1234", 0L, null);
        future.get();
        Mockito.verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt());
        Mockito.verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void writeCheckpointMessageWithErrorTest() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = this.messageWriter.writeCheckpointMessageWithError("1234", 0L, new Throwable());
        future.get();
        Mockito.verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt());
        Mockito.verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void writeInitializeMessageTest() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = this.messageWriter.writeInitializeMessage(InitializationInput.builder().shardId(shardId).build());
        future.get();
        Mockito.verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt());
        Mockito.verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void writeProcessRecordsMessageTest() throws IOException, InterruptedException, ExecutionException {
        List<KinesisClientRecord> records = Arrays.asList(
                KinesisClientRecord.builder().data(ByteBuffer.wrap("kitten".getBytes())).partitionKey("some cats")
                        .sequenceNumber("357234807854789057805").build(),
                KinesisClientRecord.builder().build()
        );
        Future<Boolean> future = this.messageWriter.writeProcessRecordsMessage(ProcessRecordsInput.builder().records(records).build());
        future.get();

        Mockito.verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt());
        Mockito.verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void writeShutdownMessageTest() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = this.messageWriter.writeShutdownMessage(ShutdownReason.SHARD_END);
        future.get();

        Mockito.verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt());
        Mockito.verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void writeShutdownRequestedMessageTest() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = this.messageWriter.writeShutdownRequestedMessage();
        future.get();

        Mockito.verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt());
        Mockito.verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void streamIOExceptionTest() throws IOException, InterruptedException, ExecutionException {
        Mockito.doThrow(IOException.class).when(stream).flush();
        Future<Boolean> initializeTask = this.messageWriter.writeInitializeMessage(InitializationInput.builder().shardId(shardId).build());
        Boolean result = initializeTask.get();
        Assert.assertNotNull(result);
        Assert.assertFalse(result);
    }

    @Test
    public void objectMapperFails() throws JsonProcessingException, InterruptedException, ExecutionException {
        ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
        Mockito.doThrow(JsonProcessingException.class).when(mapper).writeValueAsString(Mockito.any(Message.class));
        messageWriter = new MessageWriter().initialize(stream, shardId, mapper, Executors.newCachedThreadPool());

        try {
            messageWriter.writeShutdownMessage(ShutdownReason.LEASE_LOST);
            Assert.fail("The mapper failed so no write method should be able to succeed.");
        } catch (Exception e) {
            // Note that this is different than the stream failing. The stream is expected to fail, so we handle it
            // gracefully, but the JSON mapping should always succeed.
        }

    }

    @Test
    public void closeWriterTest() throws IOException {
        Assert.assertTrue(this.messageWriter.isOpen());
        this.messageWriter.close();
        Mockito.verify(this.stream, Mockito.times(1)).close();
        Assert.assertFalse(this.messageWriter.isOpen());
        try {
            // Any message should fail
            this.messageWriter.writeInitializeMessage(InitializationInput.builder().shardId(shardId).build());
            Assert.fail("MessageWriter should be closed and unable to write.");
        } catch (IllegalStateException e) {
            // This should happen.
        }
    }
}
