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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.multilang.messages.Message;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import static org.mockito.Mockito.verify;

public class MessageWriterTest {

    private static final String SHARD_ID = "shard-123";
    MessageWriter messageWriter;
    OutputStream stream;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        stream = Mockito.mock(OutputStream.class);
        messageWriter =
                new MessageWriter().initialize(stream, SHARD_ID, new ObjectMapper(), Executors.newCachedThreadPool());
    }

    /*
     * Here we are just testing that calling write causes bytes to get written to the stream.
     */
    @Test
    public void writeCheckpointMessageNoErrorTest() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = this.messageWriter.writeCheckpointMessageWithError("1234", 0L, null);
        future.get();
        verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
        verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void writeCheckpointMessageWithErrorTest() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = this.messageWriter.writeCheckpointMessageWithError("1234", 0L, new Throwable());
        future.get();
        verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
        verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void writeInitializeMessageTest() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = this.messageWriter.writeInitializeMessage(
                InitializationInput.builder().shardId(SHARD_ID).build());
        future.get();
        verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
        verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void writeProcessRecordsMessageTest() throws IOException, InterruptedException, ExecutionException {
        List<KinesisClientRecord> records = Arrays.asList(
                KinesisClientRecord.builder()
                        .data(ByteBuffer.wrap("kitten".getBytes()))
                        .partitionKey("some cats")
                        .sequenceNumber("357234807854789057805")
                        .build(),
                KinesisClientRecord.builder().build());
        Future<Boolean> future = this.messageWriter.writeProcessRecordsMessage(
                ProcessRecordsInput.builder().records(records).build());
        future.get();

        verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
        verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void writeShutdownMessageTest() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = this.messageWriter.writeShardEndedMessage(
                ShardEndedInput.builder().build());
        future.get();

        verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
        verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void writeShutdownRequestedMessageTest() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = this.messageWriter.writeShutdownRequestedMessage();
        future.get();

        verify(this.stream, Mockito.atLeastOnce()).write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
        verify(this.stream, Mockito.atLeastOnce()).flush();
    }

    @Test
    public void streamIOExceptionTest() throws IOException, InterruptedException, ExecutionException {
        Mockito.doThrow(IOException.class).when(stream).flush();
        Future<Boolean> initializeTask = this.messageWriter.writeInitializeMessage(
                InitializationInput.builder().shardId(SHARD_ID).build());
        Boolean result = initializeTask.get();
        Assert.assertNotNull(result);
        Assert.assertFalse(result);
    }

    @Test
    public void objectMapperFails() throws JsonProcessingException {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Encountered I/O error while writing LeaseLostMessage action to subprocess");

        ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
        Mockito.doThrow(JsonProcessingException.class).when(mapper).writeValueAsString(Mockito.any(Message.class));
        messageWriter = new MessageWriter().initialize(stream, SHARD_ID, mapper, Executors.newCachedThreadPool());

        messageWriter.writeLeaseLossMessage(LeaseLostInput.builder().build());
    }

    @Test
    public void closeWriterTest() throws IOException {
        Assert.assertTrue(this.messageWriter.isOpen());
        this.messageWriter.close();
        verify(this.stream, Mockito.times(1)).close();
        Assert.assertFalse(this.messageWriter.isOpen());
        try {
            // Any message should fail
            this.messageWriter.writeInitializeMessage(
                    InitializationInput.builder().shardId(SHARD_ID).build());
            Assert.fail("MessageWriter should be closed and unable to write.");
        } catch (IllegalStateException e) {
            // This should happen.
        }
    }
}
