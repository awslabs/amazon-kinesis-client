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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.multilang.config.MultiLangDaemonConfiguration;
import software.amazon.kinesis.multilang.messages.CheckpointMessage;
import software.amazon.kinesis.multilang.messages.LeaseLostMessage;
import software.amazon.kinesis.multilang.messages.Message;
import software.amazon.kinesis.multilang.messages.ProcessRecordsMessage;
import software.amazon.kinesis.multilang.messages.ShardEndedMessage;
import software.amazon.kinesis.multilang.messages.StatusMessage;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

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
    private static final List<KinesisClientRecord> EMPTY_RECORD_LIST = Collections.emptyList();

    @Mock
    private MultiLangProtocol protocol;

    @Mock
    private MessageWriter messageWriter;

    @Mock
    private MessageReader messageReader;

    private String shardId;

    @Mock
    private RecordProcessorCheckpointer checkpointer;

    @Mock
    private MultiLangDaemonConfiguration configuration;

    @Before
    public void setup() {
        this.shardId = "shard-id-123";
        protocol = new MultiLangProtocolForTesting(
                messageReader,
                messageWriter,
                InitializationInput.builder().shardId(shardId).build(),
                configuration);

        when(configuration.getTimeoutInSeconds()).thenReturn(null);
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
    public void testInitialize() {
        when(messageWriter.writeInitializeMessage(argThat(Matchers.withInit(
                        InitializationInput.builder().shardId(shardId).build()))))
                .thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT())
                .thenReturn(buildFuture(new StatusMessage("initialize"), Message.class));
        assertThat(protocol.initialize(), equalTo(true));
    }

    @Test
    public void testProcessRecords() {
        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class)))
                .thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT())
                .thenReturn(buildFuture(new StatusMessage("processRecords"), Message.class));

        assertThat(
                protocol.processRecords(
                        ProcessRecordsInput.builder().records(EMPTY_RECORD_LIST).build()),
                equalTo(true));
    }

    @Test
    public void leaseLostTest() {
        when(messageWriter.writeLeaseLossMessage(any(LeaseLostInput.class))).thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT())
                .thenReturn(buildFuture(new StatusMessage(LeaseLostMessage.ACTION), Message.class));

        assertThat(protocol.leaseLost(LeaseLostInput.builder().build()), equalTo(true));
    }

    @Test
    public void shardEndedTest() {
        when(messageWriter.writeShardEndedMessage(any(ShardEndedInput.class))).thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT())
                .thenReturn(buildFuture(new StatusMessage(ShardEndedMessage.ACTION)));

        assertThat(protocol.shardEnded(ShardEndedInput.builder().build()), equalTo(true));
    }

    @Test
    public void shutdownRequestedTest() {
        when(messageWriter.writeShutdownRequestedMessage()).thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT())
                .thenReturn(buildFuture(new StatusMessage("shutdownRequested"), Message.class));
        Mockito.doReturn(buildFuture(true)).when(messageWriter).writeShutdownRequestedMessage();
        Mockito.doReturn(buildFuture(new StatusMessage("shutdownRequested")))
                .when(messageReader)
                .getNextMessageFromSTDOUT();
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
    public void testProcessRecordsWithCheckpoints()
            throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {

        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class)))
                .thenReturn(buildFuture(true));
        when(messageWriter.writeCheckpointMessageWithError(anyString(), anyLong(), any(Throwable.class)))
                .thenReturn(buildFuture(true));
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

        boolean result = protocol.processRecords(ProcessRecordsInput.builder()
                .records(EMPTY_RECORD_LIST)
                .checkpointer(checkpointer)
                .build());

        assertThat(result, equalTo(true));

        verify(checkpointer, timeout(1)).checkpoint();
        verify(checkpointer, timeout(1)).checkpoint("123", 0L);
    }

    @Test
    public void testProcessRecordsWithABadCheckpoint() {
        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class)))
                .thenReturn(buildFuture(true));
        when(messageWriter.writeCheckpointMessageWithError(anyString(), anyLong(), any(Throwable.class)))
                .thenReturn(buildFuture(false));
        when(messageReader.getNextMessageFromSTDOUT()).thenAnswer(buildMessageAnswers(new ArrayList<Message>() {
            {
                this.add(new CheckpointMessage("456", 0L, null));
                this.add(new StatusMessage("processRecords"));
            }
        }));
        assertThat(
                protocol.processRecords(ProcessRecordsInput.builder()
                        .records(EMPTY_RECORD_LIST)
                        .checkpointer(checkpointer)
                        .build()),
                equalTo(false));
    }

    @Test(expected = NullPointerException.class)
    public void waitForStatusMessageTimeoutTest() throws InterruptedException, TimeoutException, ExecutionException {
        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class)))
                .thenReturn(buildFuture(true));
        Future<Message> future = Mockito.mock(Future.class);
        when(messageReader.getNextMessageFromSTDOUT()).thenReturn(future);
        when(configuration.getTimeoutInSeconds()).thenReturn(5);
        when(future.get(anyInt(), eq(TimeUnit.SECONDS))).thenThrow(TimeoutException.class);
        protocol = new MultiLangProtocolForTesting(
                messageReader,
                messageWriter,
                InitializationInput.builder().shardId(shardId).build(),
                configuration);

        protocol.processRecords(
                ProcessRecordsInput.builder().records(EMPTY_RECORD_LIST).build());
    }

    @Test
    public void waitForStatusMessageSuccessTest() {
        when(messageWriter.writeProcessRecordsMessage(any(ProcessRecordsInput.class)))
                .thenReturn(buildFuture(true));
        when(messageReader.getNextMessageFromSTDOUT())
                .thenReturn(buildFuture(new StatusMessage("processRecords"), Message.class));
        when(configuration.getTimeoutInSeconds()).thenReturn(5);

        assertTrue(protocol.processRecords(
                ProcessRecordsInput.builder().records(EMPTY_RECORD_LIST).build()));
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
        MultiLangProtocolForTesting(
                final MessageReader messageReader,
                final MessageWriter messageWriter,
                final InitializationInput initializationInput,
                final MultiLangDaemonConfiguration configuration) {
            super(messageReader, messageWriter, initializationInput, configuration);
        }

        @Override
        protected void haltJvm(final int exitStatus) {
            throw new NullPointerException();
        }
    }
}
