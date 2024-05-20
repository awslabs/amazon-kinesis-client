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
package software.amazon.kinesis.multilang.messages;

import java.nio.ByteBuffer;
import java.util.Collections;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.kinesis.lifecycle.ShutdownReason;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class MessageTest {

    @Test
    public void toStringTest() {
        Message[] messages = new Message[] {
            new CheckpointMessage("1234567890", 0L, null),
            new InitializeMessage(
                    InitializationInput.builder().shardId("shard-123").build()),
            new ProcessRecordsMessage(ProcessRecordsInput.builder()
                    .records(Collections.singletonList(KinesisClientRecord.builder()
                            .data(ByteBuffer.wrap("cat".getBytes()))
                            .partitionKey("cat")
                            .sequenceNumber("555")
                            .build()))
                    .build()),
            new ShutdownMessage(ShutdownReason.LEASE_LOST),
            new StatusMessage("processRecords"),
            new InitializeMessage(),
            new ProcessRecordsMessage(),
            new ShutdownRequestedMessage(),
            new LeaseLostMessage(),
            new ShardEndedMessage(),
        };

        //        TODO: fix this
        for (int i = 0; i < messages.length; i++) {
            System.out.println(messages[i].toString());
            Assert.assertTrue(
                    "Each message should contain the action field",
                    messages[i].toString().contains("action"));
        }

        // Hit this constructor
        KinesisClientRecord defaultJsonFriendlyRecord =
                KinesisClientRecord.builder().build();
        Assert.assertNull(defaultJsonFriendlyRecord.partitionKey());
        Assert.assertNull(defaultJsonFriendlyRecord.data());
        Assert.assertNull(defaultJsonFriendlyRecord.sequenceNumber());
        Assert.assertNull(new ShutdownMessage(null).getReason());

        // Hit the bad object mapping path
        Message withBadMapper = new Message() {}.withObjectMapper(new ObjectMapper() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String writeValueAsString(Object m) throws JsonProcessingException {
                throw new JsonProcessingException(new Throwable()) {};
            }
        });
        String s = withBadMapper.toString();
        Assert.assertNotNull(s);
    }
}
