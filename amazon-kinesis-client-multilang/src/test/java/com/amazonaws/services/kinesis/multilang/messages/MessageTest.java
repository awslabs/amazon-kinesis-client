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
package com.amazonaws.services.kinesis.multilang.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageTest {

    @Test
    public void toStringTest() {
        Message[] messages =
                new Message[] { new CheckpointMessage("1234567890", 0L, null), new InitializeMessage(new InitializationInput().withShardId("shard-123")),
                        new ProcessRecordsMessage(new ProcessRecordsInput().withRecords(new ArrayList<Record>() {
                            {
                                this.add(new Record() {
                                    {
                                        this.withData(ByteBuffer.wrap("cat".getBytes()));
                                        this.withPartitionKey("cat");
                                        this.withSequenceNumber("555");
                                    }
                                });
                            }
                        })), new ShutdownMessage(ShutdownReason.ZOMBIE), new StatusMessage("processRecords"),
                        new InitializeMessage(), new ProcessRecordsMessage(), new ShutdownRequestedMessage() };

        for (int i = 0; i < messages.length; i++) {
            Assert.assertTrue("Each message should contain the action field", messages[i].toString().contains("action"));
        }

        // Hit this constructor
        JsonFriendlyRecord defaultJsonFriendlyRecord = new JsonFriendlyRecord();
        Assert.assertNull(defaultJsonFriendlyRecord.getPartitionKey());
        Assert.assertNull(defaultJsonFriendlyRecord.getData());
        Assert.assertNull(defaultJsonFriendlyRecord.getSequenceNumber());
        Assert.assertNull(new ShutdownMessage(null).getReason());

        // Hit the bad object mapping path
        Message withBadMapper = new Message() {
        }.withObjectMapper(new ObjectMapper() {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String writeValueAsString(Object m) throws JsonProcessingException {
                throw new JsonProcessingException(new Throwable()) {
                };
            }
        });
        String s = withBadMapper.toString();
        Assert.assertNotNull(s);
    }
}
