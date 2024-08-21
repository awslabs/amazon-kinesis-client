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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.kinesis.multilang.messages.Message;
import software.amazon.kinesis.multilang.messages.StatusMessage;

public class MessageReaderTest {

    private static final String SHARD_ID = "shard-123";

    /**
     * This line is based on the definition of the protocol for communication between the KCL record processor and
     * the client's process.
     */
    private String buildCheckpointLine(String sequenceNumber) {
        return String.format("{\"action\":\"checkpoint\", \"checkpoint\":\"%s\"}", sequenceNumber);
    }

    /**
     * This line is based on the definition of the protocol for communication between the KCL record processor and
     * the client's process.
     */
    private String buildStatusLine(String methodName) {
        return String.format("{\"action\":\"status\", \"responseFor\":\"%s\"}", methodName);
    }

    private InputStream buildInputStreamOfGoodInput(String[] sequenceNumbers, String[] responseFors) {
        // Just interlace the lines
        StringBuilder stringBuilder = new StringBuilder();
        // This is just a reminder to anyone who changes the arrays
        Assert.assertTrue(responseFors.length == sequenceNumbers.length + 1);
        stringBuilder.append(buildStatusLine(responseFors[0]));
        stringBuilder.append("\n");
        // Also a white space line, which it should be able to handle with out failing.
        stringBuilder.append("    \n");
        // Also a bogus data line, which it should be able to handle with out failing.
        stringBuilder.append("  bogus data  \n");
        for (int i = 0; i < Math.min(sequenceNumbers.length, responseFors.length); i++) {
            stringBuilder.append(buildCheckpointLine(sequenceNumbers[i]));
            stringBuilder.append("\n");
            stringBuilder.append(buildStatusLine(responseFors[i + 1]));
            stringBuilder.append("\n");
        }

        return new ByteArrayInputStream(stringBuilder.toString().getBytes());
    }

    @Test
    public void runLoopGoodInputTest() {
        String[] sequenceNumbers = new String[] {"123", "456", "789"};
        String[] responseFors = new String[] {"initialize", "processRecords", "processRecords", "shutdown"};
        InputStream stream = buildInputStreamOfGoodInput(sequenceNumbers, responseFors);
        MessageReader reader =
                new MessageReader().initialize(stream, SHARD_ID, new ObjectMapper(), Executors.newCachedThreadPool());

        for (String responseFor : responseFors) {
            try {
                Message message = reader.getNextMessageFromSTDOUT().get();
                if (message instanceof StatusMessage) {
                    Assert.assertEquals(
                            "The status message's responseFor field should have been correct",
                            responseFor,
                            ((StatusMessage) message).getResponseFor());
                }
            } catch (InterruptedException | ExecutionException e) {
                Assert.fail("There should have been a status message for " + responseFor);
            }
        }
    }

    @Test
    public void drainInputTest() throws InterruptedException, ExecutionException {
        String[] sequenceNumbers = new String[] {"123", "456", "789"};
        String[] responseFors = new String[] {"initialize", "processRecords", "processRecords", "shutdown"};
        InputStream stream = buildInputStreamOfGoodInput(sequenceNumbers, responseFors);

        MessageReader reader =
                new MessageReader().initialize(stream, SHARD_ID, new ObjectMapper(), Executors.newCachedThreadPool());
        Future<Boolean> drainFuture = reader.drainSTDOUT();
        Boolean drainResult = drainFuture.get();
        Assert.assertNotNull(drainResult);
        Assert.assertTrue(drainResult);
    }

    /**
     * readValue should fail safely and just continue looping
     */
    @Test
    public void unexcpectedStatusFailure() {
        BufferedReader bufferReader = Mockito.mock(BufferedReader.class);
        try {
            Mockito.doAnswer(new Answer() {
                        private boolean returnedOnce = false;

                        @Override
                        public Object answer(InvocationOnMock invocation) throws Throwable {
                            if (returnedOnce) {
                                return "{\"action\":\"status\",\"responseFor\":\"processRecords\"}";
                            } else {
                                returnedOnce = true;
                                return "{\"action\":\"shutdown\",\"reason\":\"ZOMBIE\"}";
                            }
                        }
                    })
                    .when(bufferReader)
                    .readLine();
        } catch (IOException e) {
            Assert.fail("There shouldn't be an exception while setting up this mock.");
        }

        MessageReader reader = new MessageReader()
                .initialize(bufferReader, SHARD_ID, new ObjectMapper(), Executors.newCachedThreadPool());

        try {
            reader.getNextMessageFromSTDOUT().get();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("MessageReader should have handled the bad message gracefully");
        }
    }

    @Test
    public void messageReaderBuilderTest() {
        InputStream stream = new ByteArrayInputStream("".getBytes());
        MessageReader reader =
                new MessageReader().initialize(stream, SHARD_ID, new ObjectMapper(), Executors.newCachedThreadPool());
        Assert.assertNotNull(reader);
    }

    @Test
    public void readLineFails() throws IOException {
        BufferedReader input = Mockito.mock(BufferedReader.class);
        Mockito.doThrow(IOException.class).when(input).readLine();
        MessageReader reader =
                new MessageReader().initialize(input, SHARD_ID, new ObjectMapper(), Executors.newCachedThreadPool());

        Future<Message> readTask = reader.getNextMessageFromSTDOUT();

        try {
            readTask.get();
            Assert.fail("The reading task should have failed due to an IOException.");
        } catch (InterruptedException e) {
            Assert.fail(
                    "The reading task should not have been interrupted. It should have failed due to an IOException.");
        } catch (ExecutionException e) {
            // Yay!!
        }
    }

    @Test
    public void noMoreMessagesTest() throws InterruptedException {
        InputStream stream = new ByteArrayInputStream("".getBytes());
        MessageReader reader =
                new MessageReader().initialize(stream, SHARD_ID, new ObjectMapper(), Executors.newCachedThreadPool());
        Future<Message> future = reader.getNextMessageFromSTDOUT();

        try {
            future.get();
            Assert.fail("There should have been an execution exception if there were no more messages to get.");
        } catch (ExecutionException e) {
            // Good path.
        }
    }
}
