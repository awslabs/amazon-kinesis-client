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
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ReadSTDERRTaskTest {

    private static final String SHARD_ID = "shard-123";
    private BufferedReader mockBufferReader;

    @Before
    public void setup() {
        mockBufferReader = Mockito.mock(BufferedReader.class);
    }

    @Test
    public void errorReaderBuilderTest() {

        String errorMessages = "OMG\nThis is test message\n blah blah blah \n";
        InputStream stream = new ByteArrayInputStream(errorMessages.getBytes());
        LineReaderTask<Boolean> reader = new DrainChildSTDERRTask().initialize(stream, SHARD_ID, "");
        Assert.assertNotNull(reader);
    }

    @Test
    public void runTest() throws Exception {
        String errorMessages = "OMG\nThis is test message\n blah blah blah \n";
        BufferedReader bufferReader =
                new BufferedReader(new InputStreamReader(new ByteArrayInputStream(errorMessages.getBytes())));
        LineReaderTask<Boolean> errorReader = new DrainChildSTDERRTask().initialize(bufferReader, SHARD_ID, "");
        Assert.assertNotNull(errorReader);

        Boolean result = errorReader.call();
        Assert.assertTrue(result);
    }

    private void runErrorTest(Exception exceptionToThrow) {
        try {
            Mockito.doThrow(exceptionToThrow).when(mockBufferReader).readLine();
        } catch (IOException e) {
            Assert.fail("Not supposed to get an exception when we're just building our mock.");
        }
        LineReaderTask<Boolean> errorReader = new DrainChildSTDERRTask().initialize(mockBufferReader, SHARD_ID, "");
        Assert.assertNotNull(errorReader);
        Future<Boolean> result = Executors.newCachedThreadPool().submit(errorReader);
        Boolean finishedCleanly = null;
        try {
            finishedCleanly = result.get();
        } catch (InterruptedException | ExecutionException e) {
            Assert.fail(
                    "Should have been able to get a result. The error should be handled during the call and result in false.");
        }
        Assert.assertFalse("Reading a line should have thrown an exception", finishedCleanly);
    }

    @Test
    public void runCausesIOErrorTest() {
        runErrorTest(new IOException());
    }

    @Test
    public void runCausesUnExpectedErrorTest() throws IOException {
        Mockito.doThrow(IOException.class).when(this.mockBufferReader).close();
        runErrorTest(new IOException());
    }
}
