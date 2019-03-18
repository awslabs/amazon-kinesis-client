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
package com.amazonaws.services.kinesis.multilang;

import java.io.PrintStream;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

public class MultiLangDaemonTest {

    @Test
    public void buildWorkerTest() {        
        // Mocking Kinesis creds
        AWSCredentialsProvider provider = Mockito.mock(AWSCredentialsProvider.class);
        Mockito.doReturn(Mockito.mock(AWSCredentials.class)).when(provider).getCredentials();
        KinesisClientLibConfiguration configuration = new KinesisClientLibConfiguration( "Derp",
                "Blurp",
                provider,
                "Worker");
        
        MultiLangRecordProcessorFactory factory = Mockito.mock(MultiLangRecordProcessorFactory.class);
        Mockito.doReturn(new String[] { "someExecutableName" }).when(factory).getCommandArray();
        MultiLangDaemon daemon =
                new MultiLangDaemon(configuration, factory, Executors.newCachedThreadPool());
    }

    @Test
    public void usageTest() {
        PrintStream printStream = Mockito.mock(PrintStream.class);

        String message = "Everything blew up";

        MultiLangDaemon.printUsage(printStream, message);
        Mockito.verify(printStream, Mockito.times(1)).println(Mockito.contains(message));
    }
}
