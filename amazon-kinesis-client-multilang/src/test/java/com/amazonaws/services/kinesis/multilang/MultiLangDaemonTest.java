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

import java.io.PrintStream;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.mockito.Mockito;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;

public class MultiLangDaemonTest {

    @Test
    public void buildWorkerTest() {
        // Mocking Kinesis creds
        AwsCredentialsProvider provider = Mockito.mock(AwsCredentialsProvider.class);
        Mockito.doReturn(Mockito.mock(AwsCredentials.class)).when(provider).resolveCredentials();
        KinesisClientLibConfiguration configuration = new KinesisClientLibConfiguration("Derp", "Blurp", provider,
                "Worker");

        MultiLangRecordProcessorFactory factory = Mockito.mock(MultiLangRecordProcessorFactory.class);
        Mockito.doReturn(new String[] { "someExecutableName" }).when(factory).getCommandArray();
        MultiLangDaemon daemon = new MultiLangDaemon(configuration, factory, Executors.newCachedThreadPool());
    }

    @Test
    public void usageTest() {
        PrintStream printStream = Mockito.mock(PrintStream.class);

        String message = "Everything blew up";

        MultiLangDaemon.printUsage(printStream, message);
        Mockito.verify(printStream, Mockito.times(1)).println(Mockito.contains(message));
    }
}
