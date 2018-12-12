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

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfigurator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

public class MultiLangDaemonConfigTest {

    private static String FILENAME = "some.properties";

    private KinesisClientLibConfigurator buildMockConfigurator() {
        AWSCredentialsProvider credentialsProvider = Mockito.mock(AWSCredentialsProvider.class);
        AWSCredentials creds = Mockito.mock(AWSCredentials.class);
        Mockito.doReturn(creds).when(credentialsProvider).getCredentials();
        Mockito.doReturn("cool-user").when(creds).getAWSAccessKeyId();
        KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration("cool-app", "cool-stream", credentialsProvider, "cool-worker");
        KinesisClientLibConfigurator configurator = Mockito.mock(KinesisClientLibConfigurator.class);
        Mockito.doReturn(kclConfig).when(configurator).getConfiguration(Mockito.any(Properties.class));
        return configurator;
    }

    @Test
    public void constructorTest() throws IOException {
        String PROPERTIES =
                "executableName = randomEXE \n" + "applicationName = testApp \n" + "streamName = fakeStream \n"
                        + "AWSCredentialsProvider = DefaultAWSCredentialsProviderChain\n"
                        + "processingLanguage = malbolge";
        ClassLoader classLoader = Mockito.mock(ClassLoader.class);

        Mockito.doReturn(new ByteArrayInputStream(PROPERTIES.getBytes()))
                .when(classLoader)
                .getResourceAsStream(FILENAME);

        MultiLangDaemonConfig deamonConfig = new MultiLangDaemonConfig(FILENAME, classLoader, buildMockConfigurator());

        assertNotNull(deamonConfig.getExecutorService());
        assertNotNull(deamonConfig.getKinesisClientLibConfiguration());
        assertNotNull(deamonConfig.getRecordProcessorFactory());
    }

    @Test
    public void propertyValidation() {
        String PROPERTIES_NO_EXECUTABLE_NAME =
                "applicationName = testApp \n" + "streamName = fakeStream \n"
                        + "AWSCredentialsProvider = DefaultAWSCredentialsProviderChain\n"
                        + "processingLanguage = malbolge";
        ClassLoader classLoader = Mockito.mock(ClassLoader.class);

        Mockito.doReturn(new ByteArrayInputStream(PROPERTIES_NO_EXECUTABLE_NAME.getBytes()))
                .when(classLoader)
                .getResourceAsStream(FILENAME);

        MultiLangDaemonConfig config;
        try {
            config = new MultiLangDaemonConfig(FILENAME, classLoader, buildMockConfigurator());
            Assert.fail("Construction of the config should have failed due to property validation failing.");
        } catch (IllegalArgumentException e) {
            // Good
        } catch (IOException e) {
            Assert.fail();
        }
    }

}
