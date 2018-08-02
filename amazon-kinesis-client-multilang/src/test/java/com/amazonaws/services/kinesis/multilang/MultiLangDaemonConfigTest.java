/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.multilang.config.KinesisClientLibConfigurator;

import junit.framework.Assert;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class MultiLangDaemonConfigTest {
    private static String FILENAME = "some.properties";

    @Mock
    private AwsCredentialsProvider credentialsProvider;
    @Mock
    private AwsCredentials creds;
    @Mock
    private KinesisClientLibConfigurator configurator;

    @Before
    public void setup() {
        when(credentialsProvider.resolveCredentials()).thenReturn(creds);
        when(creds.accessKeyId()).thenReturn("cool-user");
        when(configurator.getConfiguration(any(Properties.class))).thenReturn(
                new KinesisClientLibConfiguration("cool-app", "cool-stream", credentialsProvider, "cool-worker"));
    }

    // TODO: Fix test
    @Ignore
    @Test
    public void constructorTest() throws IOException {
        String PROPERTIES = "executableName = randomEXE \n" + "applicationName = testApp \n"
                + "streamName = fakeStream \n" + "AWSCredentialsProvider = DefaultAWSCredentialsProviderChain\n"
                + "processingLanguage = malbolge";
        ClassLoader classLoader = Mockito.mock(ClassLoader.class);

        Mockito.doReturn(new ByteArrayInputStream(PROPERTIES.getBytes())).when(classLoader)
                .getResourceAsStream(FILENAME);

        MultiLangDaemonConfig deamonConfig = new MultiLangDaemonConfig(FILENAME, classLoader, configurator);

        assertNotNull(deamonConfig.getExecutorService());
        assertNotNull(deamonConfig.getKinesisClientLibConfiguration());
        assertNotNull(deamonConfig.getRecordProcessorFactory());
    }

    // TODO: Fix test
    @Ignore
    @Test
    public void propertyValidation() {
        String PROPERTIES_NO_EXECUTABLE_NAME = "applicationName = testApp \n" + "streamName = fakeStream \n"
                + "AWSCredentialsProvider = DefaultAWSCredentialsProviderChain\n" + "processingLanguage = malbolge";
        ClassLoader classLoader = Mockito.mock(ClassLoader.class);

        Mockito.doReturn(new ByteArrayInputStream(PROPERTIES_NO_EXECUTABLE_NAME.getBytes())).when(classLoader)
                .getResourceAsStream(FILENAME);

        MultiLangDaemonConfig config;
        try {
            config = new MultiLangDaemonConfig(FILENAME, classLoader, configurator);
            Assert.fail("Construction of the config should have failed due to property validation failing.");
        } catch (IllegalArgumentException e) {
            // Good
        } catch (IOException e) {
            Assert.fail();
        }
    }

}
