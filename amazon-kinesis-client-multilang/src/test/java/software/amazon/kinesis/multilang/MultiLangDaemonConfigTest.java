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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import junit.framework.Assert;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.kinesis.multilang.config.KinesisClientLibConfigurator;
import software.amazon.kinesis.multilang.config.MultiLangDaemonConfiguration;

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
        ConvertUtilsBean convertUtilsBean = new ConvertUtilsBean();
        BeanUtilsBean utilsBean = new BeanUtilsBean(convertUtilsBean);
        MultiLangDaemonConfiguration multiLangDaemonConfiguration = new MultiLangDaemonConfiguration(utilsBean,
                convertUtilsBean);
        multiLangDaemonConfiguration.setApplicationName("cool-app");
        multiLangDaemonConfiguration.setStreamName("cool-stream");
        multiLangDaemonConfiguration.setWorkerIdentifier("cool-worker");
        when(credentialsProvider.resolveCredentials()).thenReturn(creds);
        when(creds.accessKeyId()).thenReturn("cool-user");
        when(configurator.getConfiguration(any(Properties.class))).thenReturn(multiLangDaemonConfiguration);
    }

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
        assertNotNull(deamonConfig.getMultiLangDaemonConfiguration());
        assertNotNull(deamonConfig.getRecordProcessorFactory());
    }

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
