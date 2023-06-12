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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import junit.framework.Assert;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.kinesis.multilang.config.KinesisClientLibConfigurator;

@RunWith(MockitoJUnitRunner.class)
public class MultiLangDaemonConfigTest {
    private static String FILENAME = "some.properties";
    private static String TestExe = "TestExe.exe";
    private static String TestApplicationName = "TestApp";
    private static String TestStreamName = "fakeStream";

    private static String TestStreamNameInArn = "FAKE_STREAM_NAME";
    private static String TestRegion = "us-east-1";

    private static String TestRegionInArn = "us-east-2";
    private static String TestStreamArn = "arn:aws:kinesis:us-east-2:ACCOUNT_ID:stream/FAKE_STREAM_NAME";

    @Mock
    ClassLoader classLoader;

    @Mock
    private AwsCredentialsProvider credentialsProvider;
    @Mock
    private AwsCredentials creds;
    @Mock
    private KinesisClientLibConfigurator configurator;

    public void setup(String streamName, String streamArn) {

        String PROPERTIES = String.format("executableName = %s \n"
                + "applicationName = %s \n"
                + "AWSCredentialsProvider = DefaultAWSCredentialsProviderChain\n"
                + "processingLanguage = malbolge\n"
                + "regionName = %s \n",
                TestExe,
                TestApplicationName,
                TestRegion);

        if(streamName != null){
            PROPERTIES += String.format("streamName = %s \n", streamName);
        }
        if(streamArn != null){
            PROPERTIES += String.format("streamArn = %s \n", streamArn);
        }
        classLoader = Mockito.mock(ClassLoader.class);

        Mockito.doReturn(new ByteArrayInputStream(PROPERTIES.getBytes())).when(classLoader)
                .getResourceAsStream(FILENAME);

        when(credentialsProvider.resolveCredentials()).thenReturn(creds);
        when(creds.accessKeyId()).thenReturn("cool-user");
        configurator = new KinesisClientLibConfigurator();

    }

    @Test
    public void testConstructorFailsBecauseStreamNameAndArnAreEmpty() throws IOException {
        setup("", "");

        assertThrows(Exception.class, () -> new MultiLangDaemonConfig(FILENAME, classLoader, configurator));
    }

    @Test
    public void testConstructorFailsBecauseStreamNameAndArnAreNull() {
        setup(null, null);

        assertThrows(Exception.class, () -> new MultiLangDaemonConfig(FILENAME, classLoader, configurator));
    }

    @Test
    public void testConstructorFailsBecauseStreamNameIsNullAndArnIsEmpty() {
        setup(null, "");

        assertThrows(Exception.class, () -> new MultiLangDaemonConfig(FILENAME, classLoader, configurator));
    }

    @Test
    public void testConstructorFailsBecauseStreamNameIsEmptyAndArnIsNull() {
        setup("", null);

        assertThrows(Exception.class, () -> new MultiLangDaemonConfig(FILENAME, classLoader, configurator));
    }

    @Test
    public void testConstructorUsingStreamName() throws IOException {
        setup(TestStreamName, null);

        MultiLangDaemonConfig deamonConfig = new MultiLangDaemonConfig(FILENAME, classLoader, configurator);

        AssertConfigurationsMatch(deamonConfig, TestExe, TestApplicationName, TestStreamName, TestRegion);
    }

    @Test
    public void testConstructorUsingStreamNameAndStreamArnIsEmpty() throws IOException {
        setup(TestStreamName, "");

        MultiLangDaemonConfig deamonConfig = new MultiLangDaemonConfig(FILENAME, classLoader, configurator);

        AssertConfigurationsMatch(deamonConfig, TestExe, TestApplicationName, TestStreamName, TestRegion);
    }

    @Test
    public void testConstructorUsingStreamArn() throws IOException {
        setup(null, TestStreamArn);

        MultiLangDaemonConfig deamonConfig = new MultiLangDaemonConfig(FILENAME, classLoader, configurator);

        AssertConfigurationsMatch(deamonConfig, TestExe, TestApplicationName, TestStreamNameInArn, TestRegionInArn);
    }

    @Test
    public void testConstructorUsingStreamNameAsEmptyAndStreamArn() throws IOException {
        setup("", TestStreamArn);

        MultiLangDaemonConfig deamonConfig = new MultiLangDaemonConfig(FILENAME, classLoader, configurator);

        AssertConfigurationsMatch(deamonConfig, TestExe, TestApplicationName, TestStreamNameInArn, TestRegionInArn);
    }

    @Test
    public void testConstructorUsingStreamArnOverStreamName() throws IOException {
        setup(TestStreamName, TestStreamArn);

        MultiLangDaemonConfig deamonConfig = new MultiLangDaemonConfig(FILENAME, classLoader, configurator);

        AssertConfigurationsMatch(deamonConfig, TestExe, TestApplicationName, TestStreamNameInArn, TestRegionInArn);
    }

    /**
     * Verify the daemonConfig properties are what we expect them to be.
     * @param deamonConfig
     * @param expectedStreamName
     */
    private void AssertConfigurationsMatch(MultiLangDaemonConfig deamonConfig,
                                           String expectedExe,
                                           String expectedApplicationName,
                                           String expectedStreamName,
                                           String expectedRegionName){
        assertNotNull(deamonConfig.getExecutorService());
        assertNotNull(deamonConfig.getMultiLangDaemonConfiguration());
        assertNotNull(deamonConfig.getRecordProcessorFactory());

        assertEquals(expectedExe, deamonConfig.getRecordProcessorFactory().getCommandArray()[0]);
        assertEquals(expectedApplicationName, deamonConfig.getMultiLangDaemonConfiguration().getApplicationName());
        assertEquals(expectedStreamName, deamonConfig.getMultiLangDaemonConfiguration().getStreamName());
        assertEquals(expectedRegionName, deamonConfig.getMultiLangDaemonConfiguration().getDynamoDbClient().get("region").toString());
        assertEquals(expectedRegionName, deamonConfig.getMultiLangDaemonConfiguration().getCloudWatchClient().get("region").toString());
        assertEquals(expectedRegionName, deamonConfig.getMultiLangDaemonConfiguration().getKinesisClient().get("region").toString());
    }

    @Test
    public void testPropertyValidation() {
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
