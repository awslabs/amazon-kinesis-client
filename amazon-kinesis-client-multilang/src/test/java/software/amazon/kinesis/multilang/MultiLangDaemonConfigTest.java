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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
    private static final String TEST_FILENAME = "some.properties";
    private static final String TEST_EXE = "TestExe.exe";
    private static final String TEST_APPLICATION_NAME = "TestApp";
    private static final String TEST_STREAM_NAME = "fakeStream";
    private static final String TEST_STREAM_NAME_IN_ARN = "FAKE_STREAM_NAME";
    private static final String TEST_REGION = "us-east-1";
    private static final String TEST_STREAM_ARN = "arn:aws:kinesis:us-east-2:012345678987:stream/" + TEST_STREAM_NAME_IN_ARN;

    @Mock
    ClassLoader classLoader;

    @Mock
    private AwsCredentialsProvider credentialsProvider;
    @Mock
    private AwsCredentials creds;

    private KinesisClientLibConfigurator configurator;
    private MultiLangDaemonConfig deamonConfig;

    /**
     * Instantiate a MultiLangDaemonConfig object based on the properties passed in.
     * @param streamName
     * @param streamArn
     * @param regionName
     * @throws IOException
     */
    public void setup(String streamName, String streamArn, String regionName) throws IOException {

        String properties = String.format("executableName = %s\n"
                        + "applicationName = %s\n"
                        + "AWSCredentialsProvider = DefaultAWSCredentialsProviderChain\n"
                        + "processingLanguage = malbolge\n"
                        + "regionName = %s\n",
                TEST_EXE,
                TEST_APPLICATION_NAME,
                regionName);

        if (streamName != null) {
            properties += String.format("streamName = %s\n", streamName);
        }
        if (streamArn != null) {
            properties += String.format("streamArn = %s\n", streamArn);
        }
        classLoader = Mockito.mock(ClassLoader.class);

        Mockito.doReturn(new ByteArrayInputStream(properties.getBytes())).when(classLoader)
                .getResourceAsStream(TEST_FILENAME);

        when(credentialsProvider.resolveCredentials()).thenReturn(creds);
        when(creds.accessKeyId()).thenReturn("cool-user");
        configurator = new KinesisClientLibConfigurator();

        deamonConfig = new MultiLangDaemonConfig(TEST_FILENAME, classLoader, configurator);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void testConstructorFailsBecauseStreamArnIsInvalid() throws Exception {
        setup("", "this_is_not_a_valid_arn", TEST_REGION);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void testConstructorFailsBecauseStreamArnIsInvalid2() throws Exception {
        setup("", "arn:aws:kinesis:us-east-2:ACCOUNT_ID:BadFormatting:stream/" + TEST_STREAM_NAME_IN_ARN, TEST_REGION);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void testConstructorFailsBecauseStreamNameAndArnAreEmpty() throws Exception {
        setup("", "", TEST_REGION);
    }

    @Test(expected =  NullPointerException.class)
    public void testConstructorFailsBecauseStreamNameAndArnAreNull() throws Exception {
        setup(null, null, TEST_REGION);
    }

    @Test(expected =  NullPointerException.class)
    public void testConstructorFailsBecauseStreamNameIsNullAndArnIsEmpty() throws Exception {
        setup(null, "", TEST_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorFailsBecauseStreamNameIsEmptyAndArnIsNull() throws Exception {
        setup("", null, TEST_REGION);
    }

    @Test
    public void testConstructorUsingStreamName() throws IOException {
        setup(TEST_STREAM_NAME, null, TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_STREAM_NAME, null);
    }

    @Test
    public void testConstructorUsingStreamNameAndStreamArnIsEmpty() throws IOException {
        setup(TEST_STREAM_NAME, "", TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_STREAM_NAME, "");
    }

    @Test
    public void testConstructorUsingStreamNameAndStreamArnIsWhitespace() throws IOException {
        setup(TEST_STREAM_NAME, "   ", TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_STREAM_NAME, "");
    }

    @Test
    public void testConstructorUsingStreamArn() throws IOException {
        setup(null, TEST_STREAM_ARN, TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_STREAM_NAME_IN_ARN, TEST_STREAM_ARN);
    }

    @Test
    public void testConstructorUsingStreamNameAsEmptyAndStreamArn() throws IOException {
        setup("", TEST_STREAM_ARN, TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_STREAM_NAME_IN_ARN, TEST_STREAM_ARN);
    }

    @Test
    public void testConstructorUsingStreamArnOverStreamName() throws IOException {
        setup(TEST_STREAM_NAME, TEST_STREAM_ARN, TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_STREAM_NAME_IN_ARN, TEST_STREAM_ARN);
    }

    /**
     * Verify the daemonConfig properties are what we expect them to be.
     * @param deamonConfig
     * @param expectedStreamName
     */
    private void assertConfigurationsMatch(MultiLangDaemonConfig deamonConfig,
                                           String expectedStreamName,
                                           String expectedStreamArn){
        assertNotNull(deamonConfig.getExecutorService());
        assertNotNull(deamonConfig.getMultiLangDaemonConfiguration());
        assertNotNull(deamonConfig.getRecordProcessorFactory());

        assertEquals(TEST_EXE, deamonConfig.getRecordProcessorFactory().getCommandArray()[0]);
        assertEquals(TEST_APPLICATION_NAME, deamonConfig.getMultiLangDaemonConfiguration().getApplicationName());
        assertEquals(expectedStreamName, deamonConfig.getMultiLangDaemonConfiguration().getStreamName());
        assertEquals(TEST_REGION, deamonConfig.getMultiLangDaemonConfiguration().getDynamoDbClient().get("region").toString());
        assertEquals(TEST_REGION, deamonConfig.getMultiLangDaemonConfiguration().getCloudWatchClient().get("region").toString());
        assertEquals(TEST_REGION, deamonConfig.getMultiLangDaemonConfiguration().getKinesisClient().get("region").toString());
        assertEquals(expectedStreamArn, deamonConfig.getMultiLangDaemonConfiguration().getStreamArn());
    }

    @Test
    public void testPropertyValidation() {
        String PROPERTIES_NO_EXECUTABLE_NAME = "applicationName = testApp \n" + "streamName = fakeStream \n"
                + "AWSCredentialsProvider = DefaultAWSCredentialsProviderChain\n" + "processingLanguage = malbolge";
        ClassLoader classLoader = Mockito.mock(ClassLoader.class);

        Mockito.doReturn(new ByteArrayInputStream(PROPERTIES_NO_EXECUTABLE_NAME.getBytes())).when(classLoader)
                .getResourceAsStream(TEST_FILENAME);

        MultiLangDaemonConfig config;
        try {
            config = new MultiLangDaemonConfig(TEST_FILENAME, classLoader, configurator);
            Assert.fail("Construction of the config should have failed due to property validation failing.");
        } catch (IllegalArgumentException e) {
            // Good
        } catch (IOException e) {
            Assert.fail();
        }
    }

}