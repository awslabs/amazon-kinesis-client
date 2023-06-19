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
    private static final String TEST_REGION_IN_ARN = "us-east-2";

    private static String getTestStreamArn(){
        return String.format("arn:aws:kinesis:%s:ACCOUNT_ID:stream/%s", TEST_REGION_IN_ARN, TEST_STREAM_NAME_IN_ARN);
    }

    @Mock
    ClassLoader classLoader;

    @Mock
    private AwsCredentialsProvider credentialsProvider;
    @Mock
    private AwsCredentials creds;

    private KinesisClientLibConfigurator configurator;
    private MultiLangDaemonConfig deamonConfig;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

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

    @Test
    public void testConstructorFailsBecauseStreamArnIsInvalid() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Malformed ARN - doesn't start with 'arn:");

        setup("", "this_is_not_a_valid_arn", TEST_REGION);
    }

    @Test
    public void testConstructorFailsBecauseStreamArnHasInvalidRegion() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Unsupported region: us-east-1000");

        setup("", "arn:aws:kinesis:us-east-1:ACCOUNT_ID:stream/streamName", "us-east-1000");
    }

    @Test
    public void testConstructorFailsBecauseStreamArnHasInvalidResourceType() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("StreamArn has unsupported resource type of 'dynamodb'. Expected: stream");

        setup("", "arn:aws:kinesis:us-EAST-1:ACCOUNT_ID:dynamodb/streamName", TEST_REGION);
    }

    @Test
    public void testConstructorFailsBecauseStreamArnHasInvalidService() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("StreamArn has unsupported service type of 'kinesisFakeService'. Expected: kinesis");

        setup("", "arn:aws:kinesisFakeService:us-east-1:ACCOUNT_ID:stream/streamName", TEST_REGION);
    }

    @Test
    public void testConstructorFailsBecauseStreamNameAndArnAreEmpty() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Stream name or Stream Arn is required. Stream Arn takes precedence if both are passed in.");

        setup("", "", TEST_REGION);
    }

    @Test
    public void testConstructorFailsBecauseStreamNameAndArnAreNull() throws Exception {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("Stream name or Stream Arn is required. Stream Arn takes precedence if both are passed in.");

        setup(null, null, TEST_REGION);
    }

    @Test
    public void testConstructorFailsBecauseStreamNameIsNullAndArnIsEmpty() throws Exception {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("Stream name or Stream Arn is required. Stream Arn takes precedence if both are passed in.");

        setup(null, "", TEST_REGION);
    }

    @Test
    public void testConstructorFailsBecauseStreamNameIsEmptyAndArnIsNull() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Stream name or Stream Arn is required. Stream Arn takes precedence if both are passed in.");

        setup("", null, TEST_REGION);
    }

    @Test
    public void testConstructorUsingStreamName() throws IOException {
        setup(TEST_STREAM_NAME, null, TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_EXE, TEST_APPLICATION_NAME, TEST_STREAM_NAME, TEST_REGION, null);
    }

    @Test
    public void testConstructorUsingStreamNameAndStreamArnIsEmpty() throws IOException {
        setup(TEST_STREAM_NAME, "", TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_EXE, TEST_APPLICATION_NAME, TEST_STREAM_NAME, TEST_REGION, "");
    }

    @Test
    public void testConstructorUsingStreamNameAndStreamArnIsWhitespace() throws IOException {
        setup(TEST_STREAM_NAME, "   ", TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_EXE, TEST_APPLICATION_NAME, TEST_STREAM_NAME, TEST_REGION, "");
    }

    @Test
    public void testConstructorUsingStreamArn() throws IOException {
        setup(null, getTestStreamArn(), TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_EXE, TEST_APPLICATION_NAME, TEST_STREAM_NAME_IN_ARN, TEST_REGION, getTestStreamArn());
    }

    @Test
    public void testConstructorUsingStreamNameAsEmptyAndStreamArn() throws IOException {
        setup("", getTestStreamArn(), TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_EXE, TEST_APPLICATION_NAME, TEST_STREAM_NAME_IN_ARN, TEST_REGION, getTestStreamArn());
    }

    @Test
    public void testConstructorUsingStreamArnOverStreamName() throws IOException {
        setup(TEST_STREAM_NAME, getTestStreamArn(), TEST_REGION);

        assertConfigurationsMatch(deamonConfig, TEST_EXE, TEST_APPLICATION_NAME, TEST_STREAM_NAME_IN_ARN, TEST_REGION, getTestStreamArn());
    }

    /**
     * Verify the daemonConfig properties are what we expect them to be.
     * @param deamonConfig
     * @param expectedStreamName
     */
    private void assertConfigurationsMatch(MultiLangDaemonConfig deamonConfig,
                                           String expectedExe,
                                           String expectedApplicationName,
                                           String expectedStreamName,
                                           String expectedRegionName,
                                           String expectedStreamArn){
        assertNotNull(deamonConfig.getExecutorService());
        assertNotNull(deamonConfig.getMultiLangDaemonConfiguration());
        assertNotNull(deamonConfig.getRecordProcessorFactory());

        assertEquals(expectedExe, deamonConfig.getRecordProcessorFactory().getCommandArray()[0]);
        assertEquals(expectedApplicationName, deamonConfig.getMultiLangDaemonConfiguration().getApplicationName());
        assertEquals(expectedStreamName, deamonConfig.getMultiLangDaemonConfiguration().getStreamName());
        assertEquals(expectedRegionName, deamonConfig.getMultiLangDaemonConfiguration().getDynamoDbClient().get("region").toString());
        assertEquals(expectedRegionName, deamonConfig.getMultiLangDaemonConfiguration().getCloudWatchClient().get("region").toString());
        assertEquals(expectedRegionName, deamonConfig.getMultiLangDaemonConfiguration().getKinesisClient().get("region").toString());
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