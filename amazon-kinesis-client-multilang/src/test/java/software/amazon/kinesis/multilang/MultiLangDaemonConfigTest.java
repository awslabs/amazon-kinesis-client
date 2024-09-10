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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.multilang.config.KinesisClientLibConfigurator;
import software.amazon.kinesis.multilang.config.MultiLangDaemonConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MultiLangDaemonConfigTest {
    private static final String FILENAME = "multilang.properties";
    private static final String EXE = "TestExe.exe";
    private static final String APPLICATION_NAME = MultiLangDaemonConfigTest.class.getSimpleName();
    private static final String STREAM_NAME = "fakeStream";
    private static final String STREAM_NAME_IN_ARN = "FAKE_STREAM_NAME";
    private static final Region REGION = Region.US_EAST_1;
    private static final String STREAM_ARN = "arn:aws:kinesis:us-east-2:012345678987:stream/" + STREAM_NAME_IN_ARN;

    @Mock
    private ClassLoader classLoader;

    @Mock
    private AwsCredentialsProvider credentialsProvider;

    @Mock
    private AwsCredentials creds;

    private final KinesisClientLibConfigurator configurator = new KinesisClientLibConfigurator();
    private MultiLangDaemonConfig deamonConfig;

    /**
     * Instantiate a MultiLangDaemonConfig object
     * @param streamName
     * @param streamArn
     * @throws IOException
     */
    public void setup(String streamName, String streamArn) throws IOException {
        String properties = String.format(
                "executableName = %s\n"
                        + "applicationName = %s\n"
                        + "AwsCredentialsProvider = DefaultCredentialsProvider\n"
                        + "processingLanguage = malbolge\n"
                        + "regionName = %s\n",
                EXE, APPLICATION_NAME, "us-east-1");

        if (streamName != null) {
            properties += String.format("streamName = %s\n", streamName);
        }
        if (streamArn != null) {
            properties += String.format("streamArn = %s\n", streamArn);
        }
        classLoader = Mockito.mock(ClassLoader.class);

        Mockito.doReturn(new ByteArrayInputStream(properties.getBytes()))
                .when(classLoader)
                .getResourceAsStream(FILENAME);

        when(credentialsProvider.resolveCredentials()).thenReturn(creds);
        when(creds.accessKeyId()).thenReturn("cool-user");
        deamonConfig = new MultiLangDaemonConfig(FILENAME, classLoader, configurator);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorFailsBecauseStreamArnIsInvalid() throws Exception {
        setup("", "this_is_not_a_valid_arn");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorFailsBecauseStreamArnIsInvalid2() throws Exception {
        setup("", "arn:aws:kinesis:us-east-2:ACCOUNT_ID:BadFormatting:stream/" + STREAM_NAME_IN_ARN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorFailsBecauseStreamNameAndArnAreEmpty() throws Exception {
        setup("", "");
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorFailsBecauseStreamNameAndArnAreNull() throws Exception {
        setup(null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorFailsBecauseStreamNameIsNullAndArnIsEmpty() throws Exception {
        setup(null, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorFailsBecauseStreamNameIsEmptyAndArnIsNull() throws Exception {
        setup("", null);
    }

    @Test
    public void testConstructorUsingStreamName() throws IOException {
        setup(STREAM_NAME, null);

        assertConfigurationsMatch(STREAM_NAME, null);
    }

    @Test
    public void testConstructorUsingStreamNameAndStreamArnIsEmpty() throws IOException {
        setup(STREAM_NAME, "");

        assertConfigurationsMatch(STREAM_NAME, "");
    }

    @Test
    public void testConstructorUsingStreamNameAndStreamArnIsWhitespace() throws IOException {
        setup(STREAM_NAME, "   ");

        assertConfigurationsMatch(STREAM_NAME, "");
    }

    @Test
    public void testConstructorUsingStreamArn() throws IOException {
        setup(null, STREAM_ARN);

        assertConfigurationsMatch(STREAM_NAME_IN_ARN, STREAM_ARN);
    }

    @Test
    public void testConstructorUsingStreamNameAsEmptyAndStreamArn() throws IOException {
        setup("", STREAM_ARN);

        assertConfigurationsMatch(STREAM_NAME_IN_ARN, STREAM_ARN);
    }

    @Test
    public void testConstructorUsingStreamArnOverStreamName() throws IOException {
        setup(STREAM_NAME, STREAM_ARN);

        assertConfigurationsMatch(STREAM_NAME_IN_ARN, STREAM_ARN);
    }

    /**
     * Verify the daemonConfig properties are what we expect them to be.
     *
     * @param expectedStreamName
     */
    private void assertConfigurationsMatch(String expectedStreamName, String expectedStreamArn) {
        final MultiLangDaemonConfiguration multiLangConfiguration = deamonConfig.getMultiLangDaemonConfiguration();
        assertNotNull(deamonConfig.getExecutorService());
        assertNotNull(multiLangConfiguration);
        assertNotNull(deamonConfig.getRecordProcessorFactory());

        assertEquals(EXE, deamonConfig.getRecordProcessorFactory().getCommandArray()[0]);
        assertEquals(APPLICATION_NAME, multiLangConfiguration.getApplicationName());
        assertEquals(expectedStreamName, multiLangConfiguration.getStreamName());
        assertEquals(REGION, multiLangConfiguration.getDynamoDbClient().get("region"));
        assertEquals(REGION, multiLangConfiguration.getCloudWatchClient().get("region"));
        assertEquals(REGION, multiLangConfiguration.getKinesisClient().get("region"));
        assertEquals(expectedStreamArn, multiLangConfiguration.getStreamArn());
    }

    @Test
    public void testPropertyValidation() {
        String propertiesNoExecutableName = "applicationName = testApp \n" + "streamName = fakeStream \n"
                + "AwsCredentialsProvider = DefaultCredentialsProvider\n" + "processingLanguage = malbolge";
        ClassLoader classLoader = Mockito.mock(ClassLoader.class);

        Mockito.doReturn(new ByteArrayInputStream(propertiesNoExecutableName.getBytes()))
                .when(classLoader)
                .getResourceAsStream(FILENAME);

        try {
            new MultiLangDaemonConfig(FILENAME, classLoader, configurator);
            Assert.fail("Construction of the config should have failed due to property validation failing.");
        } catch (IllegalArgumentException e) {
            // Good
        } catch (IOException e) {
            Assert.fail();
        }
    }

    /**
     * Test the loading of a "real" properties file. This test should catch
     * any issues which might arise if there is a discrepancy between reality
     * and mocking.
     */
    @Test
    public void testActualPropertiesFile() throws Exception {
        new MultiLangDaemonConfig(FILENAME);
    }
}
