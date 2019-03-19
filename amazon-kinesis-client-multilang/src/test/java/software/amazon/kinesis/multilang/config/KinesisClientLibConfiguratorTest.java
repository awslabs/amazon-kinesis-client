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
package software.amazon.kinesis.multilang.config;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

@RunWith(MockitoJUnitRunner.class)
public class KinesisClientLibConfiguratorTest {

    private String credentialName1 = "software.amazon.kinesis.multilang.config.KinesisClientLibConfiguratorTest$AlwaysSucceedCredentialsProvider";
    private String credentialName2 = "software.amazon.kinesis.multilang.config.KinesisClientLibConfiguratorTest$AlwaysFailCredentialsProvider";
    private String credentialNameKinesis = "software.amazon.kinesis.multilang.config.KinesisClientLibConfiguratorTest$AlwaysSucceedCredentialsProviderKinesis";
    private String credentialNameDynamoDB = "software.amazon.kinesis.multilang.config.KinesisClientLibConfiguratorTest$AlwaysSucceedCredentialsProviderDynamoDB";
    private String credentialNameCloudWatch = "software.amazon.kinesis.multilang.config.KinesisClientLibConfiguratorTest$AlwaysSucceedCredentialsProviderCloudWatch";
    private KinesisClientLibConfigurator configurator = new KinesisClientLibConfigurator();

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Mock
    private ShardRecordProcessorFactory shardRecordProcessorFactory;

    @Test
    public void testWithBasicSetup() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(new String[] { "streamName = a",
                "applicationName = b", "AWSCredentialsProvider = " + credentialName1, "workerId = 123" }, '\n'));
        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "a");
        assertEquals(config.getWorkerIdentifier(), "123");
        assertThat(config.getMaxGetRecordsThreadPool(), nullValue());
        assertThat(config.getRetryGetRecordsInSeconds(), nullValue());
    }

    @Test
    public void testWithLongVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(new String[] { "applicationName = app",
                "streamName = 123", "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                "workerId = 123", "failoverTimeMillis = 100", "shardSyncIntervalMillis = 500" }, '\n'));

        assertEquals(config.getApplicationName(), "app");
        assertEquals(config.getStreamName(), "123");
        assertEquals(config.getWorkerIdentifier(), "123");
        assertEquals(config.getFailoverTimeMillis(), 100);
        assertEquals(config.getShardSyncIntervalMillis(), 500);
    }

    @Test
    public void testWithUnsupportedClientConfigurationVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] { "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2, "workerId = id",
                        "kinesisClientConfig = {}", "streamName = stream", "applicationName = b" },
                '\n'));

        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "stream");
        assertEquals(config.getWorkerIdentifier(), "id");
        // by setting the configuration there is no effect on kinesisClientConfiguration variable.
    }

    @Test
    public void testWithIntVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(new String[] { "streamName = kinesis",
                "AWSCredentialsProvider = " + credentialName2 + ", " + credentialName1, "workerId = w123",
                "maxRecords = 10", "metricsMaxQueueSize = 20", "applicationName = kinesis",
                "retryGetRecordsInSeconds = 2", "maxGetRecordsThreadPool = 1" }, '\n'));

        assertEquals(config.getApplicationName(), "kinesis");
        assertEquals(config.getStreamName(), "kinesis");
        assertEquals(config.getWorkerIdentifier(), "w123");
        assertEquals(config.getMaxRecords(), 10);
        assertEquals(config.getMetricsMaxQueueSize(), 20);
        assertThat(config.getRetryGetRecordsInSeconds(), equalTo(2));
        assertThat(config.getMaxGetRecordsThreadPool(), equalTo(1));
    }

    @Test
    public void testWithBooleanVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(new String[] { "streamName = a",
                "applicationName = b", "AWSCredentialsProvider = ABCD, " + credentialName1, "workerId = 0",
                "cleanupLeasesUponShardCompletion = false", "validateSequenceNumberBeforeCheckpointing = true" },
                '\n'));

        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "a");
        assertEquals(config.getWorkerIdentifier(), "0");
        assertFalse(config.isCleanupLeasesUponShardCompletion());
        assertTrue(config.isValidateSequenceNumberBeforeCheckpointing());
    }

    @Test
    public void testWithStringVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(new String[] { "streamName = a",
                "applicationName = b", "AWSCredentialsProvider = ABCD," + credentialName1, "workerId = 1",
                "kinesisEndpoint = https://kinesis", "metricsLevel = SUMMARY" }, '\n'));

        assertEquals(config.getWorkerIdentifier(), "1");
        assertEquals(config.getKinesisClient().get("endpointOverride"), URI.create("https://kinesis"));
        assertEquals(config.getMetricsLevel(), MetricsLevel.SUMMARY);
    }

    @Test
    public void testWithSetVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(new String[] { "streamName = a",
                "applicationName = b", "AWSCredentialsProvider = ABCD," + credentialName1, "workerId = 1",
                "metricsEnabledDimensions = ShardId, WorkerIdentifier" }, '\n'));

        Set<String> expectedMetricsEnabledDimensions = ImmutableSet.<String> builder()
                .add("ShardId", "WorkerIdentifier").build();
        assertThat(new HashSet<>(Arrays.asList(config.getMetricsEnabledDimensions())), equalTo(expectedMetricsEnabledDimensions));
    }

    @Test
    public void testWithInitialPositionInStreamVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(new String[] { "streamName = a",
                "applicationName = b", "AWSCredentialsProvider = ABCD," + credentialName1, "workerId = 123",
                "initialPositionInStream = TriM_Horizon" }, '\n'));

        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
    }

    @Test
    public void testSkippingNonKCLVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(new String[] { "streamName = a",
                "applicationName = b", "AWSCredentialsProvider = ABCD," + credentialName1, "workerId = 123",
                "initialPositionInStream = TriM_Horizon", "abc = 1" }, '\n'));

        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "a");
        assertEquals(config.getWorkerIdentifier(), "123");
        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
    }

    @Test
    public void testEmptyOptionalVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(new String[] { "streamName = a",
                "applicationName = b", "AWSCredentialsProvider = ABCD," + credentialName1, "workerId = 123",
                "initialPositionInStream = TriM_Horizon", "maxGetRecordsThreadPool = 1" }, '\n'));
        assertThat(config.getMaxGetRecordsThreadPool(), equalTo(1));
        assertThat(config.getRetryGetRecordsInSeconds(), nullValue());
    }

    @Test
    public void testWithZeroValue() {
        String test = StringUtils.join(new String[] { "streamName = a", "applicationName = b",
                "AWSCredentialsProvider = ABCD," + credentialName1, "workerId = 123",
                "initialPositionInStream = TriM_Horizon", "maxGetRecordsThreadPool = 0",
                "retryGetRecordsInSeconds = 0" }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        try {
            configurator.getConfiguration(input);
        } catch (Exception e) {
            fail("Don't expect to fail on invalid variable value");

        }
    }

    @Test
    public void testWithInvalidIntValue() {
        String test = StringUtils.join(new String[] { "streamName = a", "applicationName = b",
                "AWSCredentialsProvider = " + credentialName1, "workerId = 123", "failoverTimeMillis = 100nf" }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        try {
            configurator.getConfiguration(input);
        } catch (Exception e) {
            fail("Don't expect to fail on invalid variable value");
        }
    }

    @Test
    public void testWithNegativeIntValue() {
        String test = StringUtils.join(new String[] { "streamName = a", "applicationName = b",
                "AWSCredentialsProvider = " + credentialName1, "workerId = 123", "failoverTimeMillis = -12" }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        try {
            configurator.getConfiguration(input);
        } catch (Exception e) {
            fail("Don't expect to fail on invalid variable value");
        }
    }

    @Test
    public void testWithMissingCredentialsProvider() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("A basic set of AWS credentials must be provided");

        String test = StringUtils.join(new String[] { "streamName = a", "applicationName = b", "workerId = 123",
                "failoverTimeMillis = 100", "shardSyncIntervalMillis = 500" }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        configurator.getConfiguration(input);
    }

    @Test
    public void testWithMissingWorkerId() {
        String test = StringUtils.join(
                new String[] { "streamName = a", "applicationName = b", "AWSCredentialsProvider = " + credentialName1,
                        "failoverTimeMillis = 100", "shardSyncIntervalMillis = 500" },
                '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());
        MultiLangDaemonConfiguration config = configurator.getConfiguration(input);

        // if workerId is not provided, configurator should assign one for it automatically
        assertNotNull(config.getWorkerIdentifier());
        assertFalse(config.getWorkerIdentifier().isEmpty());
    }

    @Test
    public void testWithMissingStreamName() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("Stream name is required");

        String test = StringUtils.join(new String[] { "applicationName = b",
                "AWSCredentialsProvider = " + credentialName1, "workerId = 123", "failoverTimeMillis = 100" }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        configurator.getConfiguration(input);
    }

    @Test
    public void testWithMissingApplicationName() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("Application name is required");

        String test = StringUtils.join(new String[] { "streamName = a", "AWSCredentialsProvider = " + credentialName1,
                "workerId = 123", "failoverTimeMillis = 100" }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());
        configurator.getConfiguration(input);
    }

    @Test
    public void testWithAWSCredentialsFailed() {
        String test = StringUtils.join(
                new String[] { "streamName = a", "applicationName = b", "AWSCredentialsProvider = " + credentialName2,
                        "failoverTimeMillis = 100", "shardSyncIntervalMillis = 500" },
                '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        try {
            MultiLangDaemonConfiguration config = configurator.getConfiguration(input);
            config.getKinesisCredentialsProvider().build(AwsCredentialsProvider.class).resolveCredentials();
            fail("expect failure with wrong credentials provider");
        } catch (Exception e) {
            // succeed
        }
    }

    // TODO: fix this test
    @Test
    public void testWithDifferentAWSCredentialsForDynamoDBAndCloudWatch() {
        String test = StringUtils.join(new String[] { "streamName = a", "applicationName = b",
                "AWSCredentialsProvider = " + credentialNameKinesis,
                "AWSCredentialsProviderDynamoDB = " + credentialNameDynamoDB,
                "AWSCredentialsProviderCloudWatch = " + credentialNameCloudWatch, "failoverTimeMillis = 100",
                "shardSyncIntervalMillis = 500" }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        MultiLangDaemonConfiguration config = configurator.getConfiguration(input);
        try {
            config.getKinesisCredentialsProvider().build(AwsCredentialsProvider.class).resolveCredentials();
        } catch (Exception e) {
            fail("Kinesis credential providers should not fail.");
        }
        try {
            config.getDynamoDBCredentialsProvider().build(AwsCredentialsProvider.class).resolveCredentials();
        } catch (Exception e) {
            fail("DynamoDB credential providers should not fail.");
        }
        try {
            config.getCloudWatchCredentialsProvider().build(AwsCredentialsProvider.class).resolveCredentials();
        } catch (Exception e) {
            fail("CloudWatch credential providers should not fail.");
        }
    }

    // TODO: fix this test
    @Test
    public void testWithDifferentAWSCredentialsForDynamoDBAndCloudWatchFailed() {
        String test = StringUtils.join(new String[] { "streamName = a", "applicationName = b",
                "AWSCredentialsProvider = " + credentialNameKinesis,
                "AWSCredentialsProviderDynamoDB = " + credentialName2,
                "AWSCredentialsProviderCloudWatch = " + credentialName2, "failoverTimeMillis = 100",
                "shardSyncIntervalMillis = 500" }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        MultiLangDaemonConfiguration config = configurator.getConfiguration(input);
        try {
            config.getKinesisCredentialsProvider().build(AwsCredentialsProvider.class).resolveCredentials();
        } catch (Exception e) {
            fail("Kinesis credential providers should not fail.");
        }
        try {
            config.getDynamoDBCredentialsProvider().build(AwsCredentialsProvider.class).resolveCredentials();
            fail("DynamoDB credential providers should fail.");
        } catch (Exception e) {
            // succeed
        }
        try {
            config.getCloudWatchCredentialsProvider().build(AwsCredentialsProvider.class).resolveCredentials();
            fail("CloudWatch credential providers should fail.");
        } catch (Exception e) {
            // succeed
        }
    }

    /**
     * This credentials provider will always succeed
     */
    public static class AlwaysSucceedCredentialsProvider implements AWSCredentialsProvider {

        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials("a", "b");
        }

        @Override
        public void refresh() {

        }
    }

    /**
     * This credentials provider will always succeed
     */
    public static class AlwaysSucceedCredentialsProviderKinesis implements AWSCredentialsProvider {

        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials("", "");
        }

        @Override
        public void refresh() {

        }
    }

    /**
     * This credentials provider will always succeed
     */
    public static class AlwaysSucceedCredentialsProviderDynamoDB implements AWSCredentialsProvider {

        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials("", "");
        }

        @Override
        public void refresh() {

        }
    }

    /**
     * This credentials provider will always succeed
     */
    public static class AlwaysSucceedCredentialsProviderCloudWatch implements AWSCredentialsProvider {

        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials("", "");
        }

        @Override
        public void refresh() {

        }
    }

    /**
     * This credentials provider will always fail
     */
    public static class AlwaysFailCredentialsProvider implements AWSCredentialsProvider {

        @Override
        public AWSCredentials getCredentials() {
            throw new IllegalArgumentException();
        }

        @Override
        public void refresh() {

        }
    }

    private MultiLangDaemonConfiguration getConfiguration(String configString) {
        InputStream input = new ByteArrayInputStream(configString.getBytes());
        MultiLangDaemonConfiguration config = configurator.getConfiguration(input);
        return config;
    }
}
