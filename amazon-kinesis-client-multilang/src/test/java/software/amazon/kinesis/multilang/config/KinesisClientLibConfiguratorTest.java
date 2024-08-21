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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.metrics.MetricsLevel;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class KinesisClientLibConfiguratorTest {

    private final String credentialName1 = AlwaysSucceedCredentialsProvider.class.getName();
    private final String credentialName2 = AlwaysFailCredentialsProvider.class.getName();
    private final String credentialNameKinesis = AlwaysSucceedCredentialsProviderKinesis.class.getName();
    private final String credentialNameDynamoDB = AlwaysSucceedCredentialsProviderDynamoDB.class.getName();
    private final String credentialNameCloudWatch = AlwaysSucceedCredentialsProviderCloudWatch.class.getName();
    private final KinesisClientLibConfigurator configurator = new KinesisClientLibConfigurator();

    @Test
    public void testWithBasicSetup() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = " + credentialName1,
                    "workerId = 123"
                },
                '\n'));
        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "a");
        assertEquals(config.getWorkerIdentifier(), "123");
        assertThat(config.getMaxGetRecordsThreadPool(), nullValue());
        assertThat(config.getRetryGetRecordsInSeconds(), nullValue());
    }

    @Test
    public void testWithLongVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "applicationName = app",
                    "streamName = 123",
                    "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                    "workerId = 123",
                    "failoverTimeMillis = 100",
                    "shardSyncIntervalMillis = 500"
                },
                '\n'));

        assertEquals(config.getApplicationName(), "app");
        assertEquals(config.getStreamName(), "123");
        assertEquals(config.getWorkerIdentifier(), "123");
        assertEquals(config.getFailoverTimeMillis(), 100);
        assertEquals(config.getShardSyncIntervalMillis(), 500);
    }

    @Test
    public void testWithInitialPositionInStreamExtended() {
        long epochTimeInSeconds = 1617406032;
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "applicationName = app",
                    "streamName = 123",
                    "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                    "initialPositionInStreamExtended = " + epochTimeInSeconds
                },
                '\n'));

        assertEquals(config.getInitialPositionInStreamExtended().getTimestamp(), new Date(epochTimeInSeconds * 1000L));
        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.AT_TIMESTAMP);
    }

    @Test
    public void testInvalidInitialPositionInStream() {
        // AT_TIMESTAMP cannot be used as initialPositionInStream. If a user wants to specify AT_TIMESTAMP,
        // they must specify the time with initialPositionInStreamExtended.
        try {
            getConfiguration(StringUtils.join(
                    new String[] {
                        "applicationName = app",
                        "streamName = 123",
                        "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                        "initialPositionInStream = AT_TIMESTAMP"
                    },
                    '\n'));
            fail("Should have thrown when initialPositionInStream is set to AT_TIMESTAMP");
        } catch (Exception e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testInvalidInitialPositionInStreamExtended() {
        // initialPositionInStreamExtended takes a long value indicating seconds since epoch. If a non-long
        // value is provided, the constructor should throw an IllegalArgumentException exception.
        try {
            getConfiguration(StringUtils.join(
                    new String[] {
                        "applicationName = app",
                        "streamName = 123",
                        "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                        "initialPositionInStreamExtended = null"
                    },
                    '\n'));
            fail("Should have thrown when initialPositionInStreamExtended is set to null");
        } catch (Exception e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testWithUnsupportedClientConfigurationVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                    "workerId = id",
                    "kinesisClientConfig = {}",
                    "streamName = stream",
                    "applicationName = b"
                },
                '\n'));

        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "stream");
        assertEquals(config.getWorkerIdentifier(), "id");
        // by setting the configuration there is no effect on kinesisClientConfiguration variable.
    }

    @Test
    public void testWithIntVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "streamName = kinesis",
                    "AWSCredentialsProvider = " + credentialName2 + ", " + credentialName1,
                    "workerId = w123",
                    "maxRecords = 10",
                    "metricsMaxQueueSize = 20",
                    "applicationName = kinesis",
                    "retryGetRecordsInSeconds = 2",
                    "maxGetRecordsThreadPool = 1"
                },
                '\n'));

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
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = ABCD, " + credentialName1,
                    "workerId = 0",
                    "cleanupLeasesUponShardCompletion = false",
                    "validateSequenceNumberBeforeCheckpointing = true"
                },
                '\n'));

        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "a");
        assertEquals(config.getWorkerIdentifier(), "0");
        assertFalse(config.isCleanupLeasesUponShardCompletion());
        assertTrue(config.isValidateSequenceNumberBeforeCheckpointing());
    }

    @Test
    public void testWithStringVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = ABCD," + credentialName1,
                    "workerId = 1",
                    "kinesisEndpoint = https://kinesis",
                    "metricsLevel = SUMMARY"
                },
                '\n'));

        assertEquals(config.getWorkerIdentifier(), "1");
        assertEquals(config.getKinesisClient().get("endpointOverride"), URI.create("https://kinesis"));
        assertEquals(config.getMetricsLevel(), MetricsLevel.SUMMARY);
    }

    @Test
    public void testWithSetVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = ABCD," + credentialName1,
                    "workerId = 1",
                    "metricsEnabledDimensions = ShardId, WorkerIdentifier"
                },
                '\n'));

        Set<String> expectedMetricsEnabledDimensions = ImmutableSet.<String>builder()
                .add("ShardId", "WorkerIdentifier")
                .build();
        assertThat(
                new HashSet<>(Arrays.asList(config.getMetricsEnabledDimensions())),
                equalTo(expectedMetricsEnabledDimensions));
    }

    @Test
    public void testWithInitialPositionInStreamTrimHorizon() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = ABCD," + credentialName1,
                    "workerId = 123",
                    "initialPositionInStream = TriM_Horizon"
                },
                '\n'));

        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
    }

    @Test
    public void testWithInitialPositionInStreamLatest() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = ABCD," + credentialName1,
                    "workerId = 123",
                    "initialPositionInStream = LateSt"
                },
                '\n'));

        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.LATEST);
    }

    @Test
    public void testSkippingNonKCLVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = ABCD," + credentialName1,
                    "workerId = 123",
                    "initialPositionInStream = TriM_Horizon",
                    "abc = 1"
                },
                '\n'));

        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "a");
        assertEquals(config.getWorkerIdentifier(), "123");
        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
    }

    @Test
    public void testEmptyOptionalVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = ABCD," + credentialName1,
                    "workerId = 123",
                    "initialPositionInStream = TriM_Horizon",
                    "maxGetRecordsThreadPool = 1"
                },
                '\n'));
        assertThat(config.getMaxGetRecordsThreadPool(), equalTo(1));
        assertThat(config.getRetryGetRecordsInSeconds(), nullValue());
    }

    @Test
    public void testWithZeroValue() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = ABCD," + credentialName1,
                    "workerId = 123",
                    "initialPositionInStream = TriM_Horizon",
                    "maxGetRecordsThreadPool = 0",
                    "retryGetRecordsInSeconds = 0"
                },
                '\n');
        getConfiguration(test);
    }

    @Test
    public void testWithInvalidIntValue() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = " + credentialName1,
                    "workerId = 123",
                    "failoverTimeMillis = 100nf"
                },
                '\n');
        getConfiguration(test);
    }

    @Test
    public void testWithNegativeIntValue() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = " + credentialName1,
                    "workerId = 123",
                    "failoverTimeMillis = -12"
                },
                '\n');

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        getConfiguration(test);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithMissingCredentialsProvider() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "workerId = 123",
                    "failoverTimeMillis = 100",
                    "shardSyncIntervalMillis = 500"
                },
                '\n');

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        getConfiguration(test);
    }

    @Test
    public void testWithMissingWorkerId() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = " + credentialName1,
                    "failoverTimeMillis = 100",
                    "shardSyncIntervalMillis = 500"
                },
                '\n');
        MultiLangDaemonConfiguration config = getConfiguration(test);

        // if workerId is not provided, configurator should assign one for it automatically
        assertNotNull(config.getWorkerIdentifier());
        assertFalse(config.getWorkerIdentifier().isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testWithMissingStreamNameAndMissingStreamArn() {
        String test = StringUtils.join(
                new String[] {
                    "applicationName = b",
                    "AWSCredentialsProvider = " + credentialName1,
                    "workerId = 123",
                    "failoverTimeMillis = 100"
                },
                '\n');
        getConfiguration(test);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithEmptyStreamNameAndMissingStreamArn() {
        String test = StringUtils.join(
                new String[] {
                    "applicationName = b",
                    "AWSCredentialsProvider = " + credentialName1,
                    "workerId = 123",
                    "failoverTimeMillis = 100",
                    "streamName = ",
                    "streamArn = "
                },
                '\n');
        getConfiguration(test);
    }

    @Test(expected = NullPointerException.class)
    public void testWithMissingApplicationName() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "AWSCredentialsProvider = " + credentialName1,
                    "workerId = 123",
                    "failoverTimeMillis = 100"
                },
                '\n');
        getConfiguration(test);
    }

    @Test
    public void testWithAWSCredentialsFailed() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = " + credentialName2,
                    "failoverTimeMillis = 100",
                    "shardSyncIntervalMillis = 500"
                },
                '\n');
        MultiLangDaemonConfiguration config = getConfiguration(test);

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        try {
            config.getKinesisCredentialsProvider()
                    .build(AwsCredentialsProvider.class)
                    .resolveCredentials();
            fail("expect failure with wrong credentials provider");
        } catch (Exception e) {
            // succeed
        }
    }

    // TODO: fix this test
    @Test
    public void testWithDifferentAWSCredentialsForDynamoDBAndCloudWatch() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = " + credentialNameKinesis,
                    "AWSCredentialsProviderDynamoDB = " + credentialNameDynamoDB,
                    "AWSCredentialsProviderCloudWatch = " + credentialNameCloudWatch,
                    "failoverTimeMillis = 100",
                    "shardSyncIntervalMillis = 500"
                },
                '\n');

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        final MultiLangDaemonConfiguration config = getConfiguration(test);
        config.getKinesisCredentialsProvider()
                .build(AwsCredentialsProvider.class)
                .resolveCredentials();
        config.getDynamoDBCredentialsProvider()
                .build(AwsCredentialsProvider.class)
                .resolveCredentials();
        config.getCloudWatchCredentialsProvider()
                .build(AwsCredentialsProvider.class)
                .resolveCredentials();
    }

    // TODO: fix this test
    @Test
    public void testWithDifferentAWSCredentialsForDynamoDBAndCloudWatchFailed() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AWSCredentialsProvider = " + credentialNameKinesis,
                    "AWSCredentialsProviderDynamoDB = " + credentialName2,
                    "AWSCredentialsProviderCloudWatch = " + credentialName2,
                    "failoverTimeMillis = 100",
                    "shardSyncIntervalMillis = 500"
                },
                '\n');

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        final MultiLangDaemonConfiguration config = getConfiguration(test);
        config.getKinesisCredentialsProvider()
                .build(AwsCredentialsProvider.class)
                .resolveCredentials();
        try {
            config.getDynamoDBCredentialsProvider()
                    .build(AwsCredentialsProvider.class)
                    .resolveCredentials();
            fail("DynamoDB credential providers should fail.");
        } catch (Exception e) {
            // succeed
        }
        try {
            config.getCloudWatchCredentialsProvider()
                    .build(AwsCredentialsProvider.class)
                    .resolveCredentials();
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
        public void refresh() {}
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
        public void refresh() {}
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
        public void refresh() {}
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
        public void refresh() {}
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
        public void refresh() {}
    }

    private MultiLangDaemonConfiguration getConfiguration(String configString) {
        InputStream input = new ByteArrayInputStream(configString.getBytes());
        return configurator.getConfiguration(input);
    }
}
