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
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.metrics.MetricsLevel;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
                    "AwsCredentialsProvider = " + credentialName1,
                    "workerId = 123"
                },
                '\n'));
        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "a");
        assertEquals(config.getWorkerIdentifier(), "123");
        assertThat(config.getMaxGetRecordsThreadPool(), nullValue());
        assertThat(config.getRetryGetRecordsInSeconds(), nullValue());
        assertNull(config.getGracefulLeaseHandoffTimeoutMillis());
        assertNull(config.getIsGracefulLeaseHandoffEnabled());
    }

    @Test
    public void testWithLongVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "applicationName = app",
                    "streamName = 123",
                    "AwsCredentialsProvider = " + credentialName1 + ", " + credentialName2,
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
                    "AwsCredentialsProvider = " + credentialName1 + ", " + credentialName2,
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
                        "AwsCredentialsProvider = " + credentialName1 + ", " + credentialName2,
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
                        "AwsCredentialsProvider = " + credentialName1 + ", " + credentialName2,
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
    public void testGracefulLeaseHandoffConfig() {
        final Long testGracefulLeaseHandoffTimeoutMillis = 12345L;
        final boolean testGracefulLeaseHandoffEnabled = true;

        final MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "applicationName = dummyApplicationName",
                    "streamName = dummyStreamName",
                    "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                    "gracefulLeaseHandoffTimeoutMillis = " + testGracefulLeaseHandoffTimeoutMillis,
                    "isGracefulLeaseHandoffEnabled = " + testGracefulLeaseHandoffEnabled
                },
                '\n'));

        assertEquals(testGracefulLeaseHandoffTimeoutMillis, config.getGracefulLeaseHandoffTimeoutMillis());
        assertEquals(testGracefulLeaseHandoffEnabled, config.getIsGracefulLeaseHandoffEnabled());
    }

    @Test
    public void testClientVersionConfig() {
        final CoordinatorConfig.ClientVersionConfig testClientVersionConfig = Arrays.stream(
                        CoordinatorConfig.ClientVersionConfig.values())
                .findAny()
                .orElseThrow(NoSuchElementException::new);

        final MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "applicationName = dummyApplicationName",
                    "streamName = dummyStreamName",
                    "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                    "clientVersionConfig = " + testClientVersionConfig.name()
                },
                '\n'));

        assertEquals(testClientVersionConfig, config.getClientVersionConfig());
    }

    @Test
    public void testCoordinatorStateConfig() {
        final String testCoordinatorStateTableName = "CoordState";
        final BillingMode testCoordinatorStateBillingMode = BillingMode.PAY_PER_REQUEST;
        final long testCoordinatorStateReadCapacity = 123;
        final long testCoordinatorStateWriteCapacity = 123;

        final MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "applicationName = dummyApplicationName",
                    "streamName = dummyStreamName",
                    "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                    "coordinatorStateTableName = " + testCoordinatorStateTableName,
                    "coordinatorStateBillingMode = " + testCoordinatorStateBillingMode.name(),
                    "coordinatorStateReadCapacity = " + testCoordinatorStateReadCapacity,
                    "coordinatorStateWriteCapacity = " + testCoordinatorStateWriteCapacity
                },
                '\n'));

        assertEquals(testCoordinatorStateTableName, config.getCoordinatorStateTableName());
        assertEquals(testCoordinatorStateBillingMode, config.getCoordinatorStateBillingMode());
        assertEquals(testCoordinatorStateReadCapacity, config.getCoordinatorStateReadCapacity());
        assertEquals(testCoordinatorStateWriteCapacity, config.getCoordinatorStateWriteCapacity());
    }

    @Test
    public void testWorkerUtilizationAwareAssignmentConfig() {
        final long testInMemoryWorkerMetricsCaptureFrequencyMillis = 123;
        final long testWorkerMetricsReporterFreqInMillis = 123;
        final long testNoOfPersistedMetricsPerWorkerMetrics = 123;
        final Boolean testDisableWorkerMetrics = true;
        final double testMaxThroughputPerHostKBps = 123;
        final long testDampeningPercentage = 12;
        final long testReBalanceThresholdPercentage = 12;
        final Boolean testAllowThroughputOvershoot = false;
        final long testVarianceBalancingFrequency = 12;
        final double testWorkerMetricsEMAAlpha = .123;

        final MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "applicationName = dummyApplicationName",
                    "streamName = dummyStreamName",
                    "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                    "inMemoryWorkerMetricsCaptureFrequencyMillis = " + testInMemoryWorkerMetricsCaptureFrequencyMillis,
                    "workerMetricsReporterFreqInMillis = " + testWorkerMetricsReporterFreqInMillis,
                    "noOfPersistedMetricsPerWorkerMetrics = " + testNoOfPersistedMetricsPerWorkerMetrics,
                    "disableWorkerMetrics = " + testDisableWorkerMetrics,
                    "maxThroughputPerHostKBps = " + testMaxThroughputPerHostKBps,
                    "dampeningPercentage = " + testDampeningPercentage,
                    "reBalanceThresholdPercentage = " + testReBalanceThresholdPercentage,
                    "allowThroughputOvershoot = " + testAllowThroughputOvershoot,
                    "varianceBalancingFrequency = " + testVarianceBalancingFrequency,
                    "workerMetricsEMAAlpha = " + testWorkerMetricsEMAAlpha
                },
                '\n'));

        assertEquals(
                testInMemoryWorkerMetricsCaptureFrequencyMillis,
                config.getInMemoryWorkerMetricsCaptureFrequencyMillis());
        assertEquals(testWorkerMetricsReporterFreqInMillis, config.getWorkerMetricsReporterFreqInMillis());
        assertEquals(testNoOfPersistedMetricsPerWorkerMetrics, config.getNoOfPersistedMetricsPerWorkerMetrics());
        assertEquals(testDisableWorkerMetrics, config.getDisableWorkerMetrics());
        assertEquals(testMaxThroughputPerHostKBps, config.getMaxThroughputPerHostKBps(), 0.0001);
        assertEquals(testDampeningPercentage, config.getDampeningPercentage());
        assertEquals(testReBalanceThresholdPercentage, config.getReBalanceThresholdPercentage());
        assertEquals(testAllowThroughputOvershoot, config.getAllowThroughputOvershoot());
        assertEquals(testVarianceBalancingFrequency, config.getVarianceBalancingFrequency());
        assertEquals(testWorkerMetricsEMAAlpha, config.getWorkerMetricsEMAAlpha(), 0.0001);
    }

    @Test
    public void testWorkerMetricsConfig() {
        final String testWorkerMetricsTableName = "CoordState";
        final BillingMode testWorkerMetricsBillingMode = BillingMode.PROVISIONED;
        final long testWorkerMetricsReadCapacity = 123;
        final long testWorkerMetricsWriteCapacity = 123;

        final MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "applicationName = dummyApplicationName",
                    "streamName = dummyStreamName",
                    "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                    "workerMetricsTableName = " + testWorkerMetricsTableName,
                    "workerMetricsBillingMode = " + testWorkerMetricsBillingMode.name(),
                    "workerMetricsReadCapacity = " + testWorkerMetricsReadCapacity,
                    "workerMetricsWriteCapacity = " + testWorkerMetricsWriteCapacity
                },
                '\n'));

        assertEquals(testWorkerMetricsTableName, config.getWorkerMetricsTableName());
        assertEquals(testWorkerMetricsBillingMode, config.getWorkerMetricsBillingMode());
        assertEquals(testWorkerMetricsReadCapacity, config.getWorkerMetricsReadCapacity());
        assertEquals(testWorkerMetricsWriteCapacity, config.getWorkerMetricsWriteCapacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidClientVersionConfig() {
        getConfiguration(StringUtils.join(
                new String[] {
                    "applicationName = dummyApplicationName",
                    "streamName = dummyStreamName",
                    "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                    "clientVersionConfig = " + "invalid_client_version_config"
                },
                '\n'));
    }

    @Test
    public void testWithUnsupportedClientConfigurationVariables() {
        MultiLangDaemonConfiguration config = getConfiguration(StringUtils.join(
                new String[] {
                    "AwsCredentialsProvider = " + credentialName1 + ", " + credentialName2,
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
                    "AwsCredentialsProvider = " + credentialName2 + ", " + credentialName1,
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
                    "AwsCredentialsProvider = ABCD, " + credentialName1,
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
                    "AwsCredentialsProvider = ABCD," + credentialName1,
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
                    "AwsCredentialsProvider = ABCD," + credentialName1,
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
                    "AwsCredentialsProvider = ABCD," + credentialName1,
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
                    "AwsCredentialsProvider = ABCD," + credentialName1,
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
                    "AwsCredentialsProvider = ABCD," + credentialName1,
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
                    "AwsCredentialsProvider = ABCD," + credentialName1,
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
                    "AwsCredentialsProvider = ABCD," + credentialName1,
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
                    "AwsCredentialsProvider = " + credentialName1,
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
                    "AwsCredentialsProvider = " + credentialName1,
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
                    "AwsCredentialsProvider = " + credentialName1,
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
                    "AwsCredentialsProvider = " + credentialName1,
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
                    "AwsCredentialsProvider = " + credentialName1,
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
                    "AwsCredentialsProvider = " + credentialName1,
                    "workerId = 123",
                    "failoverTimeMillis = 100"
                },
                '\n');
        getConfiguration(test);
    }

    @Test
    public void testWithAwsCredentialsFailed() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AwsCredentialsProvider = " + credentialName2,
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

    @Test
    public void testProcessKeyWithExpectedCasing() {
        String key = "AwsCredentialsProvider";
        String result = configurator.processKey(key);
        assertEquals("awsCredentialsProvider", result);
    }

    @Test
    public void testProcessKeyWithOldCasing() {
        String key = "AWSCredentialsProvider";
        String result = configurator.processKey(key);
        assertEquals("awsCredentialsProvider", result);
    }

    @Test
    public void testProcessKeyWithMixedCasing() {
        String key = "AwScReDeNtIaLsPrOvIdEr";
        String result = configurator.processKey(key);
        assertEquals("awsCredentialsProvider", result);
    }

    @Test
    public void testProcessKeyWithSuffix() {
        String key = "awscredentialsproviderDynamoDB";
        String result = configurator.processKey(key);
        assertEquals("awsCredentialsProviderDynamoDB", result);
    }

    // TODO: fix this test
    @Test
    public void testWithDifferentAwsCredentialsForDynamoDBAndCloudWatch() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AwsCredentialsProvider = " + credentialNameKinesis,
                    "AwsCredentialsProviderDynamoDB = " + credentialNameDynamoDB,
                    "AwsCredentialsProviderCloudWatch = " + credentialNameCloudWatch,
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
    public void testWithDifferentAwsCredentialsForDynamoDBAndCloudWatchFailed() {
        String test = StringUtils.join(
                new String[] {
                    "streamName = a",
                    "applicationName = b",
                    "AwsCredentialsProvider = " + credentialNameKinesis,
                    "AwsCredentialsProviderDynamoDB = " + credentialName2,
                    "AwsCredentialsProviderCloudWatch = " + credentialName2,
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
    public static class AlwaysSucceedCredentialsProvider implements AwsCredentialsProvider {
        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create("a", "b");
        }
    }

    /**
     * This credentials provider will always succeed
     */
    public static class AlwaysSucceedCredentialsProviderKinesis implements AwsCredentialsProvider {
        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create("DUMMY_ACCESS_KEY_ID", "DUMMY_SECRET_ACCESS_KEY");
        }
    }

    /**
     * This credentials provider will always succeed
     */
    public static class AlwaysSucceedCredentialsProviderDynamoDB implements AwsCredentialsProvider {
        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create("DUMMY_ACCESS_KEY_ID", "DUMMY_SECRET_ACCESS_KEY");
        }
    }

    /**
     * This credentials provider will always succeed
     */
    public static class AlwaysSucceedCredentialsProviderCloudWatch implements AwsCredentialsProvider {
        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create("DUMMY_ACCESS_KEY_ID", "DUMMY_SECRET_ACCESS_KEY");
        }
    }

    /**
     * This credentials provider will always fail
     */
    public static class AlwaysFailCredentialsProvider implements AwsCredentialsProvider {

        @Override
        public AwsCredentials resolveCredentials() {
            throw new IllegalArgumentException();
        }
    }

    private MultiLangDaemonConfiguration getConfiguration(String configString) {
        InputStream input = new ByteArrayInputStream(configString.getBytes());
        return configurator.getConfiguration(input);
    }
}
