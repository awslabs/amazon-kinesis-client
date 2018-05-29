/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.google.common.collect.ImmutableSet;

public class KinesisClientLibConfiguratorTest {

    private String credentialName1 =
            "com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfiguratorTest$AlwaysSucceedCredentialsProvider";
    private String credentialName2 =
            "com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfiguratorTest$AlwaysFailCredentialsProvider";
    private String credentialNameKinesis =
            "com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfiguratorTest$AlwaysSucceedCredentialsProviderKinesis";
    private String credentialNameDynamoDB =
            "com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfiguratorTest$AlwaysSucceedCredentialsProviderDynamoDB";
    private String credentialNameCloudWatch =
            "com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfiguratorTest$AlwaysSucceedCredentialsProviderCloudWatch";
    private KinesisClientLibConfigurator configurator = new KinesisClientLibConfigurator();

    @Test
    public void testWithBasicSetup() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = " + credentialName1,
                        "workerId = 123"
                }, '\n'));
        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "a");
        assertEquals(config.getWorkerIdentifier(), "123");
        assertEquals(config.getMaxGetRecordsThreadPool(), Optional.empty());
        assertEquals(config.getRetryGetRecordsInSeconds(), Optional.empty());
    }

    @Test
    public void testWithLongVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "applicationName = app",
                        "streamName = 123",
                        "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                        "workerId = 123",
                        "failoverTimeMillis = 100",
                        "shardSyncIntervalMillis = 500"
                }, '\n'));

        assertEquals(config.getApplicationName(), "app");
        assertEquals(config.getStreamName(), "123");
        assertEquals(config.getWorkerIdentifier(), "123");
        assertEquals(config.getFailoverTimeMillis(), 100);
        assertEquals(config.getShardSyncIntervalMillis(), 500);
    }

    @Test
    public void testWithUnsupportedClientConfigurationVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "AWSCredentialsProvider = " + credentialName1 + ", " + credentialName2,
                        "workerId = id",
                        "kinesisClientConfig = {}",
                        "streamName = stream",
                        "applicationName = b"
                }, '\n'));

        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "stream");
        assertEquals(config.getWorkerIdentifier(), "id");
        // by setting the configuration there is no effect on kinesisClientConfiguration variable.
    }

    @Test
    public void testWithIntVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = kinesis",
                        "AWSCredentialsProvider = " + credentialName2 + ", " + credentialName1,
                        "workerId = w123",
                        "maxRecords = 10",
                        "metricsMaxQueueSize = 20",
                        "applicationName = kinesis",
                        "retryGetRecordsInSeconds = 2",
                        "maxGetRecordsThreadPool = 1"
                }, '\n'));

        assertEquals(config.getApplicationName(), "kinesis");
        assertEquals(config.getStreamName(), "kinesis");
        assertEquals(config.getWorkerIdentifier(), "w123");
        assertEquals(config.getMaxRecords(), 10);
        assertEquals(config.getMetricsMaxQueueSize(), 20);
        assertEquals(config.getRetryGetRecordsInSeconds(), Optional.of(2));
        assertEquals(config.getMaxGetRecordsThreadPool(), Optional.of(1));
    }

    @Test
    public void testWithBooleanVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD, " + credentialName1,
                        "workerId = 0",
                        "cleanupLeasesUponShardCompletion = false",
                        "validateSequenceNumberBeforeCheckpointing = true"
                }, '\n'));

        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "a");
        assertEquals(config.getWorkerIdentifier(), "0");
        assertFalse(config.shouldCleanupLeasesUponShardCompletion());
        assertTrue(config.shouldValidateSequenceNumberBeforeCheckpointing());
    }

    @Test
    public void testWithDateVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD, " + credentialName1,
                        "timestampAtInitialPositionInStream = 1527267472"
                }, '\n'));

        assertEquals(config.getTimestampAtInitialPositionInStream(), 
                new Date(1527267472 * 1000L));
    }

    @Test
    public void testWithStringVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD," + credentialName1,
                        "workerId = 1",
                        "kinesisEndpoint = https://kinesis",
                        "metricsLevel = SUMMARY"
                }, '\n'));

        assertEquals(config.getWorkerIdentifier(), "1");
        assertEquals(config.getKinesisEndpoint(), "https://kinesis");
        assertEquals(config.getMetricsLevel(), MetricsLevel.SUMMARY);
    }

    @Test
    public void testWithSetVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD," + credentialName1,
                        "workerId = 1",
                        "metricsEnabledDimensions = ShardId, WorkerIdentifier"
                }, '\n'));

        Set<String> expectedMetricsEnabledDimensions = ImmutableSet.<String>builder().add(
                "ShardId", "WorkerIdentifier").addAll(
                KinesisClientLibConfiguration.METRICS_ALWAYS_ENABLED_DIMENSIONS).build();
        assertEquals(config.getMetricsEnabledDimensions(), expectedMetricsEnabledDimensions);
    }

    @Test
    public void testWithInitialPositionInStreamVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD," + credentialName1,
                        "workerId = 123",
                        "initialPositionInStream = TriM_Horizon"
                }, '\n'));

        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
    }    

    @Test
    public void testWithTimestampAtInitialPositionInStreamVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD," + credentialName1,
                        "timestampAtInitialPositionInStream = 1527267472"
                }, '\n'));

        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.AT_TIMESTAMP);
        assertEquals(config.getTimestampAtInitialPositionInStream(), 
                new Date(1527267472 * 1000L));
    }

    @Test
    public void testWithEmptyTimestampAtInitialPositionInStreamVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD," + credentialName1,
                        "timestampAtInitialPositionInStream = "
                }, '\n'));

        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.LATEST);
        assertEquals(config.getTimestampAtInitialPositionInStream(), null);
    }

    @Test
    public void testWithNonNumericTimestampAtInitialPositionInStreamVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD," + credentialName1,
                        "timestampAtInitialPositionInStream = 123abc"
                }, '\n'));

        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.LATEST);
        assertEquals(config.getTimestampAtInitialPositionInStream(), null);
    }

    @Test
    public void testSkippingNonKCLVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD," + credentialName1,
                        "workerId = 123",
                        "initialPositionInStream = TriM_Horizon",
                        "abc = 1"
                }, '\n'));

        assertEquals(config.getApplicationName(), "b");
        assertEquals(config.getStreamName(), "a");
        assertEquals(config.getWorkerIdentifier(), "123");
        assertEquals(config.getInitialPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
    }

    @Test
    public void testEmptyOptionalVariables() {
        KinesisClientLibConfiguration config =
                getConfiguration(StringUtils.join(new String[] {
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD," + credentialName1,
                        "workerId = 123",
                        "initialPositionInStream = TriM_Horizon",
                        "maxGetRecordsThreadPool = 1"
                }, '\n'));
        assertEquals(config.getMaxGetRecordsThreadPool(), Optional.of(1));
        assertEquals(config.getRetryGetRecordsInSeconds(), Optional.empty());
    }

    @Test
    public void testWithZeroValue() {
        String test = StringUtils.join(new String[]{
                        "streamName = a",
                        "applicationName = b",
                        "AWSCredentialsProvider = ABCD," + credentialName1,
                        "workerId = 123",
                        "initialPositionInStream = TriM_Horizon",
                        "maxGetRecordsThreadPool = 0",
                        "retryGetRecordsInSeconds = 0"
                }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        try {
            configurator.getConfiguration(input);
        } catch (Exception e) {
            fail("Don't expect to fail on invalid variable value");

        }
    }

    @Test
    public void testWithInvalidIntValue() {
        String test = StringUtils.join(new String[] {
                "streamName = a",
                "applicationName = b",
                "AWSCredentialsProvider = " + credentialName1,
                "workerId = 123",
                "failoverTimeMillis = 100nf"
        }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        try {
            configurator.getConfiguration(input);
        } catch (Exception e) {
            fail("Don't expect to fail on invalid variable value");
        }
    }

    @Test
    public void testWithNegativeIntValue() {
        String test = StringUtils.join(new String[] {
                "streamName = a",
                "applicationName = b",
                "AWSCredentialsProvider = " + credentialName1,
                "workerId = 123",
                "failoverTimeMillis = -12"
        }, '\n');
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
        String test = StringUtils.join(new String[] {
                "streamName = a",
                "applicationName = b",
                "workerId = 123",
                "failoverTimeMillis = 100",
                "shardSyncIntervalMillis = 500"
        }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        try {
            configurator.getConfiguration(input);
            fail("expect failure with no credentials provider variables");
        } catch (Exception e) {
            // succeed
        }
    }

    @Test
    public void testWithMissingWorkerId() {
        String test = StringUtils.join(new String[] {
                "streamName = a",
                "applicationName = b",
                "AWSCredentialsProvider = " + credentialName1,
                "failoverTimeMillis = 100",
                "shardSyncIntervalMillis = 500"
        }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());
        KinesisClientLibConfiguration config = configurator.getConfiguration(input);

        // if workerId is not provided, configurator should assign one for it automatically
        assertNotNull(config.getWorkerIdentifier());
        assertFalse(config.getWorkerIdentifier().isEmpty());
    }

    @Test
    public void testWithMissingStreamName() {
        String test = StringUtils.join(new String[] {
                "applicationName = b",
                "AWSCredentialsProvider = " + credentialName1,
                "workerId = 123",
                "failoverTimeMillis = 100"
        }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        try {
            configurator.getConfiguration(input);
            fail("expect failure with no stream name variables");
        } catch (Exception e) {
            // succeed
        }
    }

    @Test
    public void testWithMissingApplicationName() {
        String test = StringUtils.join(new String[] {
                "streamName = a",
                "AWSCredentialsProvider = " + credentialName1,
                "workerId = 123",
                "failoverTimeMillis = 100"
        }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        try {
            configurator.getConfiguration(input);
            fail("expect failure with no application variables");
        } catch (Exception e) {
            // succeed
        }
    }

    @Test
    public void testWithAWSCredentialsFailed() {
        String test = StringUtils.join(new String[] {
                "streamName = a",
                "applicationName = b",
                "AWSCredentialsProvider = " + credentialName2,
                "failoverTimeMillis = 100",
                "shardSyncIntervalMillis = 500"
        }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        try {
            KinesisClientLibConfiguration config = configurator.getConfiguration(input);
            config.getKinesisCredentialsProvider().getCredentials();
            fail("expect failure with wrong credentials provider");
        } catch (Exception e) {
            // succeed
        }
    }

    @Test
    public void testWithDifferentAWSCredentialsForDynamoDBAndCloudWatch() {
        String test = StringUtils.join(new String[] {
                "streamName = a",
                "applicationName = b",
                "AWSCredentialsProvider = " + credentialNameKinesis,
                "AWSCredentialsProviderDynamoDB = " + credentialNameDynamoDB,
                "AWSCredentialsProviderCloudWatch = " + credentialNameCloudWatch,
                "failoverTimeMillis = 100",
                "shardSyncIntervalMillis = 500"
        }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        KinesisClientLibConfiguration config = configurator.getConfiguration(input);
        try {
            config.getKinesisCredentialsProvider().getCredentials();
        } catch (Exception e) {
            fail("Kinesis credential providers should not fail.");
        }
        try {
            config.getDynamoDBCredentialsProvider().getCredentials();
        } catch (Exception e) {
            fail("DynamoDB credential providers should not fail.");
        }
        try {
            config.getCloudWatchCredentialsProvider().getCredentials();
        } catch (Exception e) {
            fail("CloudWatch credential providers should not fail.");
        }
    }

    @Test
    public void testWithDifferentAWSCredentialsForDynamoDBAndCloudWatchFailed() {
        String test = StringUtils.join(new String[] {
                "streamName = a",
                "applicationName = b",
                "AWSCredentialsProvider = " + credentialNameKinesis,
                "AWSCredentialsProviderDynamoDB = " + credentialName1,
                "AWSCredentialsProviderCloudWatch = " + credentialName1,
                "failoverTimeMillis = 100",
                "shardSyncIntervalMillis = 500"
        }, '\n');
        InputStream input = new ByteArrayInputStream(test.getBytes());

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement

        // separate input stream with getConfiguration to explicitly catch exception from the getConfiguration statement
        KinesisClientLibConfiguration config = configurator.getConfiguration(input);
        try {
            config.getKinesisCredentialsProvider().getCredentials();
        } catch (Exception e) {
            fail("Kinesis credential providers should not fail.");
        }
        try {
            config.getDynamoDBCredentialsProvider().getCredentials();
            fail("DynamoDB credential providers should fail.");
        } catch (Exception e) {
            // succeed
        }
        try {
            config.getCloudWatchCredentialsProvider().getCredentials();
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
            return null;
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
            return new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return "";
                }

                @Override
                public String getAWSSecretKey() {
                    return "";
                }
            };
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
            return new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return "";
                }

                @Override
                public String getAWSSecretKey() {
                    return "";
                }
            };
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
            return new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return "";
                }

                @Override
                public String getAWSSecretKey() {
                    return "";
                }
            };
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

    private KinesisClientLibConfiguration getConfiguration(String configString) {
        InputStream input = new ByteArrayInputStream(configString.getBytes());
        KinesisClientLibConfiguration config = configurator.getConfiguration(input);
        return config;
    }
}
