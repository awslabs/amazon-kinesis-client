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
package software.amazon.kinesis.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class KinesisClientLibConfigurationTest {
    /*private static final long INVALID_LONG = 0L;
    private static final int INVALID_INT = 0;

    private static final long TEST_VALUE_LONG = 1000L;
    private static final int TEST_VALUE_INT = 1000;
    private static final int PARAMETER_COUNT = 6;

    private static final String TEST_STRING = "TestString";
    private static final String ALTER_STRING = "AlterString";

    // We don't want any of these tests to run checkpoint validation
    private static final boolean skipCheckpointValidationValue = false;

    @Test
    public void testKCLConfigurationConstructorWithCorrectParamters() {
        // Test the first two constructor with default values.
        // All of them should be positive.
        @SuppressWarnings("unused")
        KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration(TEST_STRING, TEST_STRING, null, TEST_STRING);

        // Test constructor with all valid arguments.
        config =
                new KinesisClientLibConfiguration(TEST_STRING,
                        TEST_STRING,
                        TEST_STRING,
                        TEST_STRING,
                        InitialPositionInStream.LATEST,
                        null,
                        null,
                        null,
                        TEST_VALUE_LONG,
                        TEST_STRING,
                        TEST_VALUE_INT,
                        TEST_VALUE_LONG,
                        false,
                        TEST_VALUE_LONG,
                        TEST_VALUE_LONG,
                        true,
                        new ClientConfiguration(),
                        new ClientConfiguration(),
                        new ClientConfiguration(),
                        TEST_VALUE_LONG,
                        TEST_VALUE_LONG,
                        TEST_VALUE_INT,
                        skipCheckpointValidationValue,
                        null,
                        TEST_VALUE_LONG);
    }

    @Test
    public void testKCLConfigurationConstructorWithInvalidParamter() {
        // Test constructor with invalid parameters.
        // Initialization should throw an error on invalid argument.
        // Try each argument at one time.
        KinesisClientLibConfiguration config = null;
        long[] longValues =
                { TEST_VALUE_LONG, TEST_VALUE_LONG, TEST_VALUE_LONG, TEST_VALUE_LONG, TEST_VALUE_LONG, TEST_VALUE_LONG,
                        TEST_VALUE_LONG };
        for (int i = 0; i < PARAMETER_COUNT; i++) {
            longValues[i] = INVALID_LONG;
            try {
                config =
                        new KinesisClientLibConfiguration(TEST_STRING,
                                TEST_STRING,
                                TEST_STRING,
                                TEST_STRING,
                                InitialPositionInStream.LATEST,
                                null,
                                null,
                                null,
                                longValues[0],
                                TEST_STRING,
                                TEST_VALUE_INT,
                                longValues[1],
                                false,
                                longValues[2],
                                longValues[3],
                                true,
                                new ClientConfiguration(),
                                new ClientConfiguration(),
                                new ClientConfiguration(),
                                longValues[4],
                                longValues[5],
                                TEST_VALUE_INT,
                                skipCheckpointValidationValue,
                                null,
                                longValues[6]);
            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
            }
            longValues[i] = TEST_VALUE_LONG;
        }
        int[] intValues = { TEST_VALUE_INT, TEST_VALUE_INT };
        for (int i = 0; i < 2; i++) {
            intValues[i] = INVALID_INT;
            try {
                config =
                        new KinesisClientLibConfiguration(TEST_STRING,
                                TEST_STRING,
                                TEST_STRING,
                                TEST_STRING,
                                InitialPositionInStream.LATEST,
                                null,
                                null,
                                null,
                                TEST_VALUE_LONG,
                                TEST_STRING,
                                intValues[0],
                                TEST_VALUE_LONG,
                                false,
                                TEST_VALUE_LONG,
                                TEST_VALUE_LONG,
                                true,
                                new ClientConfiguration(),
                                new ClientConfiguration(),
                                new ClientConfiguration(),
                                TEST_VALUE_LONG,
                                TEST_VALUE_LONG,
                                intValues[1],
                                skipCheckpointValidationValue,
                                null,
                                TEST_VALUE_LONG);
            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
            }
            intValues[i] = TEST_VALUE_INT;
        }
        Assert.assertTrue("KCLConfiguration should return null when using negative arguments", config == null);
    }

    @Test
    public void testKCLConfigurationUserAgent() {
        // There are two three of setting user agent:
        // 1) Use client configuration default user agent;
        // 2) Pass client configurations;
        // 3) Pass user agent.
        // For each case, after building KCLConfiguration, KINESIS_CLIENT_LIB_USER_AGENT
        // should be included in user agent.

        // Default user agent should be "appName,KINESIS_CLIENT_LIB_USER_AGENT"
        String expectedUserAgent = TEST_STRING + "," + KinesisClientLibConfiguration.KINESIS_CLIENT_LIB_USER_AGENT;
        KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration(TEST_STRING, TEST_STRING, null, TEST_STRING);
        testContainingKCLUserAgent(config, expectedUserAgent);
        ClientConfiguration clientConfig = new ClientConfiguration();
        config.withCommonClientConfig(clientConfig);
        testContainingKCLUserAgent(config, expectedUserAgent);

        // Use alter string to replace app name in KCLConfiguration user agent.
        expectedUserAgent = ALTER_STRING + "," + KinesisClientLibConfiguration.KINESIS_CLIENT_LIB_USER_AGENT;
        clientConfig.setUserAgent(ALTER_STRING);
        config.withCommonClientConfig(clientConfig);
        testContainingKCLUserAgent(config, expectedUserAgent);
        config.withUserAgent(ALTER_STRING);
        testContainingKCLUserAgent(config, expectedUserAgent);
    }

    // Every aws client configuration in KCL configuration should contain expected user agent
    private static void testContainingKCLUserAgent(KinesisClientLibConfiguration config, String expectedUserAgent) {
        Assert.assertTrue("Kinesis client should contain expected User Agent", config.getKinesisClientConfiguration()
                .getUserAgent()
                .contains(expectedUserAgent));
        Assert.assertTrue("DynamoDB client should contain expected User Agent", config.getDynamoDBClientConfiguration()
                .getUserAgent()
                .contains(expectedUserAgent));
        Assert.assertTrue("CloudWatch client should contain expected User Agent",
                config.getCloudWatchClientConfiguration().getUserAgent().contains(expectedUserAgent));
    }

    @Test
    public void testKCLConfigurationWithOnlyRegionPropertyProvided() {
        // test if the setRegion method has been called for each of the
        // client once by setting only the region name
        AmazonKinesisClient kclient = Mockito.mock(AmazonKinesisClient.class);
        AmazonDynamoDBClient dclient = Mockito.mock(AmazonDynamoDBClient.class);
        AmazonCloudWatchClient cclient = Mockito.mock(AmazonCloudWatchClient.class);
        Region region = RegionUtils.getRegion("us-west-2");

        AWSCredentialsProvider credentialsProvider = Mockito.mock(AWSCredentialsProvider.class);
        KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration("Test", "Test", credentialsProvider, "0")
                        .withRegionName("us-west-2");
        IRecordProcessorFactory processorFactory = Mockito.mock(IRecordProcessorFactory.class);
        new Worker(processorFactory, kclConfig, kclient, dclient, cclient);

        Mockito.verify(kclient, Mockito.times(1)).setRegion(region);
        Mockito.verify(dclient, Mockito.times(1)).setRegion(region);
        Mockito.verify(cclient, Mockito.times(1)).setRegion(region);
    }

    @Test
    public void testKCLConfigurationWithBothRegionAndEndpointProvided() {
        // test if the setRegion method has been called for each of the
        // client once and setEndpoint has been called once for kinesis
        // client by setting kinesis endpoint
        AmazonKinesisClient kclient = Mockito.mock(AmazonKinesisClient.class);
        AmazonDynamoDBClient dclient = Mockito.mock(AmazonDynamoDBClient.class);
        AmazonCloudWatchClient cclient = Mockito.mock(AmazonCloudWatchClient.class);
        Region region = RegionUtils.getRegion("us-west-2");

        AWSCredentialsProvider credentialsProvider = Mockito.mock(AWSCredentialsProvider.class);
        KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration("Test", "Test", credentialsProvider, "0")
                        .withRegionName("us-west-2")
                        .withKinesisEndpoint("https://kinesis.eu-west-1.amazonaws.com");
        IRecordProcessorFactory processorFactory = Mockito.mock(IRecordProcessorFactory.class);
        new Worker(processorFactory, kclConfig, kclient, dclient, cclient);

        Mockito.verify(kclient, Mockito.times(1)).setRegion(region);
        Mockito.verify(dclient, Mockito.times(1)).setRegion(region);
        Mockito.verify(cclient, Mockito.times(1)).setRegion(region);
        Mockito.verify(kclient, Mockito.times(1)).setEndpoint("https://kinesis.eu-west-1.amazonaws.com");
    }

    @Test
    public void testKCLConfigurationWithSimplerWorkerConstructor() {
        // test simpler worker constructor to see whether the region is been set
        // by testing how many times the getRegionName and getKinesisEndpoint has
        // has been called
        AWSCredentialsProvider credentialsProvider = Mockito.mock(AWSCredentialsProvider.class);
        KinesisClientLibConfiguration kclConfig = Mockito.spy(
                new KinesisClientLibConfiguration("Test", "Test", credentialsProvider, "0")
                        .withRegionName("us-west-2")
                        .withKinesisEndpoint("https://kinesis.eu-west-1.amazonaws.com"));

        IRecordProcessorFactory processorFactory = Mockito.mock(IRecordProcessorFactory.class);
        new Worker(processorFactory, kclConfig);

        Mockito.verify(kclConfig, Mockito.times(9)).getRegionName();
        Mockito.verify(kclConfig, Mockito.times(4)).getKinesisEndpoint();

        kclConfig = Mockito.spy(
                new KinesisClientLibConfiguration("Test", "Test", credentialsProvider, "0")
                        .withKinesisEndpoint("https://kinesis.eu-west-1.amazonaws.com"));

        new Worker(processorFactory, kclConfig);

        Mockito.verify(kclConfig, Mockito.times(3)).getRegionName();
        Mockito.verify(kclConfig, Mockito.times(3)).getKinesisEndpoint();
    }

    @Test
    public void testKCLConfigurationWithMultiRegionWithIlligalRegionName() {
        // test with illegal region name
        AWSCredentialsProvider credentialsProvider = Mockito.mock(AWSCredentialsProvider.class);

        KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration("Test", "Test", credentialsProvider, "0");
        try {
            kclConfig = kclConfig.withRegionName("abcd");
            Assert.fail("No expected Exception is thrown.");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testKCLConfigurationWithMultiRegionWithIlligalRegionNameInFullConstructor() {
        // test with illegal region name
        Mockito.mock(AWSCredentialsProvider.class);
        try {
            new KinesisClientLibConfiguration(TEST_STRING,
                    TEST_STRING,
                    TEST_STRING,
                    TEST_STRING,
                    null,
                    null,
                    null,
                    null,
                    TEST_VALUE_LONG,
                    TEST_STRING,
                    3,
                    TEST_VALUE_LONG,
                    false,
                    TEST_VALUE_LONG,
                    TEST_VALUE_LONG,
                    true,
                    new ClientConfiguration(),
                    new ClientConfiguration(),
                    new ClientConfiguration(),
                    TEST_VALUE_LONG,
                    TEST_VALUE_LONG,
                    1,
                    skipCheckpointValidationValue,
                    "abcd",
                    TEST_VALUE_LONG);
            Assert.fail("No expected Exception is thrown.");
        } catch(IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testKCLConfigurationMetricsDefaults() {
        KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration("TestApplication", "TestStream", null, "TestWorker");
        // By default, metrics level should be detailed.
        assertEquals(config.getMetricsLevel(), MetricsLevel.DETAILED);
        // By default, only Operation and ShardId dimensions should be enabled.
        assertEquals(config.getMetricsEnabledDimensions(), ImmutableSet.of("Operation", "ShardId"));
    }

    @Test
    public void testKCLConfigurationWithMetricsLevel() {
        KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration("TestApplication", "TestStream", null, "TestWorker")
                    .withMetricsLevel("NONE");
        assertEquals(config.getMetricsLevel(), MetricsLevel.NONE);
    }

    @Test
    public void testKCLConfigurationWithMetricsEnabledDimensions() {
        KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration("TestApplication", "TestStream", null, "TestWorker")
                    .withMetricsEnabledDimensions(null);
        // Operation dimension should always be there.
        assertEquals(config.getMetricsEnabledDimensions(), ImmutableSet.of("Operation"));

        config.withMetricsEnabledDimensions(ImmutableSet.of("WorkerIdentifier"));
        // Operation dimension should always be there.
        assertEquals(config.getMetricsEnabledDimensions(), ImmutableSet.of("Operation", "WorkerIdentifier"));
    }

    @Test
    public void testKCLConfigurationWithInvalidInitialPositionInStream() {
        KinesisClientLibConfiguration config;
        try {
            config = new KinesisClientLibConfiguration("TestApplication",
                    "TestStream",
                    null,
                    "TestWorker").withInitialPositionInStream(InitialPositionInStream.AT_TIMESTAMP);
            fail("Should have thrown");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }

        try {
            config = new KinesisClientLibConfiguration("TestApplication",
                    "TestStream",
                    null, "TestWorker").withTimestampAtInitialPositionInStream(null);
            fail("Should have thrown");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }

        try {
            Date timestamp = new Date(1000L);
            config = new KinesisClientLibConfiguration("TestApplication",
                    "TestStream", null, "TestWorker").withTimestampAtInitialPositionInStream(timestamp);
            assertEquals(config.getInitialPositionInStreamExtended().getInitialPositionInStream(),
                    InitialPositionInStream.AT_TIMESTAMP);
            assertEquals(config.getInitialPositionInStreamExtended().getTimestamp(), timestamp);
        } catch (Exception e) {
            fail("Should not have thrown");
        }

        try {
            config = new KinesisClientLibConfiguration("TestApplication",
                    "TestStream",
                    null,
                    "TestWorker").withInitialPositionInStream(InitialPositionInStream.LATEST);
            assertEquals(config.getInitialPositionInStreamExtended().getInitialPositionInStream(),
                    InitialPositionInStream.LATEST);
            assertNull(config.getInitialPositionInStreamExtended().getTimestamp());
        } catch (Exception e) {
            fail("Should not have thrown");
        }
    }

    @Test
    public void testKCLConfigurationWithIgnoreUnexpectedChildShards() {
        KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration("TestApplication", "TestStream", null, "TestWorker");
        // By default, unexpected child shards should not be ignored.
        assertFalse(config.shouldIgnoreUnexpectedChildShards());
        config = config.withIgnoreUnexpectedChildShards(true);
        assertTrue(config.shouldIgnoreUnexpectedChildShards());
    }*/
}
