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

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class MultiLangDaemonConfigurationTest {

    private static final String AWS_REGION_PROPERTY_NAME = "aws.region";
    private static final String DUMMY_APPLICATION_NAME = "dummyApplicationName";
    private static final String DUMMY_STREAM_NAME = "dummyStreamName";

    private BeanUtilsBean utilsBean;
    private ConvertUtilsBean convertUtilsBean;
    private String originalRegionValue;

    @Mock
    private ShardRecordProcessorFactory shardRecordProcessorFactory;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        originalRegionValue = System.getProperty(AWS_REGION_PROPERTY_NAME);
        System.setProperty(AWS_REGION_PROPERTY_NAME, "us-east-1");
        convertUtilsBean = new ConvertUtilsBean();
        utilsBean = new BeanUtilsBean(convertUtilsBean);
    }

    @After
    public void after() {
        if (originalRegionValue != null) {
            System.setProperty(AWS_REGION_PROPERTY_NAME, originalRegionValue);
        } else {
            System.clearProperty(AWS_REGION_PROPERTY_NAME);
        }
    }

    public MultiLangDaemonConfiguration baseConfiguration() {
        MultiLangDaemonConfiguration configuration = new MultiLangDaemonConfiguration(utilsBean, convertUtilsBean);
        configuration.setApplicationName(DUMMY_APPLICATION_NAME);
        configuration.setStreamName(DUMMY_STREAM_NAME);
        configuration.getKinesisCredentialsProvider().set("class", DefaultCredentialsProvider.class.getName());

        return configuration;
    }

    @Test
    public void testSetPrimitiveValue() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setMaxLeasesForWorker(10);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.leaseManagementConfig.maxLeasesForWorker(), equalTo(10));
    }

    @Test
    public void testSetEnablePriorityLeaseAssignment() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setEnablePriorityLeaseAssignment(false);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(resolvedConfiguration.leaseManagementConfig.enablePriorityLeaseAssignment(), equalTo(false));
    }

    @Test
    public void testSetLeaseTableDeletionProtectionEnabledToTrue() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setLeaseTableDeletionProtectionEnabled(true);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertTrue(resolvedConfiguration.leaseManagementConfig.leaseTableDeletionProtectionEnabled());
    }

    @Test
    public void testGracefulLeaseHandoffConfig() {
        final LeaseManagementConfig.GracefulLeaseHandoffConfig defaultGracefulLeaseHandoffConfig =
                getTestConfigsBuilder().leaseManagementConfig().gracefulLeaseHandoffConfig();

        final long testGracefulLeaseHandoffTimeoutMillis =
                defaultGracefulLeaseHandoffConfig.gracefulLeaseHandoffTimeoutMillis() + 12345;
        final boolean testGracefulLeaseHandoffEnabled =
                !defaultGracefulLeaseHandoffConfig.isGracefulLeaseHandoffEnabled();

        final MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setGracefulLeaseHandoffTimeoutMillis(testGracefulLeaseHandoffTimeoutMillis);
        configuration.setIsGracefulLeaseHandoffEnabled(testGracefulLeaseHandoffEnabled);

        final MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        final LeaseManagementConfig.GracefulLeaseHandoffConfig gracefulLeaseHandoffConfig =
                resolvedConfiguration.leaseManagementConfig.gracefulLeaseHandoffConfig();

        assertEquals(
                testGracefulLeaseHandoffTimeoutMillis, gracefulLeaseHandoffConfig.gracefulLeaseHandoffTimeoutMillis());
        assertEquals(testGracefulLeaseHandoffEnabled, gracefulLeaseHandoffConfig.isGracefulLeaseHandoffEnabled());
    }

    @Test
    public void testGracefulLeaseHandoffUsesDefaults() {
        final MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                baseConfiguration().resolvedConfiguration(shardRecordProcessorFactory);

        final LeaseManagementConfig.GracefulLeaseHandoffConfig gracefulLeaseHandoffConfig =
                resolvedConfiguration.leaseManagementConfig.gracefulLeaseHandoffConfig();

        final LeaseManagementConfig.GracefulLeaseHandoffConfig defaultGracefulLeaseHandoffConfig =
                getTestConfigsBuilder().leaseManagementConfig().gracefulLeaseHandoffConfig();

        assertEquals(defaultGracefulLeaseHandoffConfig, gracefulLeaseHandoffConfig);
    }

    @Test
    public void testWorkerUtilizationAwareAssignmentConfig() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();

        configuration.setInMemoryWorkerMetricsCaptureFrequencyMillis(123);
        configuration.setWorkerMetricsReporterFreqInMillis(123);
        configuration.setNoOfPersistedMetricsPerWorkerMetrics(123);
        configuration.setDisableWorkerMetrics(true);
        configuration.setMaxThroughputPerHostKBps(.123);
        configuration.setDampeningPercentage(12);
        configuration.setReBalanceThresholdPercentage(12);
        configuration.setAllowThroughputOvershoot(false);
        configuration.setVarianceBalancingFrequency(12);
        configuration.setWorkerMetricsEMAAlpha(.123);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);
        LeaseManagementConfig leaseManagementConfig = resolvedConfiguration.leaseManagementConfig;
        LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                leaseManagementConfig.workerUtilizationAwareAssignmentConfig();

        assertEquals(config.inMemoryWorkerMetricsCaptureFrequencyMillis(), 123);
        assertEquals(config.workerMetricsReporterFreqInMillis(), 123);
        assertEquals(config.noOfPersistedMetricsPerWorkerMetrics(), 123);
        assertTrue(config.disableWorkerMetrics());
        assertEquals(config.maxThroughputPerHostKBps(), .123, .25);
        assertEquals(config.dampeningPercentage(), 12);
        assertEquals(config.reBalanceThresholdPercentage(), 12);
        assertFalse(config.allowThroughputOvershoot());
        assertEquals(config.varianceBalancingFrequency(), 12);
        assertEquals(config.workerMetricsEMAAlpha(), .123, .25);
    }

    @Test
    public void testWorkerUtilizationAwareAssignmentConfigUsesDefaults() {
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig defaultWorkerUtilAwareAssignmentConfig =
                getTestConfigsBuilder().leaseManagementConfig().workerUtilizationAwareAssignmentConfig();

        final MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setVarianceBalancingFrequency(
                defaultWorkerUtilAwareAssignmentConfig.varianceBalancingFrequency() + 12345);

        final MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig resolvedWorkerUtilAwareAssignmentConfig =
                resolvedConfiguration.leaseManagementConfig.workerUtilizationAwareAssignmentConfig();

        assertNotEquals(defaultWorkerUtilAwareAssignmentConfig, resolvedWorkerUtilAwareAssignmentConfig);

        // apart from the single updated configuration, all other config values should be equal to the default
        resolvedWorkerUtilAwareAssignmentConfig.varianceBalancingFrequency(
                defaultWorkerUtilAwareAssignmentConfig.varianceBalancingFrequency());
        assertEquals(defaultWorkerUtilAwareAssignmentConfig, resolvedWorkerUtilAwareAssignmentConfig);
    }

    @Test
    public void testWorkerMetricsTableConfigBean() {
        final BillingMode testWorkerMetricsTableBillingMode = BillingMode.PROVISIONED;

        MultiLangDaemonConfiguration configuration = baseConfiguration();

        configuration.setWorkerMetricsTableName("testTable");
        configuration.setWorkerMetricsBillingMode(testWorkerMetricsTableBillingMode);
        configuration.setWorkerMetricsReadCapacity(123);
        configuration.setWorkerMetricsWriteCapacity(123);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);
        LeaseManagementConfig leaseManagementConfig = resolvedConfiguration.leaseManagementConfig;
        LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig workerUtilizationConfig =
                leaseManagementConfig.workerUtilizationAwareAssignmentConfig();
        LeaseManagementConfig.WorkerMetricsTableConfig workerMetricsConfig =
                workerUtilizationConfig.workerMetricsTableConfig();

        assertEquals(workerMetricsConfig.tableName(), "testTable");
        assertEquals(workerMetricsConfig.billingMode(), testWorkerMetricsTableBillingMode);
        assertEquals(workerMetricsConfig.readCapacity(), 123);
        assertEquals(workerMetricsConfig.writeCapacity(), 123);
    }

    @Test
    public void testWorkerMetricsTableConfigUsesDefaults() {
        final LeaseManagementConfig.WorkerMetricsTableConfig defaultWorkerMetricsTableConfig = getTestConfigsBuilder()
                .leaseManagementConfig()
                .workerUtilizationAwareAssignmentConfig()
                .workerMetricsTableConfig();

        final MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setWorkerMetricsBillingMode(Arrays.stream(BillingMode.values())
                .filter(billingMode -> billingMode != defaultWorkerMetricsTableConfig.billingMode())
                .findFirst()
                .orElseThrow(NoSuchElementException::new));

        final MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        final LeaseManagementConfig.WorkerMetricsTableConfig resolvedWorkerMetricsTableConfig = resolvedConfiguration
                .leaseManagementConfig
                .workerUtilizationAwareAssignmentConfig()
                .workerMetricsTableConfig();

        assertNotEquals(defaultWorkerMetricsTableConfig, resolvedWorkerMetricsTableConfig);

        // apart from the single updated configuration, all other config values should be equal to the default
        resolvedWorkerMetricsTableConfig.billingMode(defaultWorkerMetricsTableConfig.billingMode());
        assertEquals(defaultWorkerMetricsTableConfig, resolvedWorkerMetricsTableConfig);
    }

    @Test
    public void testCoordinatorStateTableConfigBean() {
        final BillingMode testWorkerMetricsTableBillingMode = BillingMode.PAY_PER_REQUEST;

        MultiLangDaemonConfiguration configuration = baseConfiguration();

        configuration.setCoordinatorStateTableName("testTable");
        configuration.setCoordinatorStateBillingMode(testWorkerMetricsTableBillingMode);
        configuration.setCoordinatorStateReadCapacity(123);
        configuration.setCoordinatorStateWriteCapacity(123);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);
        CoordinatorConfig coordinatorConfig = resolvedConfiguration.getCoordinatorConfig();
        CoordinatorConfig.CoordinatorStateTableConfig coordinatorStateConfig =
                coordinatorConfig.coordinatorStateTableConfig();
        assertEquals(coordinatorStateConfig.tableName(), "testTable");
        assertEquals(coordinatorStateConfig.billingMode(), testWorkerMetricsTableBillingMode);
        assertEquals(coordinatorStateConfig.readCapacity(), 123);
        assertEquals(coordinatorStateConfig.writeCapacity(), 123);
    }

    @Test
    public void testCoordinatorStateTableConfigUsesDefaults() {
        final CoordinatorConfig.CoordinatorStateTableConfig defaultCoordinatorStateTableConfig =
                getTestConfigsBuilder().coordinatorConfig().coordinatorStateTableConfig();

        final MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setCoordinatorStateWriteCapacity(defaultCoordinatorStateTableConfig.writeCapacity() + 12345);

        final MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        final CoordinatorConfig.CoordinatorStateTableConfig resolvedCoordinatorStateTableConfig =
                resolvedConfiguration.coordinatorConfig.coordinatorStateTableConfig();

        assertNotEquals(defaultCoordinatorStateTableConfig, resolvedCoordinatorStateTableConfig);

        // apart from the single updated configuration, all other config values should be equal to the default
        resolvedCoordinatorStateTableConfig.writeCapacity(defaultCoordinatorStateTableConfig.writeCapacity());
        assertEquals(defaultCoordinatorStateTableConfig, resolvedCoordinatorStateTableConfig);
    }

    @Test
    public void testSetLeaseTablePitrEnabledToTrue() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setLeaseTablePitrEnabled(true);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertTrue(resolvedConfiguration.leaseManagementConfig.leaseTablePitrEnabled());
    }

    @Test
    public void testSetLeaseTableDeletionProtectionEnabledToFalse() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setLeaseTableDeletionProtectionEnabled(false);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertFalse(resolvedConfiguration.leaseManagementConfig.leaseTableDeletionProtectionEnabled());
    }

    @Test
    public void testSetLeaseTablePitrEnabledToFalse() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setLeaseTablePitrEnabled(false);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertFalse(resolvedConfiguration.leaseManagementConfig.leaseTablePitrEnabled());
    }

    @Test
    public void testDefaultRetrievalConfig() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(
                resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(), instanceOf(FanOutConfig.class));
    }

    @Test
    public void testDefaultRetrievalConfigWithPollingConfigSet() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setMaxRecords(10);
        configuration.setIdleTimeBetweenReadsInMillis(60000);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(
                resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(), instanceOf(PollingConfig.class));
        assertEquals(
                10,
                ((PollingConfig) resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig()).maxRecords());
        assertEquals(
                60000,
                ((PollingConfig) resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig())
                        .idleTimeBetweenReadsInMillis());
        assertTrue(((PollingConfig) resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig())
                .usePollingConfigIdleTimeValue());
    }

    @Test
    public void testFanoutRetrievalMode() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setRetrievalMode(RetrievalMode.FANOUT);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(
                resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(), instanceOf(FanOutConfig.class));
    }

    @Test
    public void testPollingRetrievalMode() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setRetrievalMode(RetrievalMode.POLLING);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(
                resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(), instanceOf(PollingConfig.class));
    }

    @Test
    public void testRetrievalModeSetForPollingString() throws Exception {
        MultiLangDaemonConfiguration configuration = baseConfiguration();

        utilsBean.setProperty(
                configuration, "retrievalMode", RetrievalMode.POLLING.name().toLowerCase());

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(
                resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(), instanceOf(PollingConfig.class));
    }

    @Test
    public void testRetrievalModeSetForFanoutString() throws Exception {
        MultiLangDaemonConfiguration configuration = baseConfiguration();

        utilsBean.setProperty(
                configuration, "retrievalMode", RetrievalMode.FANOUT.name().toLowerCase());

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(
                resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(), instanceOf(FanOutConfig.class));
    }

    @Test
    public void testInvalidRetrievalMode() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Unknown retrieval type");

        MultiLangDaemonConfiguration configuration = baseConfiguration();

        utilsBean.setProperty(configuration, "retrievalMode", "invalid");
    }

    // @Test
    // TODO : Enable this test once https://github.com/awslabs/amazon-kinesis-client/issues/692 is resolved
    public void testmetricsEnabledDimensions() {
        MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setMetricsEnabledDimensions(new String[] {"Operation"});
        configuration.resolvedConfiguration(shardRecordProcessorFactory);
    }

    @Test
    public void testFanoutConfigSetConsumerName() {
        String consumerArn = "test-consumer";

        MultiLangDaemonConfiguration configuration = baseConfiguration();

        configuration.setRetrievalMode(RetrievalMode.FANOUT);
        configuration.getFanoutConfig().setConsumerArn(consumerArn);

        MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        assertThat(
                resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig(), instanceOf(FanOutConfig.class));
        FanOutConfig fanOutConfig =
                (FanOutConfig) resolvedConfiguration.getRetrievalConfig().retrievalSpecificConfig();

        assertThat(fanOutConfig.consumerArn(), equalTo(consumerArn));
    }

    @Test
    public void testClientVersionConfig() {
        final CoordinatorConfig.ClientVersionConfig testClientVersionConfig =
                CoordinatorConfig.ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X;

        final MultiLangDaemonConfiguration configuration = baseConfiguration();
        configuration.setClientVersionConfig(testClientVersionConfig);

        final MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                configuration.resolvedConfiguration(shardRecordProcessorFactory);

        final CoordinatorConfig coordinatorConfig = resolvedConfiguration.coordinatorConfig;

        assertEquals(testClientVersionConfig, coordinatorConfig.clientVersionConfig());
    }

    @Test
    public void testClientVersionConfigUsesDefault() {
        final MultiLangDaemonConfiguration.ResolvedConfiguration resolvedConfiguration =
                baseConfiguration().resolvedConfiguration(shardRecordProcessorFactory);

        final CoordinatorConfig coordinatorConfig = resolvedConfiguration.coordinatorConfig;

        assertEquals(
                getTestConfigsBuilder().coordinatorConfig().clientVersionConfig(),
                coordinatorConfig.clientVersionConfig());
    }

    private ConfigsBuilder getTestConfigsBuilder() {
        return new ConfigsBuilder(
                DUMMY_STREAM_NAME,
                DUMMY_APPLICATION_NAME,
                Mockito.mock(KinesisAsyncClient.class),
                Mockito.mock(DynamoDbAsyncClient.class),
                Mockito.mock(CloudWatchAsyncClient.class),
                "dummyWorkerIdentifier",
                shardRecordProcessorFactory);
    }
}
