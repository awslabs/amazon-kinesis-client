package software.amazon.kinesis.multilang.config;

import java.io.IOException;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.kinesis.coordinator.CoordinatorConfig.ClientVersionConfig;
import software.amazon.kinesis.multilang.MultiLangDaemonConfig;
import software.amazon.kinesis.multilang.config.MultiLangDaemonConfiguration.ResolvedConfiguration;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PropertiesMappingE2ETest {
    private static final String PROPERTIES_FILE = "multilang.properties";
    private static final String PROPERTIES_FILE_V3 = "multilangv3.properties";

    @Test
    public void testKclV3PropertiesMapping() throws IOException {
        final MultiLangDaemonConfig config = new MultiLangDaemonConfig(PROPERTIES_FILE);

        final ResolvedConfiguration kclV3Config =
                config.getMultiLangDaemonConfiguration().resolvedConfiguration(new TestRecordProcessorFactory());

        assertEquals(
                ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X,
                kclV3Config.coordinatorConfig.clientVersionConfig());

        assertEquals(
                "MultiLangTest-CoordinatorState-CustomName",
                kclV3Config.coordinatorConfig.coordinatorStateConfig().tableName());
        assertEquals(
                BillingMode.PROVISIONED,
                kclV3Config.coordinatorConfig.coordinatorStateConfig().billingMode());
        assertEquals(
                1000, kclV3Config.coordinatorConfig.coordinatorStateConfig().readCapacity());
        assertEquals(500, kclV3Config.coordinatorConfig.coordinatorStateConfig().writeCapacity());

        assertEquals(
                10000L,
                kclV3Config.leaseManagementConfig.gracefulLeaseHandoffConfig().gracefulLeaseHandoffTimeoutMillis());
        assertFalse(
                kclV3Config.leaseManagementConfig.gracefulLeaseHandoffConfig().isGracefulLeaseHandoffEnabled());

        assertEquals(
                5000L,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .inMemoryWorkerMetricsCaptureFrequencyMillis());
        assertEquals(
                60000L,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .workerMetricsReporterFreqInMillis());
        assertEquals(
                50,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .noOfPersistedMetricsPerWorkerMetrics());
        assertTrue(kclV3Config
                .leaseManagementConfig
                .workerUtilizationAwareAssignmentConfig()
                .disableWorkerMetrics());
        assertEquals(
                10000,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .maxThroughputPerHostKBps());
        assertEquals(
                90,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .dampeningPercentage());
        assertEquals(
                5,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .reBalanceThresholdPercentage());
        assertFalse(kclV3Config
                .leaseManagementConfig
                .workerUtilizationAwareAssignmentConfig()
                .allowThroughputOvershoot());
        assertEquals(
                Duration.ofHours(12),
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .staleWorkerMetricsEntryCleanupDuration());
        assertEquals(
                5,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .varianceBalancingFrequency());
        assertEquals(
                0.18D,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .workerMetricsEMAAlpha());

        assertEquals(
                "MultiLangTest-WorkerMetrics-CustomName",
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .workerMetricsTableConfig()
                        .tableName());
        assertEquals(
                BillingMode.PROVISIONED,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .workerMetricsTableConfig()
                        .billingMode());
        assertEquals(
                250,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .workerMetricsTableConfig()
                        .readCapacity());
        assertEquals(
                90,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .workerMetricsTableConfig()
                        .writeCapacity());
    }

    @Test
    public void testKclV3PropertiesMappingForDefaultValues() throws IOException {
        final MultiLangDaemonConfig config = new MultiLangDaemonConfig(PROPERTIES_FILE_V3);

        final ResolvedConfiguration kclV3Config =
                config.getMultiLangDaemonConfiguration().resolvedConfiguration(new TestRecordProcessorFactory());

        assertEquals(ClientVersionConfig.CLIENT_VERSION_CONFIG_3X, kclV3Config.coordinatorConfig.clientVersionConfig());

        assertEquals(
                "MultiLangTest-CoordinatorState",
                kclV3Config.coordinatorConfig.coordinatorStateConfig().tableName());
        assertEquals(
                BillingMode.PAY_PER_REQUEST,
                kclV3Config.coordinatorConfig.coordinatorStateConfig().billingMode());

        assertEquals(
                30_000L,
                kclV3Config.leaseManagementConfig.gracefulLeaseHandoffConfig().gracefulLeaseHandoffTimeoutMillis());
        assertTrue(
                kclV3Config.leaseManagementConfig.gracefulLeaseHandoffConfig().isGracefulLeaseHandoffEnabled());

        assertEquals(
                1000L,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .inMemoryWorkerMetricsCaptureFrequencyMillis());
        assertEquals(
                30000L,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .workerMetricsReporterFreqInMillis());
        assertEquals(
                10,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .noOfPersistedMetricsPerWorkerMetrics());
        assertFalse(kclV3Config
                .leaseManagementConfig
                .workerUtilizationAwareAssignmentConfig()
                .disableWorkerMetrics());
        assertEquals(
                Double.MAX_VALUE,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .maxThroughputPerHostKBps());
        assertEquals(
                60,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .dampeningPercentage());
        assertEquals(
                10,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .reBalanceThresholdPercentage());
        assertTrue(kclV3Config
                .leaseManagementConfig
                .workerUtilizationAwareAssignmentConfig()
                .allowThroughputOvershoot());
        assertEquals(
                Duration.ofDays(1),
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .staleWorkerMetricsEntryCleanupDuration());
        assertEquals(
                3,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .varianceBalancingFrequency());
        assertEquals(
                0.5D,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .workerMetricsEMAAlpha());

        assertEquals(
                "MultiLangTest-WorkerMetricStats",
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .workerMetricsTableConfig()
                        .tableName());
        assertEquals(
                BillingMode.PAY_PER_REQUEST,
                kclV3Config
                        .leaseManagementConfig
                        .workerUtilizationAwareAssignmentConfig()
                        .workerMetricsTableConfig()
                        .billingMode());
    }

    private static class TestRecordProcessorFactory implements ShardRecordProcessorFactory {
        @Override
        public ShardRecordProcessor shardRecordProcessor() {
            return null;
        }
    }
}
