package software.amazon.kinesis.worker;

import java.util.Collections;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.worker.metric.impl.container.Cgroupv1CpuWorkerMetric;
import software.amazon.kinesis.worker.metric.impl.container.Cgroupv2CpuWorkerMetric;
import software.amazon.kinesis.worker.metric.impl.container.EcsCpuWorkerMetric;
import software.amazon.kinesis.worker.metric.impl.linux.LinuxCpuWorkerMetric;
import software.amazon.kinesis.worker.platform.OperatingRangeDataProvider;
import software.amazon.kinesis.worker.platform.ResourceMetadataProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkerWorkerMetricsSelectorTest {

    private ResourceMetadataProvider resourceMetadataProvider;
    private WorkerMetricsSelector workerMetricsSelector;

    @BeforeEach
    void setUp() {
        resourceMetadataProvider = mock(ResourceMetadataProvider.class);
        workerMetricsSelector = new WorkerMetricsSelector(Collections.singletonList(resourceMetadataProvider));

        when(resourceMetadataProvider.getPlatform()).thenReturn(ResourceMetadataProvider.ComputePlatform.EC2);
        when(resourceMetadataProvider.isOnPlatform()).thenReturn(true);
        when(resourceMetadataProvider.getOperatingRangeDataProvider())
                .thenReturn(Optional.of(OperatingRangeDataProvider.LINUX_PROC));
    }

    @Test
    void testOnEc2AndLinuxProc() {
        assertEquals(1, workerMetricsSelector.getDefaultWorkerMetrics().size());
        assertEquals(
                LinuxCpuWorkerMetric.class,
                workerMetricsSelector.getDefaultWorkerMetrics().get(0).getClass());
    }

    @Test
    void testOnEc2ButNotHaveLinuxProc() {
        when(resourceMetadataProvider.getOperatingRangeDataProvider()).thenReturn(Optional.empty());
        assertEquals(0, workerMetricsSelector.getDefaultWorkerMetrics().size());
    }

    @Test
    void testOnEksAndCgroupV1() {
        when(resourceMetadataProvider.getPlatform()).thenReturn(ResourceMetadataProvider.ComputePlatform.EKS);
        when(resourceMetadataProvider.getOperatingRangeDataProvider())
                .thenReturn(Optional.of(OperatingRangeDataProvider.LINUX_EKS_CGROUP_V1));
        assertEquals(1, workerMetricsSelector.getDefaultWorkerMetrics().size());
        assertEquals(
                Cgroupv1CpuWorkerMetric.class,
                workerMetricsSelector.getDefaultWorkerMetrics().get(0).getClass());
    }

    @Test
    void testOnEksAndCgroupV2() {
        when(resourceMetadataProvider.getPlatform()).thenReturn(ResourceMetadataProvider.ComputePlatform.EKS);
        when(resourceMetadataProvider.getOperatingRangeDataProvider())
                .thenReturn(Optional.of(OperatingRangeDataProvider.LINUX_EKS_CGROUP_V2));
        assertEquals(1, workerMetricsSelector.getDefaultWorkerMetrics().size());
        assertEquals(
                Cgroupv2CpuWorkerMetric.class,
                workerMetricsSelector.getDefaultWorkerMetrics().get(0).getClass());
    }

    @Test
    void testOnEcsAndUsesEcsWorkerMetric() {
        when(resourceMetadataProvider.getPlatform()).thenReturn(ResourceMetadataProvider.ComputePlatform.ECS);
        when(resourceMetadataProvider.getOperatingRangeDataProvider())
                .thenReturn(Optional.of(OperatingRangeDataProvider.LINUX_ECS_METADATA_KEY_V4));
        assertEquals(1, workerMetricsSelector.getDefaultWorkerMetrics().size());
        assertEquals(
                EcsCpuWorkerMetric.class,
                workerMetricsSelector.getDefaultWorkerMetrics().get(0).getClass());
    }

    @Test
    void testNotOnSupportedPlatform() {
        when(resourceMetadataProvider.isOnPlatform()).thenReturn(false);
        assertEquals(0, workerMetricsSelector.getDefaultWorkerMetrics().size());
    }
}
