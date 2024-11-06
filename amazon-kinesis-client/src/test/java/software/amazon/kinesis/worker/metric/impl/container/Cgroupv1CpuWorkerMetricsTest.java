package software.amazon.kinesis.worker.metric.impl.container;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.worker.metric.WorkerMetricsTestUtils.writeLineToFile;

class Cgroupv1CpuWorkerMetricsTest {

    @Mock
    private Clock clock;

    @BeforeEach
    void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void sanity_capture(final @TempDir Path tempDir) throws IOException {
        final File cpuTimeFile = new File(tempDir.toAbsolutePath() + "/cpuTime");
        final File cfsQuotaFile = new File(tempDir.toAbsolutePath() + "/cfsQuota");
        final File cfsPeriodFile = new File(tempDir.toAbsolutePath() + "/cfsPeriod");
        final File effectiveCpusFile = new File(tempDir.toAbsolutePath() + "/cpuset.effective_cpus");
        final OperatingRange operatingRange =
                OperatingRange.builder().maxUtilization(80).build();

        writeLineToFile(cfsQuotaFile, "20000");
        writeLineToFile(cfsPeriodFile, "10000");

        final Cgroupv1CpuWorkerMetric cgroupv1CpuWorkerMetrics = new Cgroupv1CpuWorkerMetric(
                operatingRange,
                cpuTimeFile.getAbsolutePath(),
                cfsQuotaFile.getAbsolutePath(),
                cfsPeriodFile.getAbsolutePath(),
                effectiveCpusFile.getAbsolutePath(),
                clock);

        when(clock.millis()).thenReturn(1000L, 2000L);

        writeLineToFile(cpuTimeFile, String.valueOf(TimeUnit.MILLISECONDS.toNanos(1000)));
        final WorkerMetric.WorkerMetricValue response1 = cgroupv1CpuWorkerMetrics.capture();
        // First request so expects the value to be 0;
        assertEquals(0D, response1.getValue());
        cpuTimeFile.delete();

        writeLineToFile(cpuTimeFile, String.valueOf(TimeUnit.MILLISECONDS.toNanos(1500)));
        // The Second request asserts non-zero value.
        final WorkerMetric.WorkerMetricValue response2 = cgroupv1CpuWorkerMetrics.capture();

        // Over 1 second time passed, the container has used 1500 ms-1000ms = 0.5 seconds of cpu time. The container
        // can use up to 2 cpu cores so cpu utilization is 0.5 / 2 = 25%
        assertEquals(25, response2.getValue().doubleValue());

        cfsQuotaFile.delete();
        cfsPeriodFile.delete();
        cpuTimeFile.delete();
        effectiveCpusFile.delete();
    }

    @Test
    void capture_noCpuLimit(final @TempDir Path tempDir) throws IOException {
        final File cpuTimeFile = new File(tempDir.toAbsolutePath() + "/cpuTime");
        final File cfsQuotaFile = new File(tempDir.toAbsolutePath() + "/cfsQuota");
        final File cfsPeriodFile = new File(tempDir.toAbsolutePath() + "/cfsPeriod");
        final File effectiveCpusFile = new File(tempDir.toAbsolutePath() + "/cpuset.effective_cpus");
        final OperatingRange operatingRange =
                OperatingRange.builder().maxUtilization(80).build();

        writeLineToFile(cfsQuotaFile, "-1");
        writeLineToFile(cfsPeriodFile, "10000");

        final Cgroupv1CpuWorkerMetric cgroupv1CpuWorkerMetrics = new Cgroupv1CpuWorkerMetric(
                operatingRange,
                cpuTimeFile.getAbsolutePath(),
                cfsQuotaFile.getAbsolutePath(),
                cfsPeriodFile.getAbsolutePath(),
                effectiveCpusFile.getAbsolutePath(),
                clock);

        when(clock.millis()).thenReturn(1000L, 2000L);

        // Can use up to 8 cores
        writeLineToFile(effectiveCpusFile, "0-7");
        writeLineToFile(cpuTimeFile, String.valueOf(TimeUnit.MILLISECONDS.toNanos(1000)));
        final WorkerMetric.WorkerMetricValue response1 = cgroupv1CpuWorkerMetrics.capture();
        // First request so expects the value to be 0;
        assertEquals(0D, response1.getValue());
        cpuTimeFile.delete();

        writeLineToFile(cpuTimeFile, String.valueOf(TimeUnit.MILLISECONDS.toNanos(1500)));
        // The Second request asserts non-zero value.
        final WorkerMetric.WorkerMetricValue response2 = cgroupv1CpuWorkerMetrics.capture();

        // Over 1 second time passed, the container has used 1500 ms-1000ms = 0.5 seconds of cpu time. The container
        // can use up to 8 cpu cores so cpu utilization is 0.5 / 8 = 6.25%
        assertEquals(6.25, response2.getValue().doubleValue());

        cfsQuotaFile.delete();
        cfsPeriodFile.delete();
        cpuTimeFile.delete();
        effectiveCpusFile.delete();
    }

    @Test
    void sanity_capture_file_not_found() {
        final OperatingRange operatingRange =
                OperatingRange.builder().maxUtilization(80).build();
        final Cgroupv1CpuWorkerMetric cgroupv1CpuWorkerMetrics = new Cgroupv1CpuWorkerMetric(
                operatingRange, "/someBadPath", "/someBadPath", "/someBadPath", "/someBadPath", clock);
        assertThrows(IllegalArgumentException.class, () -> cgroupv1CpuWorkerMetrics.capture());
    }
}
