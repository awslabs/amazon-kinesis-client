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

class Cgroupv2CpuWorkerMetricsTest {

    @Mock
    private Clock clock;

    @BeforeEach
    void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void sanity_capture(final @TempDir Path tempDir) throws IOException {
        final File cpuMaxFile = new File(tempDir.toAbsolutePath() + "/cpu.max");
        final File effectiveCpusFile = new File(tempDir.toAbsolutePath() + "/cpuset.cpus.effective");
        final File cpuStatFile = new File(tempDir.toAbsolutePath() + "/cpu.stat");
        final OperatingRange operatingRange =
                OperatingRange.builder().maxUtilization(80).build();

        final Cgroupv2CpuWorkerMetric cgroupv2CpuWorkerMetric = new Cgroupv2CpuWorkerMetric(
                operatingRange,
                cpuMaxFile.getAbsolutePath(),
                effectiveCpusFile.getAbsolutePath(),
                cpuStatFile.getAbsolutePath(),
                clock);

        when(clock.millis()).thenReturn(1000L, 2000L);

        // Can use 2 cores
        writeLineToFile(cpuMaxFile, "20000 10000");
        writeLineToFile(cpuStatFile, "usage_usec " + TimeUnit.MILLISECONDS.toMicros(1000));
        final WorkerMetric.WorkerMetricValue response1 = cgroupv2CpuWorkerMetric.capture();
        // First request so expects the value to be 0;
        assertEquals(0D, response1.getValue());

        writeLineToFile(cpuStatFile, "usage_usec " + TimeUnit.MILLISECONDS.toMicros(1500));
        // The Second request asserts non-zero value.
        final WorkerMetric.WorkerMetricValue response2 = cgroupv2CpuWorkerMetric.capture();

        // Over 1 second time passed, the container has used 1500 ms- 1000ms = 0.5 seconds of cpu time. The container
        // can use up to 2 cpu cores so cpu utilization is 0.5 / 2 = 25%
        assertEquals(25, response2.getValue().doubleValue());

        cpuMaxFile.delete();
        effectiveCpusFile.delete();
        cpuStatFile.delete();
    }

    @Test
    void capture_noCpuLimit(final @TempDir Path tempDir) throws IOException {
        final File cpuMaxFile = new File(tempDir.toAbsolutePath() + "/cpu.max");
        final File effectiveCpusFile = new File(tempDir.toAbsolutePath() + "/cpuset.cpus.effective");
        final File cpuStatFile = new File(tempDir.toAbsolutePath() + "/cpu.stat");
        final OperatingRange operatingRange =
                OperatingRange.builder().maxUtilization(80).build();

        final Cgroupv2CpuWorkerMetric cgroupv2CpuWorkerMetric = new Cgroupv2CpuWorkerMetric(
                operatingRange,
                cpuMaxFile.getAbsolutePath(),
                effectiveCpusFile.getAbsolutePath(),
                cpuStatFile.getAbsolutePath(),
                clock);

        when(clock.millis()).thenReturn(1000L, 2000L);

        // Can use all available cores
        writeLineToFile(cpuMaxFile, "max 10000");
        writeLineToFile(effectiveCpusFile, "0-7");
        writeLineToFile(cpuStatFile, "usage_usec " + TimeUnit.MILLISECONDS.toMicros(1000));
        final WorkerMetric.WorkerMetricValue response1 = cgroupv2CpuWorkerMetric.capture();
        // First request so expects the value to be 0;
        assertEquals(0D, response1.getValue());

        writeLineToFile(cpuStatFile, "usage_usec " + TimeUnit.MILLISECONDS.toMicros(1500));
        // The Second request asserts non-zero value.
        final WorkerMetric.WorkerMetricValue response2 = cgroupv2CpuWorkerMetric.capture();

        // Over 1 second time passed, the container has used 1500 ms- 1000ms = 0.5 seconds of cpu time. The container
        // can use up to 8 cpu cores so cpu utilization is 0.5 / 8 = 6.25
        assertEquals(6.25, response2.getValue().doubleValue());

        cpuMaxFile.delete();
        effectiveCpusFile.delete();
        cpuStatFile.delete();
    }

    @Test
    void sanity_capture_file_not_found() {
        final OperatingRange operatingRange =
                OperatingRange.builder().maxUtilization(80).build();
        final Cgroupv2CpuWorkerMetric cgroupv2CpuWorkerMetric =
                new Cgroupv2CpuWorkerMetric(operatingRange, "/someBadPath", "/someBadPath", "/someBadPath", clock);
        assertThrows(IllegalArgumentException.class, () -> cgroupv2CpuWorkerMetric.capture());
    }
}
