package software.amazon.kinesis.worker.metric.impl.container;

import java.io.IOException;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EcsCpuWorkerMetricsTest {

    /**
     * Stats has pre cpu usage 100000000 and pre system cpu usage 100000000
     * Stats has cpu usage 150000000 and system cpu usage 200000000
     * Diff cpu usage is 50000000 and diff system cpu usage is 100000000
     * The container that is running is DockerId ea32192c8553fbff06c9340478a2ff089b2bb5646fb718b4ee206641c9086d66
     * The container has 50 CPU shares and is the only container that is running = 50 total CPU shares
     * The container can use 50/50 = 100% of the CPU time allocated to the task
     * Online CPUs for the task is 2
     * Task has no CPU limit so containers can use all the online CPUs (2)
     * Total CPU core time the container can use is 2 * 100% = 2
     * CPU usage is 50000000 / 100000000 * 2 = 1 CPU core time
     * 1 CPU core time used / 2 available = 50% usage
     */
    @Test
    void sanity_capture_noTaskCpuLimitOneContainer() throws IOException {
        final String testDataPath = "src/test/data/ecstestdata/noTaskCpuLimitOneContainer";
        runWorkerMetricTest(testDataPath, 50D);
    }

    /**
     * Stats has pre cpu usage 100000000 and pre system cpu usage 100000000
     * Stats has cpu usage 150000000 and system cpu usage 200000000
     * Diff cpu usage is 50000000 and diff system cpu usage is 100000000
     * The container that is running is DockerId ea32192c8553fbff06c9340478a2ff089b2bb5646fb718b4ee206641c9086d66
     * The container has 50 CPU shares and is in the same task as another container with 30 CPU shares = 80 total CPU shares
     * The container can use 50/80 = 62.5% of the CPU time allocated to the task
     * Online CPUs for the task is 2
     * Task has no CPU limit so containers can use all the online CPUs (2)
     * Total CPU core time the container can use is 2 * 62.5% = 1.25
     * CPU usage is 50000000 / 100000000 * 2 = 1 CPU core time
     * 1 CPU core time used / 1.25 available = 80% usage
     */
    @Test
    void sanity_capture_noTaskCpuLimitTwoContainers() throws IOException {
        final String testDataPath = "src/test/data/ecstestdata/noTaskCpuLimitTwoContainers";
        runWorkerMetricTest(testDataPath, 80D);
    }

    /**
     * Behaves the same as sanity_capture_noTaskCpuLimitOneContainer, but it is possible for a customer to supply
     * a memory limit but not a CPU limit which makes the code path a little different
     */
    @Test
    void sanity_capture_noTaskCpuLimitButHasMemoryLimitOneContainer() throws IOException {
        final String testDataPath = "src/test/data/ecstestdata/noTaskCpuLimitButHasMemoryLimitOneContainer";
        runWorkerMetricTest(testDataPath, 50D);
    }

    /**
     * Stats has pre cpu usage 100000000 and pre system cpu usage 100000000
     * Stats has cpu usage 150000000 and system cpu usage 200000000
     * Diff cpu usage is 50000000 and diff system cpu usage is 100000000
     * The container that is running is DockerId ea32192c8553fbff06c9340478a2ff089b2bb5646fb718b4ee206641c9086d66
     * The container has 50 CPU shares and is the only container that is running = 50 total CPU shares
     * The container can use 50/50 = 100% of the CPU time allocated to the task
     * Online CPUs for the task is 8, but is overridden by task CPU limit
     * Task has CPU limit of 4
     * Total CPU core time the container can use is 2 * 100% = 2
     * CPU usage is 50000000 / 100000000 * 2 = 1 CPU core time
     * 1 CPU core time used / 4 available = 25% usage
     */
    @Test
    void sanity_capture_taskCpuLimitOneContainer() throws IOException {
        final String testDataPath = "src/test/data/ecstestdata/taskCpuLimitOneContainer";
        runWorkerMetricTest(testDataPath, 25D);
    }

    /**
     * Using the same test data as sanity_capture_taskCpuLimitOneContainer.
     */
    @Test
    void sanity_capture_NoPrecpuStats() throws IOException {
        final String testDataPath = "src/test/data/ecstestdata/noPrecpuStats";
        runWorkerMetricTest(testDataPath, 0D);
    }

    @Test
    void sanity_capture_NoSystemCpuUsage() throws IOException {
        final String testDataPath = "src/test/data/ecstestdata/noSystemCpuUsage";
        runWorkerMetricTest(testDataPath, 100D);
    }

    @Test
    void sanity_capture_bad_metadata_url() {
        final OperatingRange operatingRange =
                OperatingRange.builder().maxUtilization(80).build();
        final EcsCpuWorkerMetric ecsCpuWorkerMetric =
                new EcsCpuWorkerMetric(operatingRange, "/someBadPath", "/someBadPath", "/someBadPath");
        assertThrows(IllegalArgumentException.class, () -> ecsCpuWorkerMetric.capture());
    }

    void runWorkerMetricTest(String testDataPath, double expectedCpuUtilization) throws IOException {
        final OperatingRange operatingRange =
                OperatingRange.builder().maxUtilization(80).build();

        final String containerStatsPath = Paths.get(testDataPath + "/stats")
                .toAbsolutePath()
                .toUri()
                .toURL()
                .toString();
        final String taskMetadataPath = Paths.get(testDataPath + "/task")
                .toAbsolutePath()
                .toUri()
                .toURL()
                .toString();
        final String containerMetadataPath = Paths.get(testDataPath + "/root")
                .toAbsolutePath()
                .toUri()
                .toURL()
                .toString();
        final EcsCpuWorkerMetric ecsCpuWorkerMetric =
                new EcsCpuWorkerMetric(operatingRange, containerStatsPath, taskMetadataPath, containerMetadataPath);

        final WorkerMetric.WorkerMetricValue response1 = ecsCpuWorkerMetric.capture();
        assertEquals(expectedCpuUtilization, response1.getValue());
    }
}
