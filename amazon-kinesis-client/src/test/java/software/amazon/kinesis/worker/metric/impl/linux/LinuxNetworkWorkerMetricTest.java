package software.amazon.kinesis.worker.metric.impl.linux;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.kinesis.worker.metric.OperatingRange;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.kinesis.worker.metric.WorkerMetricsTestUtils.writeLineToFile;

public class LinuxNetworkWorkerMetricTest {

    // The first and the second input in both cases has a difference of 1048576(1MB) rx bytes and tx 2097152(2MB) bytes.
    private static final String INPUT_1 =
            "Inter-|   Receive                                                |  Transmit\n"
                    + " face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n"
                    + "     lo: 51335658  460211    0    0    0     0          0         0 51335658  460211    0    0    0     0       0          0\n"
                    + "   eth0: 0 11860562    0    0    0     0          0   4234156 0 3248505    0    0    0     0       0          0\n";

    private static final String INPUT_2 =
            "Inter-|   Receive                                                |  Transmit\n"
                    + " face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n"
                    + "     lo: 51335668  460211    0    0    0     0          0         0 51335678  460211    0    0    0     0       0          0\n"
                    + "   eth0: 1048576 11860562    0    0    0     0          0   4234156 2097152 3248505    0    0    0     0       0          0\n";

    private static final String NO_WHITESPACE_INPUT_1 =
            "Inter-|   Receive                                                |  Transmit\n"
                    + " face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n"
                    + "     lo:51335658  460211    0    0    0     0          0         0 51335658  460211    0    0    0     0       0          0\n"
                    + "   eth0:3120842478 11860562    0    0    0     0          0   4234156 336491180 3248505"
                    + "    0    0    0     0       0          0\n";

    private static final String NO_WHITESPACE_INPUT_2 =
            "Inter-|   Receive                                                |  Transmit\n"
                    + " face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n"
                    + "     lo:51335668  460211    0    0    0     0          0         0 51335678  460211"
                    + "    0    0    0     0       0          0\n"
                    + "   eth0:3121891054 11860562    0    0    0     0          0   4234156 338588332 3248505"
                    + "    0    0    0     0       0          0\n";

    private static final OperatingRange TEST_OPERATING_RANGE =
            OperatingRange.builder().build();

    @TempDir
    private Path tempDir;

    @Test
    void capture_sanityWith1SecondTicker() throws IOException {
        executeTestForInAndOutWorkerMetric(INPUT_1, INPUT_2, 1000L, 10, 20);
        executeTestForInAndOutWorkerMetric(NO_WHITESPACE_INPUT_1, NO_WHITESPACE_INPUT_2, 1000L, 10, 20);
    }

    @Test
    void capture_sanityWith500MsTicker() throws IOException {
        executeTestForInAndOutWorkerMetric(INPUT_1, INPUT_2, 500L, 20, 40);
        executeTestForInAndOutWorkerMetric(NO_WHITESPACE_INPUT_1, NO_WHITESPACE_INPUT_2, 500L, 20, 40);
    }

    @Test
    void capture_withNoTimeElapsed() {
        assertThrows(
                IllegalArgumentException.class, () -> executeTestForInAndOutWorkerMetric(INPUT_1, INPUT_2, 0L, 20, 40));
    }

    void executeTestForInAndOutWorkerMetric(
            final String input1,
            final String input2,
            final long tickMillis,
            final long expectedIn,
            final long expectedOut)
            throws IOException {
        final File statFile = new File(tempDir.toAbsolutePath() + "/netStat");

        final LinuxNetworkInWorkerMetric linuxNetworkInWorkerMetric = new LinuxNetworkInWorkerMetric(
                TEST_OPERATING_RANGE,
                "eth0",
                statFile.getAbsolutePath(),
                10,
                getMockedStopWatchWithOneSecondTicker(tickMillis));

        writeFileAndRunTest(statFile, linuxNetworkInWorkerMetric, input1, input2, expectedIn);

        final LinuxNetworkOutWorkerMetric linuxNetworkOutWorkerMetric = new LinuxNetworkOutWorkerMetric(
                TEST_OPERATING_RANGE,
                "eth0",
                statFile.getAbsolutePath(),
                10,
                getMockedStopWatchWithOneSecondTicker(tickMillis));

        writeFileAndRunTest(statFile, linuxNetworkOutWorkerMetric, input1, input2, expectedOut);
    }

    @Test
    void capture_nonExistingFile_assertIllegalArgumentException() {
        final LinuxNetworkInWorkerMetric linuxNetworkInWorkerMetric = new LinuxNetworkInWorkerMetric(
                TEST_OPERATING_RANGE, "eth0", "/non/existing/file", 10, getMockedStopWatchWithOneSecondTicker(1000L));

        assertThrows(IllegalArgumentException.class, linuxNetworkInWorkerMetric::capture);
    }

    @Test
    void capture_nonExistingNetworkInterface_assertIllegalArgumentException() throws IOException {

        final File statFile = new File(tempDir.toAbsolutePath() + "/netStat");

        final LinuxNetworkInWorkerMetric linuxNetworkInWorkerMetric = new LinuxNetworkInWorkerMetric(
                TEST_OPERATING_RANGE,
                "randomName",
                statFile.getAbsolutePath(),
                10,
                getMockedStopWatchWithOneSecondTicker(1000L));

        writeLineToFile(statFile, INPUT_1);

        assertThrows(IllegalArgumentException.class, linuxNetworkInWorkerMetric::capture);
    }

    @Test
    void capture_configuredMaxLessThanUtilized_assert100Percent() throws IOException {
        final File statFile = new File(tempDir.toAbsolutePath() + "/netStat");

        // configured bandwidth is 1 MB and utilized bandwidth is 2 MB.
        final LinuxNetworkOutWorkerMetric linuxNetworkOutWorkerMetric = new LinuxNetworkOutWorkerMetric(
                TEST_OPERATING_RANGE,
                "eth0",
                statFile.getAbsolutePath(),
                1,
                getMockedStopWatchWithOneSecondTicker(1000L));

        writeFileAndRunTest(statFile, linuxNetworkOutWorkerMetric, NO_WHITESPACE_INPUT_1, NO_WHITESPACE_INPUT_2, 100);
    }

    @Test
    void capture_maxBandwidthInMBAsZero_assertIllegalArgumentException() throws IOException {
        final File statFile = new File(tempDir.toAbsolutePath() + "/netStat");

        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxNetworkOutWorkerMetric(
                        TEST_OPERATING_RANGE,
                        "eth0",
                        statFile.getAbsolutePath(),
                        0,
                        getMockedStopWatchWithOneSecondTicker(1000L)));
    }

    private void writeFileAndRunTest(
            final File statFile,
            final LinuxNetworkWorkerMetricBase linuxNetworkWorkerMetricBase,
            final String input1,
            final String input2,
            final double expectedValues)
            throws IOException {

        writeLineToFile(statFile, input1);
        // The First call is expected to be returning 0;
        assertEquals(0, linuxNetworkWorkerMetricBase.capture().getValue());

        writeLineToFile(statFile, input2);
        assertEquals(expectedValues, linuxNetworkWorkerMetricBase.capture().getValue());
    }

    private Stopwatch getMockedStopWatchWithOneSecondTicker(final long tickMillis) {
        final Ticker ticker = new Ticker() {
            private int readCount = 0;

            @Override
            public long read() {
                readCount++;
                return Duration.ofMillis(readCount * tickMillis).toNanos();
            }
        };
        return Stopwatch.createUnstarted(ticker);
    }
}
