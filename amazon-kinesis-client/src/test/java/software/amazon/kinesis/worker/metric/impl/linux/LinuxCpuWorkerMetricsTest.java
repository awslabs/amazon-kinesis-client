package software.amazon.kinesis.worker.metric.impl.linux;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static software.amazon.kinesis.worker.metric.WorkerMetricsTestUtils.writeLineToFile;

class LinuxCpuWorkerMetricsTest {

    @Test
    void sanity_capture(final @TempDir Path tempDir) throws IOException {
        final File statFile = new File(tempDir.toAbsolutePath() + "/cpuStat");

        final LinuxCpuWorkerMetric linuxCpuWorkerMetric = new LinuxCpuWorkerMetric(
                OperatingRange.builder().maxUtilization(80).build(), statFile.getAbsolutePath());

        writeLineToFile(statFile, String.format("cpu  %d %d %d %d %d 0 0 0 0 0", 20000, 200, 2000, 1500000, 1000));
        final WorkerMetric.WorkerMetricValue response1 = linuxCpuWorkerMetric.capture();
        // First request so expects the value to be 0;
        assertEquals(0D, response1.getValue());
        statFile.delete();

        writeLineToFile(statFile, String.format("cpu  %d %d %d %d %d 0 0 0 0 0", 30000, 3000, 30000, 2000000, 2000));
        // The Second request asserts non-zero value.
        final WorkerMetric.WorkerMetricValue response2 = linuxCpuWorkerMetric.capture();
        assertEquals(7, (int) response2.getValue().doubleValue());
        statFile.delete();
    }

    @Test
    void sanity_capture_file_not_found(final @TempDir Path tempDir) {
        final LinuxCpuWorkerMetric linuxCpuWorkerMetric = new LinuxCpuWorkerMetric(
                OperatingRange.builder().maxUtilization(80).build(), tempDir.toAbsolutePath() + "/randomPath");
        assertThrows(IllegalArgumentException.class, linuxCpuWorkerMetric::capture);
    }

    @Test
    void capture_rareIoWaitFieldDecreased_assert0Response(final @TempDir Path tempDir) throws IOException {
        final File statFile = new File(tempDir.toAbsolutePath() + "/cpuStat");

        final LinuxCpuWorkerMetric linuxCpuWorkerMetric = new LinuxCpuWorkerMetric(
                OperatingRange.builder().maxUtilization(80).build(), statFile.getAbsolutePath());

        writeLine(
                statFile, String.format("cpu  %d %d %d %d %d 0 0 0 0 0", 5469899, 773829, 2079951, 2814572566L, 52048));
        final WorkerMetric.WorkerMetricValue response1 = linuxCpuWorkerMetric.capture();
        // First request so expects the value to be 0;
        assertEquals(0D, response1.getValue());
        statFile.delete();

        writeLine(
                statFile, String.format("cpu  %d %d %d %d %d 0 0 0 0 0", 5469899, 773829, 2079951, 2814575765L, 52047));
        // The Second request asserts zero value as the iow field has decreased and thus diff_tot < diff_idl
        final WorkerMetric.WorkerMetricValue response2 = linuxCpuWorkerMetric.capture();
        assertEquals(0D, Math.round(response2.getValue() * 1000.0) / 1000.0);
        statFile.delete();
    }

    private void writeLine(final File file, final String line) throws IOException {
        final FileOutputStream fileOutputStream = new FileOutputStream(file);
        final OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
        outputStreamWriter.write(line);
        outputStreamWriter.close();
        fileOutputStream.close();
    }
}
