package software.amazon.kinesis.worker.metricstats;

import java.time.Instant;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerMetricsTest {

    @Test
    void isAnyWorkerMetricFailing_withFailingWorkerMetric_assertTrue() {
        final WorkerMetricStats workerMetrics = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of(
                        "C", ImmutableList.of(50D, -1D),
                        "M", ImmutableList.of(20D, 11D)))
                .build();

        assertTrue(
                workerMetrics.isAnyWorkerMetricFailing(),
                "isAnyWorkerMetricFailing does not return true even with failing workerMetric");
    }

    @Test
    void isAnyWorkerMetricFailing_withoutFailingWorkerMetric_assertFalse() {
        final WorkerMetricStats workerMetrics = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of(
                        "C", ImmutableList.of(50D, 1D),
                        "M", ImmutableList.of(-1D, 11D)))
                .build();

        assertFalse(
                workerMetrics.isAnyWorkerMetricFailing(),
                "isAnyWorkerMetricFailing does not return false even without failing workerMetric");
    }

    @Test
    void isAnyWorkerMetricFailing_withoutAnyValues_assertFalse() {
        final WorkerMetricStats workerMetrics = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of("C", ImmutableList.of()))
                .build();

        assertFalse(
                workerMetrics.isAnyWorkerMetricFailing(),
                "isAnyWorkerMetricFailing does not return false even without failing workerMetric");
    }

    @Test
    void isValidWorkerMetrics_sanity() {
        final WorkerMetricStats workerMetricsEntryForDefaultWorkerMetric = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .build();

        assertTrue(workerMetricsEntryForDefaultWorkerMetric.isValidWorkerMetric());
        assertTrue(workerMetricsEntryForDefaultWorkerMetric.isUsingDefaultWorkerMetric());

        final WorkerMetricStats workerMetricsEntryWithEmptyResourceMapsForDefaultWorkerMetric =
                WorkerMetricStats.builder()
                        .workerId("WorkerId1")
                        .metricStats(ImmutableMap.of())
                        .operatingRange(ImmutableMap.of())
                        .lastUpdateTime(Instant.now().getEpochSecond())
                        .build();

        assertTrue(workerMetricsEntryWithEmptyResourceMapsForDefaultWorkerMetric.isValidWorkerMetric());
        assertTrue(workerMetricsEntryWithEmptyResourceMapsForDefaultWorkerMetric.isUsingDefaultWorkerMetric());

        final WorkerMetricStats workerMetricsEntryMissingOperatingRange = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of("C", ImmutableList.of()))
                .build();

        assertFalse(workerMetricsEntryMissingOperatingRange.isValidWorkerMetric());

        final WorkerMetricStats workerMetricsEntryMissingLastUpdateTime = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .metricStats(ImmutableMap.of("C", ImmutableList.of(5D, 5D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L, 10L)))
                .build();

        assertFalse(workerMetricsEntryMissingLastUpdateTime.isValidWorkerMetric());

        final WorkerMetricStats workerMetricsEntryMissingResourceMetrics = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L, 10L)))
                .build();

        assertFalse(workerMetricsEntryMissingResourceMetrics.isValidWorkerMetric());

        // C workerMetric has resourceStats but not operatingRange
        final WorkerMetricStats workerMetricsEntryWithMismatchWorkerStatsAndOperatingRangeKey =
                WorkerMetricStats.builder()
                        .workerId("WorkerId1")
                        .lastUpdateTime(Instant.now().getEpochSecond())
                        .metricStats(ImmutableMap.of("C", ImmutableList.of(5D, 5D)))
                        .operatingRange(ImmutableMap.of("M", ImmutableList.of(80L, 10L)))
                        .build();

        assertFalse(workerMetricsEntryWithMismatchWorkerStatsAndOperatingRangeKey.isValidWorkerMetric());

        final WorkerMetricStats workerMetricsEntryWithEmptyOperatingRangeValue = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of("C", ImmutableList.of(5D, 5D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of()))
                .build();

        assertFalse(workerMetricsEntryWithEmptyOperatingRangeValue.isValidWorkerMetric());

        final WorkerMetricStats workerMetricsEntryWithNoMetricStats = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of())
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L, 10L)))
                .build();

        assertTrue(workerMetricsEntryWithNoMetricStats.isValidWorkerMetric());

        final WorkerMetricStats workerMetricsEntryWithNullResourceMetrics = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L, 10L)))
                .build();

        assertFalse(workerMetricsEntryWithNullResourceMetrics.isValidWorkerMetric());

        final WorkerMetricStats workerMetricsEntryWithZeroMaxUtilization = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of("C", ImmutableList.of(5D, 5D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(0L, 10L)))
                .build();

        assertFalse(workerMetricsEntryWithZeroMaxUtilization.isValidWorkerMetric());

        final WorkerMetricStats validWorkerMetricsEntry = WorkerMetricStats.builder()
                .workerId("WorkerId1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of("C", ImmutableList.of(5D, 5D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L, 10L)))
                .build();

        assertTrue(validWorkerMetricsEntry.isValidWorkerMetric());
    }
}
