package software.amazon.kinesis.worker.metricstats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetric;
import software.amazon.kinesis.worker.metric.WorkerMetricType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.worker.metricstats.WorkerMetricStatsManager.METRICS_IN_MEMORY_REPORTER_FAILURE;

class WorkerMetricsManagerTest {

    private static final int TEST_STATS_COUNT = 10;

    private Map<String, List<Double>> metricsMap;

    @Mock
    private MetricsFactory metricsFactory;

    private final MetricsScope metricsScope = new MetricsScope() {

        @Override
        public void addData(String name, double value, StandardUnit unit) {
            metricsMap.putIfAbsent(name, new ArrayList<>());
            metricsMap.get(name).add(value);
        }

        @Override
        public void addData(String name, double value, StandardUnit unit, MetricsLevel level) {
            metricsMap.putIfAbsent(name, new ArrayList<>());
            metricsMap.get(name).add(value);
        }

        @Override
        public void addDimension(String name, String value) {}

        @Override
        public void end() {}
    };

    @BeforeEach
    void setup() {
        MockitoAnnotations.initMocks(this);
        when(metricsFactory.createMetrics()).thenReturn(metricsScope);
        metricsMap = new HashMap<>();
    }

    @Test
    void computeStats_sanity() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        final TestWorkerMetric testWorkerMetric = new TestWorkerMetric(countDownLatch, 10.0, false);

        final WorkerMetricStatsManager workerMetricsManager =
                createManagerInstanceAndWaitTillAwait(testWorkerMetric, countDownLatch);

        assertTrue(workerMetricsManager
                        .getWorkerMetricsToRawHighFreqValuesMap()
                        .get(testWorkerMetric)
                        .size()
                >= 10);
        workerMetricsManager
                .getWorkerMetricsToRawHighFreqValuesMap()
                .get(testWorkerMetric)
                .forEach(value -> assertEquals(10D, value, "in memory stats map have incorrect value."));

        List<Double> values1 = workerMetricsManager.computeMetrics().get(WorkerMetricType.CPU.getShortName());
        assertEquals(1, values1.size(), "Lengths of values list does not match");
        assertEquals(10D, values1.get(0), "Averaged value is not correct");
        // After computeStats called, inMemoryQueue is expected to drain.
        assertEquals(
                0,
                workerMetricsManager
                        .getWorkerMetricsToRawHighFreqValuesMap()
                        .get(testWorkerMetric)
                        .size());

        // calling stats again without inMemory update is expected to return -1 for last value.
        List<Double> values2 = workerMetricsManager.computeMetrics().get(WorkerMetricType.CPU.getShortName());
        assertEquals(2, values2.size(), "Lengths of values list does not match");
        assertEquals(-1, values2.get(1), "Last value of compute stats is not -1");
    }

    @Test
    void computeStats_workerMetricReturningValueWithMoreThan6DigitAfterDecimal_assertTriming()
            throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(5);
        final TestWorkerMetric testWorkerMetric = new TestWorkerMetric(countDownLatch, 10.12345637888234234, false);
        final WorkerMetricStatsManager workerMetricsManager =
                createManagerInstanceAndWaitTillAwait(testWorkerMetric, countDownLatch);
        final List<Double> values1 = workerMetricsManager.computeMetrics().get(WorkerMetricType.CPU.getShortName());

        // assert that upto 6 digit after decimal is returned
        assertEquals(10.123456, values1.get(0));
    }

    @Test
    void computeStats_workerMetricReturningNull_expectWorkerMetricFailureStatsComputed() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        final TestWorkerMetric testWorkerMetric = new TestWorkerMetric(countDownLatch, null, false);

        final WorkerMetricStatsManager workerMetricsManager =
                createManagerInstanceAndWaitTillAwait(testWorkerMetric, countDownLatch);

        assertEquals(
                0,
                workerMetricsManager
                        .getWorkerMetricsToRawHighFreqValuesMap()
                        .get(testWorkerMetric)
                        .size());
        assertEquals(
                1,
                workerMetricsManager
                        .computeMetrics()
                        .get(WorkerMetricType.CPU.getShortName())
                        .size(),
                "Lengths of values list does not match");
    }

    @ParameterizedTest
    @CsvSource({"101, false", "50, true", "-10, false", ", false"})
    void recordStats_workerMetricReturningInvalidValues_assertNoDataRecordedAndMetricsForFailure(
            final Double value, final boolean shouldThrowException) throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        final TestWorkerMetric testWorkerMetric = new TestWorkerMetric(countDownLatch, value, shouldThrowException);

        final WorkerMetricStatsManager workerMetricsManager =
                createManagerInstanceAndWaitTillAwait(testWorkerMetric, countDownLatch);
        List<Double> metricsValues = metricsMap.get(METRICS_IN_MEMORY_REPORTER_FAILURE);

        assertTrue(metricsValues.size() > 0, "Metrics for reporter failure not published");
        assertEquals(1, metricsMap.get(METRICS_IN_MEMORY_REPORTER_FAILURE).get(0));
        assertEquals(
                0,
                workerMetricsManager
                        .getWorkerMetricsToRawHighFreqValuesMap()
                        .get(testWorkerMetric)
                        .size());
    }

    private WorkerMetricStatsManager createManagerInstanceAndWaitTillAwait(
            final TestWorkerMetric testWorkerMetric, final CountDownLatch countDownLatch) throws InterruptedException {
        final WorkerMetricStatsManager workerMetricsManager = new WorkerMetricStatsManager(
                TEST_STATS_COUNT, Collections.singletonList(testWorkerMetric), metricsFactory, 10);

        workerMetricsManager.startManager();
        boolean awaitSuccess = countDownLatch.await(10 * 15, TimeUnit.MILLISECONDS);
        workerMetricsManager.stopManager();

        assertTrue(awaitSuccess, "CountDownLatch did not complete successfully");

        return workerMetricsManager;
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class TestWorkerMetric implements WorkerMetric {

        private final WorkerMetricType workerMetricType = WorkerMetricType.CPU;

        private final CountDownLatch countDownLatch;
        private final Double workerMetricValue;
        private final Boolean shouldThrowException;

        @Override
        public String getShortName() {
            return workerMetricType.getShortName();
        }

        @Override
        public WorkerMetricValue capture() {
            countDownLatch.countDown();

            if (shouldThrowException) {
                throw new RuntimeException("Test exception");
            }

            return WorkerMetricValue.builder().value(workerMetricValue).build();
        }

        @Override
        public OperatingRange getOperatingRange() {
            return null;
        }

        public WorkerMetricType getWorkerMetricType() {
            return workerMetricType;
        }
    }
}
