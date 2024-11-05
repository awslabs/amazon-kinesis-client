package software.amazon.kinesis.worker.metricstats;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.worker.metric.WorkerMetric;

/**
 * WorkerMetricStatsManager is a class that manages the collection of raw WorkerMetricStats values for the list of WorkerMetricStats
 * periodically and store in a bounded in-memory queue.
 * This class runs a periodic thread at every {@link #inMemoryStatsCaptureThreadFrequencyMillis} interval which
 * captures each WorkerMetricStats's raw value and stores them in {@link #workerMetricsToRawHighFreqValuesMap} for each.
 * When computeStats is invoked, the method drains the in-memory raw values queue for each WorkerMetricStats and computes the
 * average and stores the computed average in #computedAverageStats for each WorkerMetricStats.
 * For each WorkerMetricStats last {@link #maxMetricStatsCount} values are captured in {@link #computedAverageMetrics}
 *
 * This class is thread safe.
 */
@Slf4j
@KinesisClientInternalApi
public final class WorkerMetricStatsManager {

    /**
     * 6 digit after decimal
     */
    private static final int DEFAULT_AVERAGE_VALUES_DIGIT_AFTER_DECIMAL = 6;

    private static final String METRICS_OPERATION_WORKER_STATS_REPORTER = "WorkerMetricStatsReporter";
    static final String METRICS_IN_MEMORY_REPORTER_FAILURE = "InMemoryMetricStatsReporterFailure";
    // 1 value per sec gives 5 minutes worth of past data for 300 count which is sufficient.
    // In case of reporter running more frequently than 5 minutes the queue will not reach this value anyway.
    private static final int HIGH_FREQUENCY_STATS_COUNT = 300;
    private static final long SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS = 60L;

    private final ScheduledExecutorService scheduledExecutorService;
    /**
     * Max count of values per WorkerMetricStats that is recorded in the storage.
     */
    private final int maxMetricStatsCount;
    /**
     * List of WorkerMetricStats configured for the application, the values from these will be recorded in the storage.
     */
    private final List<WorkerMetric> workerMetricList;
    /**
     * Map of WorkerMetricStats to its trailing (#maxMetricStatsCount) values.
     */
    @Getter(AccessLevel.PACKAGE)
    private final Map<WorkerMetric, Queue<Double>> computedAverageMetrics;
    /**
     * Map of the WorkerMetricStats to its raw values since the last flush to storage was done.
     */
    @Getter(AccessLevel.PACKAGE)
    private final Map<WorkerMetric, Queue<Double>> workerMetricsToRawHighFreqValuesMap;
    /**
     * Frequency for capturing raw WorkerMetricsValues in millis.
     */
    private final long inMemoryStatsCaptureThreadFrequencyMillis;

    private final MetricsFactory metricsFactory;
    private ScheduledFuture<?> managerProcessFuture;

    public WorkerMetricStatsManager(
            final int maxMetricStatsCount,
            final List<WorkerMetric> workerMetricList,
            final MetricsFactory metricsFactory,
            long inMemoryStatsCaptureThreadFrequencyMillis) {
        // Set thread as daemon to not block VM from exit.
        this.scheduledExecutorService = Executors.newScheduledThreadPool(
                1,
                new ThreadFactoryBuilder()
                        .daemonThreads(true)
                        .threadNamePrefix("worker-metrics-manager")
                        .build());
        this.maxMetricStatsCount = maxMetricStatsCount;
        this.workerMetricList = workerMetricList;
        this.computedAverageMetrics = new HashMap<>();
        this.workerMetricsToRawHighFreqValuesMap = new HashMap<>();
        this.metricsFactory = metricsFactory;
        this.inMemoryStatsCaptureThreadFrequencyMillis = inMemoryStatsCaptureThreadFrequencyMillis;
        init();
    }

    private void init() {
        for (final WorkerMetric workerMetric : workerMetricList) {
            computedAverageMetrics.put(workerMetric, EvictingQueue.create(maxMetricStatsCount));
            workerMetricsToRawHighFreqValuesMap.put(
                    workerMetric, Queues.synchronizedQueue(EvictingQueue.create(HIGH_FREQUENCY_STATS_COUNT)));
        }
        log.info(
                "Completed initialization with maxMetricStatsCount : {} and total WorkerMetricStats : {}",
                maxMetricStatsCount,
                workerMetricList.size());
    }

    public void startManager() {
        managerProcessFuture = scheduledExecutorService.scheduleWithFixedDelay(
                this::recordWorkerMetrics, 0, inMemoryStatsCaptureThreadFrequencyMillis, TimeUnit.MILLISECONDS);
        log.info("Started manager process...");
    }

    public void stopManager() {
        if (managerProcessFuture != null) {
            managerProcessFuture.cancel(false);
        }
        if (!scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
            try {
                if (scheduledExecutorService.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    scheduledExecutorService.shutdownNow();
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted when shutting down the scheduler, forcing shutdown", e);
                scheduledExecutorService.shutdownNow();
            }
        }
    }

    private void recordWorkerMetrics() {
        for (final WorkerMetric workerMetric : workerMetricList) {
            final Optional<Double> value = fetchWorkerMetricsValue(workerMetric);
            value.ifPresent(aDouble ->
                    workerMetricsToRawHighFreqValuesMap.get(workerMetric).add(aDouble));
        }
    }

    private Optional<Double> fetchWorkerMetricsValue(final WorkerMetric workerMetric) {
        try {
            final Double value = workerMetric.capture().getValue();
            return Optional.of(value);
        } catch (final Throwable throwable) {
            log.error(
                    "WorkerMetricStats {} failure : ",
                    workerMetric.getWorkerMetricType().name(),
                    throwable);
            final MetricsScope scope =
                    MetricsUtil.createMetricsWithOperation(metricsFactory, METRICS_OPERATION_WORKER_STATS_REPORTER);
            try {
                scope.addData(METRICS_IN_MEMORY_REPORTER_FAILURE, 1, StandardUnit.COUNT, MetricsLevel.SUMMARY);
            } finally {
                MetricsUtil.endScope(scope);
            }
            return Optional.empty();
        }
    }

    /**
     * Computes the metric stats for each WorkerMetricStats by averaging the values in inMemoryQueue and returns last
     * {@link WorkerMetricStatsManager#maxMetricStatsCount } averaged values for each WorkerMetricStats.
     *
     * In the case of empty inMemoryQueue, computedStats has -1 value to denote that specific WorkerMetricStats has failed.
     * @return Map of WorkerMetricStats shortName to averaged {@link WorkerMetricStatsManager#maxMetricStatsCount } values.
     */
    public synchronized Map<String, List<Double>> computeMetrics() {
        final Map<String, List<Double>> result = new HashMap<>();
        workerMetricsToRawHighFreqValuesMap.forEach((workerMetrics, statsQueue) -> {
            final List<Double> currentWorkerMetricsStats = drainQueue(statsQueue);

            final Queue<Double> computedMetrics = computedAverageMetrics.get(workerMetrics);

            if (currentWorkerMetricsStats.isEmpty()) {
                // In case currentWorkerMetricsStats is empty that means values from workerMetrics were not capture due
                // to some
                // reason, and thus there are no recent values, compute the value to be -1 to denote workerMetrics
                // failure
                computedMetrics.add(-1D);
            } else {
                computedMetrics.add(computeAverage(currentWorkerMetricsStats));
            }

            result.put(workerMetrics.getShortName(), new ArrayList<>(computedMetrics));
        });
        return result;
    }

    /**
     * Gets the operating range for each WorkerMetricStats that is registered.
     * @return Map of WorkerMetricStats to list of two values, first value is max utilization, and second value is variance %.
     */
    public Map<String, List<Long>> getOperatingRange() {
        final Map<String, List<Long>> operatingRange = new HashMap<>();
        workerMetricList.forEach(
                workerMetrics -> operatingRange.put(workerMetrics.getShortName(), ImmutableList.of((long)
                        workerMetrics.getOperatingRange().getMaxUtilization())));
        return operatingRange;
    }

    private static List<Double> drainQueue(final Queue<Double> queue) {
        final List<Double> elements = new ArrayList<>();
        final int queueLength = queue.size();
        for (int i = 0; i < queueLength; ++i) {
            elements.add(queue.poll());
        }
        return elements;
    }

    private Double computeAverage(final List<Double> values) {
        final double average =
                values.stream().mapToDouble(Double::doubleValue).average().orElse(0D);
        return BigDecimal.valueOf(average)
                .setScale(DEFAULT_AVERAGE_VALUES_DIGIT_AFTER_DECIMAL, RoundingMode.HALF_UP)
                .doubleValue();
    }
}
