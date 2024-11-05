package software.amazon.kinesis.worker.metric.impl.linux;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetricType;

/**
 * Ref java doc for {@link LinuxNetworkWorkerMetricBase}
 */
public class LinuxNetworkInWorkerMetric extends LinuxNetworkWorkerMetricBase {
    private static final WorkerMetricType NETWORK_IN_WORKER_METRICS_TYPE = WorkerMetricType.NETWORK_IN;

    public LinuxNetworkInWorkerMetric(
            final OperatingRange operatingRange, final String interfaceName, final double maxBandwidthInMB) {
        this(operatingRange, interfaceName, DEFAULT_NETWORK_STAT_FILE, maxBandwidthInMB, Stopwatch.createUnstarted());
    }

    public LinuxNetworkInWorkerMetric(final OperatingRange operatingRange, final double maxBandwidthInMB) {
        this(
                operatingRange,
                DEFAULT_INTERFACE_NAME,
                DEFAULT_NETWORK_STAT_FILE,
                maxBandwidthInMB,
                Stopwatch.createUnstarted());
    }

    @VisibleForTesting
    LinuxNetworkInWorkerMetric(
            final OperatingRange operatingRange,
            final String interfaceName,
            final String statFile,
            final double maxBandwidthInMB,
            final Stopwatch stopwatch) {
        super(operatingRange, interfaceName, statFile, maxBandwidthInMB, stopwatch);
    }

    @Override
    protected WorkerMetricType getWorkerMetricsType() {
        return NETWORK_IN_WORKER_METRICS_TYPE;
    }
}
