/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.worker.metric.impl.linux;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetricType;

/**
 * Ref java doc for {@link LinuxNetworkWorkerMetricBase}
 */
@KinesisClientInternalApi
public class LinuxNetworkOutWorkerMetric extends LinuxNetworkWorkerMetricBase {
    private static final WorkerMetricType NETWORK_OUT_WORKER_METRICS_TYPE = WorkerMetricType.NETWORK_OUT;

    public LinuxNetworkOutWorkerMetric(
            final OperatingRange operatingRange, final String interfaceName, final double maxBandwidthInMB) {
        this(operatingRange, interfaceName, DEFAULT_NETWORK_STAT_FILE, maxBandwidthInMB, Stopwatch.createUnstarted());
    }

    public LinuxNetworkOutWorkerMetric(final OperatingRange operatingRange, final double maxBandwidthInMB) {
        this(
                operatingRange,
                DEFAULT_INTERFACE_NAME,
                DEFAULT_NETWORK_STAT_FILE,
                maxBandwidthInMB,
                Stopwatch.createUnstarted());
    }

    @VisibleForTesting
    LinuxNetworkOutWorkerMetric(
            final OperatingRange operatingRange,
            final String interfaceName,
            final String statFile,
            final double maxBandwidthInMB,
            final Stopwatch stopwatch) {
        super(operatingRange, interfaceName, statFile, maxBandwidthInMB, stopwatch);
    }

    @Override
    protected WorkerMetricType getWorkerMetricsType() {
        return NETWORK_OUT_WORKER_METRICS_TYPE;
    }
}
