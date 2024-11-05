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
package software.amazon.kinesis.worker.metricstats;

import java.time.Instant;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

/**
 * Reporter that is periodically executed to report WorkerMetricStats. It collects
 * the in memory metric stats and writes into the DDB WorkerMetricStats table.
 */
@Slf4j
@RequiredArgsConstructor
@KinesisClientInternalApi
public class WorkerMetricStatsReporter implements Runnable {
    private final MetricsFactory metricsFactory;
    private final String workerIdentifier;
    private final WorkerMetricStatsManager workerMetricsManager;
    private final WorkerMetricStatsDAO workerMetricsDAO;

    @Override
    public void run() {
        final MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, "WorkerMetricStatsReporter");
        final long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            /*
             * OperatingRange value fetched during the initialization and is same afterwards. It's possible
             * to update OperatingRange only in first call and then skip, but we do not want to do that to avoid
             * case where a worker can have a failure for some time and thus does not update the workerMetrics entry
             * and LeaseAssigmentManager cleans it and then worker ends updating entry without operating range.
             */
            final WorkerMetricStats workerMetrics = WorkerMetricStats.builder()
                    .workerId(workerIdentifier)
                    .metricStats(workerMetricsManager.computeMetrics())
                    .operatingRange(workerMetricsManager.getOperatingRange())
                    .lastUpdateTime(Instant.now().getEpochSecond())
                    .build();
            workerMetricsDAO.updateMetrics(workerMetrics);
            success = true;
        } catch (final Exception e) {
            log.error("Failed to update worker metric stats for worker : {}", workerIdentifier, e);
        } finally {
            MetricsUtil.addWorkerIdentifier(scope, workerIdentifier);
            MetricsUtil.addSuccessAndLatency(scope, success, startTime, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(scope);
        }
    }
}
