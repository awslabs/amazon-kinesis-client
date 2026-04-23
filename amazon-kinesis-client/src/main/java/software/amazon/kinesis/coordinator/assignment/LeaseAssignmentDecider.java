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

package software.amazon.kinesis.coordinator.assignment;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

@KinesisClientInternalApi
public abstract class LeaseAssignmentDecider {
    protected final PriorityQueue<WorkerMetricStats> assignableWorkerSortedByAvailableCapacity;
    protected final Map<String, Double> workerMetricsToFleetLevelAverageMap = new HashMap<>();

    protected LeaseAssignmentDecider(LeaseAssignmentManager.InMemoryStorageView inMemoryStorageView) {
        initializeWorkerMetricsToFleetLevelAverageMap(inMemoryStorageView.getAssignableWorkers());
        final Comparator<WorkerMetricStats> comparator = Comparator.comparingDouble(
                workerMetrics -> workerMetrics.computePercentageToReachAverage(workerMetricsToFleetLevelAverageMap));
        this.assignableWorkerSortedByAvailableCapacity = new PriorityQueue<>(comparator.reversed());

        // Workers with WorkerMetricStats running hot are also available for assignment as the goal is to balance
        // utilization always (e.g., if all workers have hot WorkerMetricStats, balance the variance between them too)
        this.assignableWorkerSortedByAvailableCapacity.addAll(inMemoryStorageView.getAssignableWorkers().stream()
                .filter(workerMetrics -> inMemoryStorageView.isWorkerTotalThroughputLessThanMaxThroughput(
                                workerMetrics.getWorkerId())
                        && inMemoryStorageView.isWorkerAssignedLeasesLessThanMaxLeases(workerMetrics.getWorkerId()))
                .collect(Collectors.toList()));
    }
    /**
     * Assigns expiredOrUnAssignedLeases to the available workers.
     */
    public abstract void assignExpiredOrUnassignedLeases(final List<Lease> expiredOrUnAssignedLeases);

    /**
     * Balances the leases between workers in the fleet.
     * Implementation can choose to balance leases based on lease count or throughput or to bring the variance in
     * resource utilization to a minimum.
     * Check documentation on implementation class to see how it balances the leases.
     */
    public abstract void balanceWorkerVariance();

    private void initializeWorkerMetricsToFleetLevelAverageMap(final List<WorkerMetricStats> assignableWorkers) {
        final Map<String, Double> workerMetricsNameToAverage = assignableWorkers.stream()
                .flatMap(workerMetrics -> workerMetrics.getMetricStats().keySet().stream()
                        .map(workerMetricsName -> new AbstractMap.SimpleEntry<>(
                                workerMetricsName, workerMetrics.getMetricStat(workerMetricsName))))
                .collect(Collectors.groupingBy(
                        AbstractMap.SimpleEntry::getKey,
                        HashMap::new,
                        Collectors.averagingDouble(AbstractMap.SimpleEntry::getValue)));

        workerMetricsToFleetLevelAverageMap.putAll(workerMetricsNameToAverage);
    }
}
