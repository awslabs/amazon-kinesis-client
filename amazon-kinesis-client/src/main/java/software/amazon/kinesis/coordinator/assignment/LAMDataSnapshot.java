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

import java.util.List;

import lombok.Builder;
import lombok.Value;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

/**
 * Immutable snapshot of the data loaded by {@link LAMDataProvider} for a single
 * {@link LeaseAssignmentManager} assignment cycle.
 *
 * <p>Contains the merged view of leases and worker metrics from all relevant tables.
 * LAM consumes this without any awareness of which table(s) the data originated from.</p>
 */
@Value
@Builder
@KinesisClientInternalApi
public class LAMDataSnapshot {

    /**
     * All leases from the lease table.
     */
    List<Lease> leases;

    /**
     * Lease keys that failed deserialization during the scan.
     * Used for metrics/logging by LAM.
     */
    List<String> leaseDeserializationFailures;

    /**
     * All worker metric stats entries, merged from all sources (lease table + legacy table
     * during migration). This list may contain duplicate entries for the same workerId
     * (e.g., one from legacy table and one from lease table). Callers are responsible for
     * deduplication as needed.
     */
    List<WorkerMetricStats> workerMetricStats;
}
