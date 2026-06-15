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
package software.amazon.kinesis.coordinator.migration;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Summary computed by the {@link software.amazon.kinesis.coordinator.assignment.MigrationAwareLAMDataManager}
 * during data loading, and pushed to a registered consumer for use by the table migration
 * state machine.
 *
 * <p>This summary is derived from the same DDB scan data that LAM uses for lease assignment,
 * so it adds zero extra I/O overhead.</p>
 *
 * <p>The table migration state machine uses this summary to evaluate readiness for state
 * transitions (e.g., all workers emitting to lease table, min support code met, etc.).</p>
 */
@Value
@Builder
@ToString
@KinesisClientInternalApi
public class TableMigrationSummary {

    /**
     * Total number of active workers that have worker metric stats entries (across all tables).
     * "Active" is determined by the worker metrics expiry threshold.
     * This represents the merged/deduplicated count.
     */
    int totalActiveWorkersWithMetrics;

    /**
     * Number of active workers that have worker metric stats entries in the legacy
     * (dedicated WorkerMetricStats) table.
     * During migration this helps track how many workers still write to the legacy table.
     * Once migration is complete, this should be 0.
     */
    int activeWorkersWithMetricsInLegacyTable;

    /**
     * Number of active workers that have worker metric stats entries in the lease table.
     * During migration this grows as workers begin writing to the lease table.
     * Once migration is complete, this should equal {@link #totalActiveWorkersWithMetrics}.
     */
    int activeWorkersWithMetricsInLeaseTable;

    /**
     * Number of distinct workers that currently own at least one unexpired lease.
     * This is the count of lease owners whose leases have not expired.
     */
    int workersWithUnexpiredLeases;

    /**
     * Total number of distinct workers that own at least one lease (regardless of lease expiry).
     * This mirrors the "unique lease owners" concept from MigrationReadyMonitor and includes
     * all lease owners, not just those with unexpired leases.
     */
    int totalWorkersWithLeases;

    /**
     * Number of lease owners that are emitting active worker metrics (in either table).
     * This is the intersection of all lease owners and workers with active metrics,
     * mirroring the readiness check in MigrationReadyMonitor.areLeaseOwnersEmittingWorkerMetrics.
     * When this equals {@link #totalWorkersWithLeases}, all lease owners are emitting metrics.
     */
    int leaseOwnersWithActiveMetrics;

    /**
     * The fleet-wide minimum support code computed across all lease-owning workers.
     * <ul>
     *   <li>Returns 0 if any lease owner lacks support or has an expired support code heartbeat.</li>
     *   <li>Returns -1 if there are no lease owners to evaluate.</li>
     *   <li>Otherwise, returns the minimum support code ordinal across all lease-owning workers.</li>
     * </ul>
     *
     * <p>The state machine uses this to gate state transitions on fleet-wide readiness
     * (e.g., all workers must support SINGLE_TABLE_MIGRATION before advancing from INIT).</p>
     */
    int minSupportCode;
}
