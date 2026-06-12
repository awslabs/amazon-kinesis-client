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

import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsScope;

/**
 * Manages data loading, validation, and cleanup for {@link LeaseAssignmentManager}.
 *
 * <p>This interface encapsulates:
 * <ul>
 *   <li>All table-awareness (which DDB table(s) to read from, how to merge results during migration)</li>
 *   <li>Worker metric validation, filtering, and cleanup (invalid/stale/expired entries)</li>
 *   <li>Metrics emission for data quality issues (deserialization failures, invalid entries, failing metrics)</li>
 *   <li>Async deletion of stale worker metric entries</li>
 *   <li>Computing and publishing {@link software.amazon.kinesis.coordinator.migration.TableMigrationSummary}</li>
 * </ul>
 *
 * <p>LAM is fully agnostic to table migration and worker metric lifecycle — it just gets clean,
 * active leases and worker metrics via this manager.</p>
 */
@KinesisClientInternalApi
public interface LAMDataManager {

    /**
     * Load, validate, and clean worker metrics and leases for a single LAM assignment cycle.
     *
     * <p>The implementation will:
     * <ol>
     *   <li>Scan the lease table for LEASE and WORKER_METRIC_STATS entities (single DDB scan)</li>
     *   <li>If table migration is not complete, also scan the legacy worker-metrics table</li>
     *   <li>Filter out invalid worker metrics, log and emit metrics for them</li>
     *   <li>Filter out expired/stale worker metrics, trigger async deletion</li>
     *   <li>Emit count of workers with failing worker metrics</li>
     *   <li>Compute migration summary and push them to the registered consumer</li>
     *   <li>Return only active, valid leases and worker metrics</li>
     * </ol>
     *
     * @param metricsScope the metrics scope for emitting data quality metrics
     * @return a snapshot containing clean leases and only active, valid worker metrics for LAM
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if a required table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     */
    LAMDataSnapshot loadData(MetricsScope metricsScope)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException;
}
