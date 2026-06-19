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
package software.amazon.kinesis.leases;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Value;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 * DAO that reads all entity types from the lease table in a single DDB scan.
 * When all entities (Leases, WorkerMetricStats, etc.) are co-located in the lease table,
 * this DAO allows scanning all entities in a single DDB scan and grouping them by their type,
 * avoiding redundant scans.
 */
@KinesisClientInternalApi
public interface EntityDAO {

    /**
     * Interface for all entity types stored in the lease table.
     * Implementations: {@link Lease}, {@link software.amazon.kinesis.worker.metricstats.WorkerMetricStats},
     * {@link software.amazon.kinesis.coordinator.CoordinatorState}
     */
    interface Entity {
        /**
         * Returns the {@link EntityType} that identifies what kind of entity this is.
         * Used by the DAO to group scan results by type.
         */
        EntityType getEntityType();
    }

    /**
     * Holds the scan result for a single entity type: the successfully deserialized entities
     * and the partition keys of items that failed deserialization.
     */
    @Value
    @Builder
    class EntityScanList {
        /**
         * Successfully deserialized entities of this type.
         */
        @Builder.Default
        List<Entity> entities = Collections.emptyList();

        /**
         * Partition keys of items that were identified as this entity type but could not be
         * deserialized (e.g., corrupted or schema-incompatible items). Callers can use this
         * to emit metrics or log warnings.
         */
        @Builder.Default
        List<String> deserializationFailures = Collections.emptyList();
    }

    /**
     * Scan all entities from the lease table and group them by their {@link EntityType}.
     * Records without an entityType attribute are treated as {@link EntityType#LEASE}.
     *
     * @return map of entity type to {@link EntityScanList} containing entities and deserialization failures
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if the table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     */
    Map<EntityType, EntityScanList> scanAllEntities()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Scan entities from the lease table filtered to only the specified entity types.
     * This allows callers to request only the entity types they need (e.g., LEASE and WORKER_METRIC_STATS)
     * and ignore others.
     *
     * @param entityTypes the set of entity types to include in the result
     * @return map of requested entity types to {@link EntityScanList} containing entities and
     *         deserialization failures per type
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if the table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     */
    Map<EntityType, EntityScanList> scanEntities(EntityType... entityTypes)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException;

    /**
     * Shuts down any resources held by this DAO, such as thread pools used for parallel scans.
     * This method is idempotent.
     */
    default void shutdown() {
        // no-op by default for backward compatibility
    }
}
