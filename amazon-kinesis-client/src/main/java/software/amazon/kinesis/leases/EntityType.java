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

import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Global enum representing all entity types that can be stored in the DynamoDB tables
 * used by KCL (Lease table, CoordinatorState table). Each entity type has a unique
 * DDB value that is persisted as the "entityType" attribute.
 *
 * This enum is persisted in storage, so any changes to it need to be backward compatible.
 */
@KinesisClientInternalApi
public enum EntityType {
    /**
     * Lease entities tracking shard ownership across workers.
     */
    LEASE("LEASE"),

    /**
     * Stream metadata tracking stream identifiers and their IDs.
     */
    STREAM_INFO("STREAM"),

    /**
     * Leader election lock entry.
     */
    LEADER_LOCK("LEADER"),

    /**
     * Client version migration state (KCLv2.x -> KCLv3.x).
     */
    CLIENT_VERSION_MIGRATION("CLIENT_VERSION_MIGRATION"),

    /**
     * Table migration state (to migrate legacy coordinator and worker metrics table
     * to lease table).
     */
    TABLE_MIGRATION("TABLE_MIGRATION"),

    /**
     * Worker metric stats for resource-based lease assignment.
     */
    WORKER_METRIC_STATS("WORKER_METRIC_STATS");

    private final String ddbValue;

    EntityType(final String ddbValue) {
        this.ddbValue = ddbValue;
    }

    /**
     * Subset enum for entity types that are specifically coordinator state entities.
     * Each value maps back to its parent {@link EntityType}.
     * Used by {@code CoordinatorState} for type-safe coordinator entity type tracking.
     */
    public enum CoordinatorStateType {
        STREAM_INFO(EntityType.STREAM_INFO),
        LEADER_LOCK(EntityType.LEADER_LOCK),
        CLIENT_VERSION_MIGRATION(EntityType.CLIENT_VERSION_MIGRATION),
        TABLE_MIGRATION(EntityType.TABLE_MIGRATION);

        private final EntityType entityType;

        CoordinatorStateType(final EntityType entityType) {
            this.entityType = entityType;
        }

        /**
         * Get the parent {@link EntityType} for this coordinator state type.
         */
        public EntityType getEntityType() {
            return entityType;
        }

        /**
         * The DDB value string (delegates to parent EntityType).
         */
        public String getDdbValue() {
            return entityType.getDdbValue();
        }

        /**
         * Look up a CoordinatorStateType by its DDB value.
         * @param ddbValue the value stored in DynamoDB
         * @return the matching CoordinatorStateType, or null if not found
         */
        public static CoordinatorStateType fromDdbValue(final String ddbValue) {
            if (ddbValue == null) {
                return null;
            }
            for (final CoordinatorStateType type : values()) {
                if (type.getDdbValue().equals(ddbValue)) {
                    return type;
                }
            }
            return null;
        }
    }

    /**
     * The string value persisted in DynamoDB as the entityType attribute.
     */
    public String getDdbValue() {
        return ddbValue;
    }

    /**
     * Look up an EntityType by its DDB value.
     * @param ddbValue the value stored in DynamoDB
     * @return the matching EntityType, or null if not found
     */
    public static EntityType fromDdbValue(final String ddbValue) {
        if (ddbValue == null) {
            return null;
        }
        for (final EntityType type : values()) {
            if (type.ddbValue.equals(ddbValue)) {
                return type;
            }
        }
        return null;
    }
}
