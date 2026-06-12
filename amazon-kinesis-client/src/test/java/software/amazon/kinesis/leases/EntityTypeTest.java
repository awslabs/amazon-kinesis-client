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

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import software.amazon.kinesis.leases.EntityType.CoordinatorStateType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for the {@link EntityType} enum and its nested {@link CoordinatorStateType} sub-enum.
 */
class EntityTypeTest {

    // --- EntityType DDB value mappings ---

    @Test
    void lease_hasDdbValue_LEASE() {
        assertEquals("LEASE", EntityType.LEASE.getDdbValue());
    }

    @Test
    void streamInfo_hasDdbValue_STREAM() {
        assertEquals("STREAM", EntityType.STREAM_INFO.getDdbValue());
    }

    @Test
    void leaderLock_hasDdbValue_LEADER() {
        assertEquals("LEADER", EntityType.LEADER_LOCK.getDdbValue());
    }

    @Test
    void clientVersionMigration_hasDdbValue_CLIENT_VERSION_MIGRATION() {
        assertEquals("CLIENT_VERSION_MIGRATION", EntityType.CLIENT_VERSION_MIGRATION.getDdbValue());
    }

    @Test
    void tableMigration_hasDdbValue_TABLE_MIGRATION() {
        assertEquals("TABLE_MIGRATION", EntityType.TABLE_MIGRATION.getDdbValue());
    }

    @Test
    void workerMetricStats_hasDdbValue_WORKER_METRIC_STATS() {
        assertEquals("WORKER_METRIC_STATS", EntityType.WORKER_METRIC_STATS.getDdbValue());
    }

    // --- fromDdbValue round-trip ---

    @Test
    void fromDdbValue_allEntityTypes_roundTrip() {
        for (final EntityType type : EntityType.values()) {
            assertEquals(
                    type,
                    EntityType.fromDdbValue(type.getDdbValue()),
                    "fromDdbValue should return " + type + " for ddbValue=" + type.getDdbValue());
        }
    }

    @Test
    void fromDdbValue_nullInput_returnsNull() {
        assertNull(EntityType.fromDdbValue(null));
    }

    @Test
    void fromDdbValue_unknownValue_returnsNull() {
        assertNull(EntityType.fromDdbValue("UNKNOWN_TYPE"));
    }

    @Test
    void fromDdbValue_emptyString_returnsNull() {
        assertNull(EntityType.fromDdbValue(""));
    }

    @Test
    void fromDdbValue_caseSensitive() {
        // "lease" (lowercase) should not match "LEASE"
        assertNull(EntityType.fromDdbValue("lease"));
    }

    // --- DDB values uniqueness ---

    @Test
    void allDdbValues_areUnique() {
        final Set<String> ddbValues = new HashSet<>();
        for (final EntityType type : EntityType.values()) {
            final boolean added = ddbValues.add(type.getDdbValue());
            assertEquals(true, added, "Duplicate ddbValue found: " + type.getDdbValue());
        }
    }

    // --- CoordinatorStateType tests ---

    @Test
    void coordinatorStateType_streamInfo_mapsToEntityType() {
        assertEquals(EntityType.STREAM_INFO, CoordinatorStateType.STREAM_INFO.getEntityType());
    }

    @Test
    void coordinatorStateType_leaderLock_mapsToEntityType() {
        assertEquals(EntityType.LEADER_LOCK, CoordinatorStateType.LEADER_LOCK.getEntityType());
    }

    @Test
    void coordinatorStateType_clientVersionMigration_mapsToEntityType() {
        assertEquals(
                EntityType.CLIENT_VERSION_MIGRATION, CoordinatorStateType.CLIENT_VERSION_MIGRATION.getEntityType());
    }

    @Test
    void coordinatorStateType_tableMigration_mapsToEntityType() {
        assertEquals(EntityType.TABLE_MIGRATION, CoordinatorStateType.TABLE_MIGRATION.getEntityType());
    }

    @Test
    void coordinatorStateType_getDdbValue_delegatesToParent() {
        for (final CoordinatorStateType csType : CoordinatorStateType.values()) {
            assertEquals(csType.getEntityType().getDdbValue(), csType.getDdbValue());
        }
    }

    @Test
    void coordinatorStateType_fromDdbValue_roundTrip() {
        for (final CoordinatorStateType csType : CoordinatorStateType.values()) {
            assertEquals(
                    csType,
                    CoordinatorStateType.fromDdbValue(csType.getDdbValue()),
                    "fromDdbValue should return " + csType + " for ddbValue=" + csType.getDdbValue());
        }
    }

    @Test
    void coordinatorStateType_fromDdbValue_nullInput_returnsNull() {
        assertNull(CoordinatorStateType.fromDdbValue(null));
    }

    @Test
    void coordinatorStateType_fromDdbValue_unknownValue_returnsNull() {
        assertNull(CoordinatorStateType.fromDdbValue("UNKNOWN"));
    }

    @Test
    void coordinatorStateType_fromDdbValue_leaseValue_returnsNull() {
        // LEASE is not a CoordinatorStateType
        assertNull(CoordinatorStateType.fromDdbValue("LEASE"));
    }

    @Test
    void coordinatorStateType_fromDdbValue_workerMetricStats_returnsNull() {
        // WORKER_METRIC_STATS is not a CoordinatorStateType
        assertNull(CoordinatorStateType.fromDdbValue("WORKER_METRIC_STATS"));
    }

    @Test
    void coordinatorStateType_allValues_haveNonNullEntityType() {
        for (final CoordinatorStateType csType : CoordinatorStateType.values()) {
            assertNotNull(
                    csType.getEntityType(), "CoordinatorStateType " + csType + " should have a non-null EntityType");
        }
    }

    @Test
    void coordinatorStateType_allDdbValues_areUnique() {
        final Set<String> ddbValues = new HashSet<>();
        for (final CoordinatorStateType csType : CoordinatorStateType.values()) {
            final boolean added = ddbValues.add(csType.getDdbValue());
            assertEquals(true, added, "Duplicate ddbValue in CoordinatorStateType: " + csType.getDdbValue());
        }
    }
}
