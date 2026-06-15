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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TableMigrationStatusTest {

    @Test
    void getBakeTimeSeconds_withDefault_returnsDefaultValue() {
        long bakeTime = TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED.getBakeTimeSeconds(Collections.emptyMap());
        assertEquals(TableMigrationState.DEFAULT_BAKE_TIME_SECONDS, bakeTime);
    }

    @Test
    void getBakeTimeSeconds_withOverride_returnsOverrideValue() {
        Map<TableMigrationStatus, Long> overrides = new HashMap<>();
        overrides.put(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, 999L);
        long bakeTime = TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED.getBakeTimeSeconds(overrides);
        assertEquals(999L, bakeTime);
    }

    @Test
    void allStatusValues_exist() {
        assertEquals(5, TableMigrationStatus.values().length);
        assertNotNull(TableMigrationStatus.TABLE_MIGRATION_STATUS_UNKNOWN);
        assertNotNull(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);
        assertNotNull(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);
        assertNotNull(TableMigrationStatus.TABLE_MIGRATION_STATUS_PENDING);
        assertNotNull(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
    }
}
