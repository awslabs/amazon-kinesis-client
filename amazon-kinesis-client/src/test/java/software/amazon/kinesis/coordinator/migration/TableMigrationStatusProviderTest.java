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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableMigrationStatusProviderTest {

    @Test
    void defaultState_returnsUnknown() {
        TableMigrationStatusProvider provider = new TableMigrationStatusProvider();

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_UNKNOWN, provider.getTableMigrationStatus());
    }

    @Test
    void initialize_setsStatusAndDynamicMode() {
        TableMigrationStatusProvider provider = new TableMigrationStatusProvider();

        provider.initialize(true, TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT, provider.getTableMigrationStatus());
        assertTrue(provider.dynamicModeChangeSupportNeeded());
    }

    @Test
    void initialize_withoutDynamicModeChange_setsStatusCorrectly() {
        TableMigrationStatusProvider provider = new TableMigrationStatusProvider();

        provider.initialize(false, TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, provider.getTableMigrationStatus());
        assertFalse(provider.dynamicModeChangeSupportNeeded());
    }

    @Test
    void initialize_calledTwice_ignoresSecondCall() {
        TableMigrationStatusProvider provider = new TableMigrationStatusProvider();

        provider.initialize(true, TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);
        provider.initialize(false, TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        // First initialization wins
        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT, provider.getTableMigrationStatus());
        assertTrue(provider.dynamicModeChangeSupportNeeded());
    }

    @Test
    void updateTableMode_withDynamicModeSupport_updatesStatus() {
        TableMigrationStatusProvider provider = new TableMigrationStatusProvider();
        provider.initialize(true, TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);

        provider.updateTableMigrationStatus(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED, provider.getTableMigrationStatus());
    }

    @Test
    void updateTableMode_withDynamicModeSupport_multipleUpdates() {
        TableMigrationStatusProvider provider = new TableMigrationStatusProvider();
        provider.initialize(true, TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT);

        provider.updateTableMigrationStatus(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);
        provider.updateTableMigrationStatus(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        assertEquals(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE, provider.getTableMigrationStatus());
    }

    @Test
    void updateTableMode_withoutDynamicModeSupport_throwsException() {
        TableMigrationStatusProvider provider = new TableMigrationStatusProvider();
        provider.initialize(false, TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);

        assertThrows(
                IllegalStateException.class,
                () -> provider.updateTableMigrationStatus(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT));
    }

    @Test
    void updateTableMode_beforeInitialization_throwsException() {
        TableMigrationStatusProvider provider = new TableMigrationStatusProvider();

        assertThrows(
                IllegalStateException.class,
                () -> provider.updateTableMigrationStatus(TableMigrationStatus.TABLE_MIGRATION_STATUS_INIT));
    }

    @Test
    void dynamicModeChangeSupportNeeded_defaultIsFalse() {
        TableMigrationStatusProvider provider = new TableMigrationStatusProvider();

        assertFalse(provider.dynamicModeChangeSupportNeeded());
    }
}
