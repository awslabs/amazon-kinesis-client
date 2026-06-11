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
