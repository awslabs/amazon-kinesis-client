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
package software.amazon.kinesis.coordinator;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import software.amazon.kinesis.coordinator.CoordinatorConfig.ClientVersionConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link CoordinatorConfig} particularly the new fields added in CR 5.
 */
public class CoordinatorConfigTest {

    private static final String APPLICATION_NAME = "TestApp";

    @Test
    public void testDefaultClientVersionConfig_is3X() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        assertEquals(ClientVersionConfig.CLIENT_VERSION_CONFIG_3X, config.clientVersionConfig());
    }

    @Test
    public void testDefaultMigrateAllEntitiesToLeaseTable_isFalse() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        assertFalse(config.migrateAllEntitiesToLeaseTable());
    }

    @Test
    public void testDefaultTableMigrationCompleteBakeTimeSeconds_isOneDay() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        assertEquals(TimeUnit.DAYS.toSeconds(1), config.tableMigrationCompleteBakeTimeSeconds());
    }

    // ========================
    // effectiveTableMigrationCompleteBakeTimeSeconds tests
    // ========================

    @Test
    public void testEffectiveBakeTime_defaultValue_returnsDefault() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        // Default is 1 day, which is within [1h, 7d]
        assertEquals(TimeUnit.DAYS.toSeconds(1), config.effectiveTableMigrationCompleteBakeTimeSeconds());
    }

    @Test
    public void testEffectiveBakeTime_belowMin_clampedToMin() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        config.tableMigrationCompleteBakeTimeSeconds(60); // 1 minute - below 1 hour minimum

        // Should be clamped to 1 hour minimum
        assertEquals(TimeUnit.HOURS.toSeconds(1), config.effectiveTableMigrationCompleteBakeTimeSeconds());
    }

    @Test
    public void testEffectiveBakeTime_zero_clampedToMin() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        config.tableMigrationCompleteBakeTimeSeconds(0);

        assertEquals(TimeUnit.HOURS.toSeconds(1), config.effectiveTableMigrationCompleteBakeTimeSeconds());
    }

    @Test
    public void testEffectiveBakeTime_aboveMax_clampedToMax() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        config.tableMigrationCompleteBakeTimeSeconds(TimeUnit.DAYS.toSeconds(30)); // 30 days - above 7 day max

        // Should be clamped to 7 day maximum
        assertEquals(TimeUnit.DAYS.toSeconds(7), config.effectiveTableMigrationCompleteBakeTimeSeconds());
    }

    @Test
    public void testEffectiveBakeTime_exactlyMin_returnsMin() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        config.tableMigrationCompleteBakeTimeSeconds(TimeUnit.HOURS.toSeconds(1));

        assertEquals(TimeUnit.HOURS.toSeconds(1), config.effectiveTableMigrationCompleteBakeTimeSeconds());
    }

    @Test
    public void testEffectiveBakeTime_exactlyMax_returnsMax() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        config.tableMigrationCompleteBakeTimeSeconds(TimeUnit.DAYS.toSeconds(7));

        assertEquals(TimeUnit.DAYS.toSeconds(7), config.effectiveTableMigrationCompleteBakeTimeSeconds());
    }

    @Test
    public void testEffectiveBakeTime_withinRange_returnsValue() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        final long twoDays = TimeUnit.DAYS.toSeconds(2);
        config.tableMigrationCompleteBakeTimeSeconds(twoDays);

        assertEquals(twoDays, config.effectiveTableMigrationCompleteBakeTimeSeconds());
    }

    // ========================
    // ClientVersionConfig enum tests
    // ========================

    @Test
    public void testClientVersionConfigEnum_hasAllExpectedValues() {
        assertEquals(3, ClientVersionConfig.values().length);
        assertNotNull(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1);
        assertNotNull(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);
        assertNotNull(ClientVersionConfig.CLIENT_VERSION_CONFIG_3X);
    }

    @Test
    public void testSetClientVersionConfig_phase1() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        config.clientVersionConfig(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1);
        assertEquals(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1, config.clientVersionConfig());
    }

    @Test
    public void testSetClientVersionConfig_phase2() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        config.clientVersionConfig(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);
        assertEquals(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X, config.clientVersionConfig());
    }

    // ========================
    // CoordinatorStateTableConfig tests
    // ========================

    @Test
    public void testCoordinatorStateTableConfig_defaultTableName() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        assertNotNull(config.coordinatorStateTableConfig());
        assertEquals(
                APPLICATION_NAME + "-CoordinatorState",
                config.coordinatorStateTableConfig().tableName());
    }

    @Test
    public void testSetMigrateAllEntitiesToLeaseTable() {
        final CoordinatorConfig config = new CoordinatorConfig(APPLICATION_NAME);
        config.migrateAllEntitiesToLeaseTable(true);
        assertEquals(true, config.migrateAllEntitiesToLeaseTable());
    }
}
