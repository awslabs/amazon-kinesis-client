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

class TableMigrationSummaryTest {

    @Test
    void builder_allFields_createsCorrectSummary() {
        TableMigrationSummary summary = TableMigrationSummary.builder()
                .totalActiveWorkersWithMetrics(5)
                .activeWorkersWithMetricsInLegacyTable(2)
                .activeWorkersWithMetricsInLeaseTable(3)
                .workersWithUnexpiredLeases(4)
                .totalWorkersWithLeases(6)
                .leaseOwnersWithActiveMetrics(4)
                .minSupportCode(2)
                .build();

        assertEquals(5, summary.getTotalActiveWorkersWithMetrics());
        assertEquals(2, summary.getActiveWorkersWithMetricsInLegacyTable());
        assertEquals(3, summary.getActiveWorkersWithMetricsInLeaseTable());
        assertEquals(4, summary.getWorkersWithUnexpiredLeases());
        assertEquals(6, summary.getTotalWorkersWithLeases());
        assertEquals(4, summary.getLeaseOwnersWithActiveMetrics());
        assertEquals(2, summary.getMinSupportCode());
    }

    @Test
    void builder_defaults_zeroValues() {
        TableMigrationSummary summary = TableMigrationSummary.builder().build();

        assertEquals(0, summary.getTotalActiveWorkersWithMetrics());
        assertEquals(0, summary.getActiveWorkersWithMetricsInLegacyTable());
        assertEquals(0, summary.getActiveWorkersWithMetricsInLeaseTable());
        assertEquals(0, summary.getWorkersWithUnexpiredLeases());
        assertEquals(0, summary.getTotalWorkersWithLeases());
        assertEquals(0, summary.getLeaseOwnersWithActiveMetrics());
        assertEquals(0, summary.getMinSupportCode());
    }

    @Test
    void builder_migrationComplete_legacyIsZero() {
        TableMigrationSummary summary = TableMigrationSummary.builder()
                .totalActiveWorkersWithMetrics(3)
                .activeWorkersWithMetricsInLegacyTable(0)
                .activeWorkersWithMetricsInLeaseTable(3)
                .workersWithUnexpiredLeases(3)
                .minSupportCode(3)
                .build();

        assertEquals(0, summary.getActiveWorkersWithMetricsInLegacyTable());
        assertEquals(3, summary.getActiveWorkersWithMetricsInLeaseTable());
        assertEquals(summary.getTotalActiveWorkersWithMetrics(), summary.getActiveWorkersWithMetricsInLeaseTable());
    }

    @Test
    void builder_noLeaseOwners_minSupportCodeNegativeOne() {
        TableMigrationSummary summary = TableMigrationSummary.builder()
                .totalActiveWorkersWithMetrics(2)
                .activeWorkersWithMetricsInLegacyTable(1)
                .activeWorkersWithMetricsInLeaseTable(1)
                .workersWithUnexpiredLeases(0)
                .minSupportCode(-1)
                .build();

        assertEquals(-1, summary.getMinSupportCode());
        assertEquals(0, summary.getWorkersWithUnexpiredLeases());
    }

    @Test
    void builder_workerWithoutSupportCode_minSupportCodeZero() {
        TableMigrationSummary summary = TableMigrationSummary.builder()
                .totalActiveWorkersWithMetrics(3)
                .activeWorkersWithMetricsInLegacyTable(1)
                .activeWorkersWithMetricsInLeaseTable(2)
                .workersWithUnexpiredLeases(3)
                .minSupportCode(0)
                .build();

        assertEquals(0, summary.getMinSupportCode());
    }

    @Test
    void toString_containsAllFields() {
        TableMigrationSummary summary = TableMigrationSummary.builder()
                .totalActiveWorkersWithMetrics(5)
                .activeWorkersWithMetricsInLegacyTable(2)
                .activeWorkersWithMetricsInLeaseTable(3)
                .workersWithUnexpiredLeases(4)
                .minSupportCode(2)
                .build();

        String str = summary.toString();
        // Verify toString includes the field values
        assertTrue(str.contains("5"));
        assertTrue(str.contains("2"));
        assertTrue(str.contains("3"));
        assertTrue(str.contains("4"));
    }

    private static void assertTrue(boolean condition) {
        org.junit.jupiter.api.Assertions.assertTrue(condition);
    }
}
