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
package software.amazon.kinesis.worker.metricstats;

import java.time.Duration;
import java.time.Instant;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.leases.EntityDAO;
import software.amazon.kinesis.leases.EntityType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the Entity type system additions to WorkerMetricStats:
 * - EntityDAO.Entity interface implementation
 * - entityType field serialization/deserialization via DDB Enhanced Client accessors
 * - Support code expiration logic
 * - Staleness and expiration time-based checks
 * - WorkerSupportInfo support code expiration
 */
class WorkerMetricStatsEntityTypeTest {

    // --- Entity interface and entityType field tests ---

    @Test
    void implementsEntityInterface() {
        final WorkerMetricStats stats =
                WorkerMetricStats.builder().workerId("worker-1").build();
        assertTrue(stats instanceof EntityDAO.Entity, "WorkerMetricStats should implement EntityDAO.Entity");
    }

    @Test
    void getEntityType_defaultBuilder_returnsEntityType() {
        final WorkerMetricStats stats =
                WorkerMetricStats.builder().workerId("worker-1").build();
        // entityType is not set by default builder
        assertEquals("WORKER_METRIC_STATS", stats.getEntityTypeDdbValue());
    }

    @Test
    void getEntityType_whenSetExplicitly_returnsWorkerMetricStats() {
        final WorkerMetricStats stats = WorkerMetricStats.builder()
                .workerId("worker-1")
                .entityType(EntityType.WORKER_METRIC_STATS)
                .build();
        assertEquals(EntityType.WORKER_METRIC_STATS, stats.getEntityType());
    }

    @Test
    void getEntityTypeDdbValue_whenSet_returnsDdbString() {
        final WorkerMetricStats stats = WorkerMetricStats.builder()
                .workerId("worker-1")
                .entityType(EntityType.WORKER_METRIC_STATS)
                .build();
        assertEquals("WORKER_METRIC_STATS", stats.getEntityTypeDdbValue());
    }

    @Test
    void setEntityTypeDdbValue_setsEntityTypeFromDdbString() {
        final WorkerMetricStats stats =
                WorkerMetricStats.builder().workerId("worker-1").build();

        stats.setEntityTypeDdbValue("WORKER_METRIC_STATS");

        assertEquals(EntityType.WORKER_METRIC_STATS, stats.getEntityType());
    }

    @Test
    void setEntityTypeDdbValue_withUnknownValue_setsNull() {
        final WorkerMetricStats stats = WorkerMetricStats.builder()
                .workerId("worker-1")
                .entityType(EntityType.WORKER_METRIC_STATS)
                .build();

        stats.setEntityTypeDdbValue("UNKNOWN_TYPE");

        assertNull(stats.getEntityType());
    }

    @Test
    void setEntityTypeDdbValue_withNull_setsNull() {
        final WorkerMetricStats stats = WorkerMetricStats.builder()
                .workerId("worker-1")
                .entityType(EntityType.WORKER_METRIC_STATS)
                .build();

        stats.setEntityTypeDdbValue(null);

        assertNull(stats.getEntityType());
    }

    // --- isExpired tests ---

    @Test
    void isExpired_withNullLastUpdateTime_returnsTrue() {
        final WorkerMetricStats stats =
                WorkerMetricStats.builder().workerId("worker-1").build();

        assertTrue(stats.isExpired(Duration.ofMinutes(5)));
    }

    @Test
    void isExpired_withRecentUpdate_returnsFalse() {
        final WorkerMetricStats stats = WorkerMetricStats.builder()
                .workerId("worker-1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .build();

        assertFalse(stats.isExpired(Duration.ofMinutes(5)));
    }

    @Test
    void isExpired_withOldUpdate_returnsTrue() {
        final long tenMinutesAgo = Instant.now().minus(Duration.ofMinutes(10)).getEpochSecond();
        final WorkerMetricStats stats = WorkerMetricStats.builder()
                .workerId("worker-1")
                .lastUpdateTime(tenMinutesAgo)
                .build();

        assertTrue(stats.isExpired(Duration.ofMinutes(5)));
    }

    // --- isStale tests ---

    @Test
    void isStale_withNullLastUpdateTime_returnsTrue() {
        final WorkerMetricStats stats =
                WorkerMetricStats.builder().workerId("worker-1").build();

        assertTrue(stats.isStale(Duration.ofHours(1)));
    }

    @Test
    void isStale_withRecentUpdate_returnsFalse() {
        final WorkerMetricStats stats = WorkerMetricStats.builder()
                .workerId("worker-1")
                .lastUpdateTime(Instant.now().getEpochSecond())
                .build();

        assertFalse(stats.isStale(Duration.ofHours(1)));
    }

    @Test
    void isStale_withVeryOldUpdate_returnsTrue() {
        final long twoHoursAgo = Instant.now().minus(Duration.ofHours(2)).getEpochSecond();
        final WorkerMetricStats stats = WorkerMetricStats.builder()
                .workerId("worker-1")
                .lastUpdateTime(twoHoursAgo)
                .build();

        assertTrue(stats.isStale(Duration.ofHours(1)));
    }

    // --- WorkerSupportInfo tests ---

    @Test
    void workerSupportInfo_isSupportCodeExpired_withNullTimestamp_returnsTrue() {
        final WorkerMetricStats.WorkerSupportInfo info =
                WorkerMetricStats.WorkerSupportInfo.builder().supportCode(1).build();

        assertTrue(info.isSupportCodeExpired(Duration.ofMinutes(5)));
    }

    @Test
    void workerSupportInfo_isSupportCodeExpired_withRecentTimestamp_returnsFalse() {
        final WorkerMetricStats.WorkerSupportInfo info = WorkerMetricStats.WorkerSupportInfo.builder()
                .supportCode(1)
                .supportCodeUpdateEpochSeconds(Instant.now().getEpochSecond())
                .build();

        assertFalse(info.isSupportCodeExpired(Duration.ofMinutes(5)));
    }

    @Test
    void workerSupportInfo_isSupportCodeExpired_withStaleTimestamp_returnsTrue() {
        final long twentyMinutesAgo =
                Instant.now().minus(Duration.ofMinutes(20)).getEpochSecond();
        final WorkerMetricStats.WorkerSupportInfo info = WorkerMetricStats.WorkerSupportInfo.builder()
                .supportCode(1)
                .supportCodeUpdateEpochSeconds(twentyMinutesAgo)
                .build();

        assertTrue(info.isSupportCodeExpired(Duration.ofMinutes(5)));
    }

    // --- SUPPORT_CODE constant and Features enum ---

    @Test
    void supportCode_constant_matchesFeaturesLength() {
        // SUPPORT_CODE should be Features.values().length - 1 (1-indexed)
        assertEquals(WorkerMetricStats.Features.values().length - 1, WorkerMetricStats.SUPPORT_CODE);
    }

    @Test
    void features_enum_hasExpectedValues() {
        assertEquals(0, WorkerMetricStats.Features.ZERO_INDEX_PLACEHOLDER.ordinal());
        assertEquals(1, WorkerMetricStats.Features.SINGLE_TABLE_MIGRATION.ordinal());
    }

    // --- LegacyWorkerMetricStats and LeaseTableWorkerMetricStats subclass tests ---

    @Test
    void legacyWorkerMetricStats_getWorkerId_returnsWorkerId() {
        final WorkerMetricStats.LegacyWorkerMetricStats legacyStats =
                WorkerMetricStats.LegacyWorkerMetricStats.builder()
                        .workerId("legacy-worker-1")
                        .lastUpdateTime(Instant.now().getEpochSecond())
                        .metricStats(ImmutableMap.of("CPU", ImmutableList.of(50.0)))
                        .build();

        assertEquals("legacy-worker-1", legacyStats.getWorkerId());
    }

    @Test
    void leaseTableWorkerMetricStats_getWorkerId_returnsWorkerId() {
        final WorkerMetricStats.LeaseTableWorkerMetricStats leaseTableStats =
                WorkerMetricStats.LeaseTableWorkerMetricStats.builder()
                        .workerId("lease-table-worker-1")
                        .lastUpdateTime(Instant.now().getEpochSecond())
                        .metricStats(ImmutableMap.of("CPU", ImmutableList.of(50.0)))
                        .build();

        assertEquals("lease-table-worker-1", leaseTableStats.getWorkerId());
    }

    @Test
    void leaseTableWorkerMetricStats_implementsEntityInterface() {
        final WorkerMetricStats.LeaseTableWorkerMetricStats stats = new WorkerMetricStats.LeaseTableWorkerMetricStats();
        stats.setWorkerId("worker-1");
        assertTrue(stats instanceof EntityDAO.Entity);
    }
}
