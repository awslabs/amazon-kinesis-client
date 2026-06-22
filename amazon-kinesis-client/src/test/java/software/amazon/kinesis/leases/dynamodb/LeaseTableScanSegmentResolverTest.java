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
package software.amazon.kinesis.leases.dynamodb;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LeaseTableScanSegmentResolverTest {

    private static final long BYTES_PER_GB = 1024L * 1024 * 1024;

    private static DescribeTableResponse describeWithSizeBytes(final long bytes) {
        return DescribeTableResponse.builder()
                .table(TableDescription.builder().tableSizeBytes(bytes).build())
                .build();
    }

    @Test
    void calculateTotalSegments_appliesFormulaAndBounds() {
        // Empty / tiny table -> minimum of 1 segment.
        assertEquals(1, LeaseTableScanSegmentResolver.calculateTotalSegments(0));
        // 0.2GB -> exactly 1 segment.
        assertEquals(1, LeaseTableScanSegmentResolver.calculateTotalSegments((long) (0.2 * BYTES_PER_GB)));
        // 1GB -> ceil(1 / 0.2) = 5 segments.
        assertEquals(5, LeaseTableScanSegmentResolver.calculateTotalSegments(BYTES_PER_GB));
        // Very large table -> clamped to the maximum of 32 segments.
        assertEquals(32, LeaseTableScanSegmentResolver.calculateTotalSegments(100 * BYTES_PER_GB));
    }

    @Test
    void resolveTotalSegments_usesConfiguredValueWithoutDescribingTableAndIsNotCapped() throws Exception {
        // 50 is above the dynamic maximum (32); a configured value must be honored as-is.
        final int configuredSegments = 50;
        final AtomicInteger describeCalls = new AtomicInteger(0);
        final LeaseTableScanSegmentResolver resolver = new LeaseTableScanSegmentResolver(configuredSegments, () -> {
            describeCalls.incrementAndGet();
            return describeWithSizeBytes(100 * BYTES_PER_GB);
        });

        assertEquals(configuredSegments, resolver.resolveTotalSegments());
        // Configured value short-circuits dynamic sizing entirely.
        assertEquals(0, describeCalls.get());
    }

    @Test
    void resolveTotalSegments_computesDynamicallyWhenNotConfigured() throws Exception {
        final LeaseTableScanSegmentResolver resolver =
                new LeaseTableScanSegmentResolver(0, () -> describeWithSizeBytes(BYTES_PER_GB));

        assertEquals(5, resolver.resolveTotalSegments());
    }

    @Test
    void resolveTotalSegments_usesDefaultFactorWhenTableMissing() throws Exception {
        final LeaseTableScanSegmentResolver resolver = new LeaseTableScanSegmentResolver(0, () -> null);

        assertEquals(
                LeaseTableScanSegmentResolver.DEFAULT_LEASE_TABLE_SCAN_PARALLELISM_FACTOR,
                resolver.resolveTotalSegments());
    }

    @Test
    void resolveTotalSegments_fallsBackGracefullyWhenDescribeThrows() {
        final LeaseTableScanSegmentResolver resolver = new LeaseTableScanSegmentResolver(0, () -> {
            throw new RuntimeException("access denied");
        });

        assertEquals(
                LeaseTableScanSegmentResolver.DEFAULT_LEASE_TABLE_SCAN_PARALLELISM_FACTOR,
                resolver.resolveTotalSegments());
    }

    @Test
    void resolveTotalSegments_retriesDescribeAfterFailure() {
        final AtomicInteger describeCalls = new AtomicInteger(0);
        final LeaseTableScanSegmentResolver resolver = new LeaseTableScanSegmentResolver(0, () -> {
            if (describeCalls.incrementAndGet() == 1) {
                throw new RuntimeException("transient failure");
            }
            return describeWithSizeBytes(BYTES_PER_GB);
        });

        // First call fails — falls back to default
        assertEquals(
                LeaseTableScanSegmentResolver.DEFAULT_LEASE_TABLE_SCAN_PARALLELISM_FACTOR,
                resolver.resolveTotalSegments());
        // Second call retries and succeeds
        assertEquals(5, resolver.resolveTotalSegments());
        assertEquals(2, describeCalls.get());
    }

    @Test
    void resolveTotalSegments_cachesDynamicResult() throws Exception {
        final AtomicInteger describeCalls = new AtomicInteger(0);
        final LeaseTableScanSegmentResolver resolver = new LeaseTableScanSegmentResolver(0, () -> {
            describeCalls.incrementAndGet();
            return describeWithSizeBytes(BYTES_PER_GB);
        });

        assertEquals(5, resolver.resolveTotalSegments());
        assertEquals(5, resolver.resolveTotalSegments());
        // The DescribeTable result is cached, so the describer is only invoked once.
        assertEquals(1, describeCalls.get());
    }
}
