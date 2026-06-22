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

import java.time.Duration;
import java.time.Instant;

import com.google.common.annotations.VisibleForTesting;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

@Slf4j
@RequiredArgsConstructor
@KinesisClientInternalApi
class LeaseTableScanSegmentResolver {

    /**
     * Supplies the table description used for dynamic sizing. Implementations should return
     * {@code null} when the table does not (yet) exist.
     */
    @FunctionalInterface
    interface TableDescriber {
        DescribeTableResponse describe() throws Exception;
    }

    /**
     * Default parallelism factor used when the table size cannot be determined.
     */
    static final int DEFAULT_LEASE_TABLE_SCAN_PARALLELISM_FACTOR = 10;

    private static final long NUMBER_OF_BYTES_PER_GB = 1024 * 1024 * 1024;
    private static final double GB_PER_SEGMENT = 0.2;
    private static final int MIN_SCAN_SEGMENTS = 1;
    private static final int MAX_SCAN_SEGMENTS = 32;
    private static final Duration CACHE_DURATION_FOR_TOTAL_SEGMENTS = Duration.ofHours(2);

    /**
     * Customer-configured number of segments. When {@code <= 0}, segments are sized dynamically.
     */
    private final int configuredTotalSegments;

    private final TableDescriber tableDescriber;

    private Integer cachedTotalSegments;
    private Instant expirationTimeForTotalSegmentsCache;

    @VisibleForTesting
    static int calculateTotalSegments(final long tableSizeBytes) {
        final double tableSizeGB = (double) tableSizeBytes / NUMBER_OF_BYTES_PER_GB;
        return Math.min(Math.max((int) Math.ceil(tableSizeGB / GB_PER_SEGMENT), MIN_SCAN_SEGMENTS), MAX_SCAN_SEGMENTS);
    }

    public synchronized int resolveTotalSegments() {
        if (configuredTotalSegments > 0) {
            return configuredTotalSegments;
        }
        if (isTotalSegmentsCacheValid()) {
            return cachedTotalSegments;
        }

        int totalSegments =
                cachedTotalSegments == null ? DEFAULT_LEASE_TABLE_SCAN_PARALLELISM_FACTOR : cachedTotalSegments;
        try {
            final DescribeTableResponse describeTableResponse = tableDescriber.describe();
            if (describeTableResponse == null) {
                log.info("DescribeTable returned null, using totalSegments: {}", totalSegments);
            } else {
                totalSegments =
                        calculateTotalSegments(describeTableResponse.table().tableSizeBytes());
                log.info("TotalSegments for Lease table parallel scan: {}", totalSegments);
            }
            cachedTotalSegments = totalSegments;
            expirationTimeForTotalSegmentsCache = Instant.now().plus(CACHE_DURATION_FOR_TOTAL_SEGMENTS);
        } catch (final Exception e) {
            log.warn("DescribeTable failed, using totalSegments: {}. Error: {}", totalSegments, e.getMessage());
        }
        return totalSegments;
    }

    private boolean isTotalSegmentsCacheValid() {
        return cachedTotalSegments != null && Instant.now().isBefore(expirationTimeForTotalSegmentsCache);
    }
}
