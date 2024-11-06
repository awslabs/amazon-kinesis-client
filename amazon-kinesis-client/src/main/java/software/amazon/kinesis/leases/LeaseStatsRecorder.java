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

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.utils.ExponentialMovingAverage;

import static java.util.Objects.isNull;

/**
 * This class records the stats for the leases.
 * The stats are recorded in a thread safe queue, and the throughput is calculated by summing up the bytes and dividing
 * by interval in seconds.
 * This class is thread safe and backed by thread safe data structures.
 */
@RequiredArgsConstructor
@KinesisClientInternalApi
@ThreadSafe
public class LeaseStatsRecorder {

    /**
     * This default alpha is chosen based on the testing so far between simple average and moving average with 0.5.
     * In the future, if one value does not fit all use cases, inject this via config.
     */
    private static final double DEFAULT_ALPHA = 0.5;

    public static final int BYTES_PER_KB = 1024;

    private final Long renewerFrequencyInMillis;
    private final Map<String, Queue<LeaseStats>> leaseStatsMap = new ConcurrentHashMap<>();
    private final Map<String, ExponentialMovingAverage> leaseKeyToExponentialMovingAverageMap =
            new ConcurrentHashMap<>();
    private final Callable<Long> timeProviderInMillis;

    /**
     * This method provides happens-before semantics (i.e., the action (access or removal) from a thread happens
     * before the action from subsequent thread) for the stats recording in multithreaded environment.
     */
    public void recordStats(@NonNull final LeaseStats leaseStats) {
        final Queue<LeaseStats> leaseStatsQueue =
                leaseStatsMap.computeIfAbsent(leaseStats.getLeaseKey(), lease -> new ConcurrentLinkedQueue<>());
        leaseStatsQueue.add(leaseStats);
    }

    /**
     * Calculates the throughput in KBps for the given leaseKey.
     * Method first clears the items that are older than {@link #renewerFrequencyInMillis} from the queue and then
     * calculates the throughput per second during {@link #renewerFrequencyInMillis} interval and then returns the
     * ExponentialMovingAverage of the throughput. If method is called in quick succession with or without new stats
     * the result can be different as ExponentialMovingAverage decays old values on every new call.
     * This method is thread safe.
     * @param leaseKey leaseKey for which stats are required
     * @return throughput in Kbps, returns null if there is no stats available for the leaseKey.
     */
    public Double getThroughputKBps(final String leaseKey) {
        final Queue<LeaseStats> leaseStatsQueue = leaseStatsMap.get(leaseKey);

        if (isNull(leaseStatsQueue)) {
            // This means there is no entry for this leaseKey yet
            return null;
        }

        filterExpiredEntries(leaseStatsQueue);

        // Convert bytes into KB and divide by interval in second to get throughput per second.
        final ExponentialMovingAverage exponentialMovingAverage = leaseKeyToExponentialMovingAverageMap.computeIfAbsent(
                leaseKey, leaseId -> new ExponentialMovingAverage(DEFAULT_ALPHA));

        // Specifically dividing by 1000.0 rather than using Duration class to get seconds, because Duration class
        // implementation rounds off to seconds and precision is lost.
        final double frequency = renewerFrequencyInMillis / 1000.0;
        final double throughput = readQueue(leaseStatsQueue).stream()
                        .mapToDouble(LeaseStats::getBytes)
                        .sum()
                / BYTES_PER_KB
                / frequency;
        exponentialMovingAverage.add(throughput);
        return exponentialMovingAverage.getValue();
    }

    /**
     * Gets the currentTimeMillis and then iterates over the queue to get the stats with creation time less than
     * currentTimeMillis.
     * This is specifically done to avoid potential race between with high-frequency put thread blocking get thread.
     */
    private Queue<LeaseStats> readQueue(final Queue<LeaseStats> leaseStatsQueue) {
        final long currentTimeMillis = getCurrenTimeInMillis();
        final Queue<LeaseStats> response = new LinkedList<>();
        for (LeaseStats leaseStats : leaseStatsQueue) {
            if (leaseStats.creationTimeMillis > currentTimeMillis) {
                break;
            }
            response.add(leaseStats);
        }
        return response;
    }

    private long getCurrenTimeInMillis() {
        try {
            return timeProviderInMillis.call();
        } catch (final Exception e) {
            // Fallback to using the System.currentTimeMillis if failed.
            return System.currentTimeMillis();
        }
    }

    private void filterExpiredEntries(final Queue<LeaseStats> leaseStatsQueue) {
        final long currentTime = getCurrenTimeInMillis();
        while (!leaseStatsQueue.isEmpty()) {
            final LeaseStats leaseStats = leaseStatsQueue.peek();
            if (isNull(leaseStats) || currentTime - leaseStats.getCreationTimeMillis() < renewerFrequencyInMillis) {
                break;
            }
            leaseStatsQueue.poll();
        }
    }

    /**
     * Clear the in-memory stats for the lease when a lease is reassigned (due to shut down or lease stealing)
     * @param leaseKey leaseKey, for which stats are supposed to be clear.
     */
    public void dropLeaseStats(final String leaseKey) {
        leaseStatsMap.remove(leaseKey);
        leaseKeyToExponentialMovingAverageMap.remove(leaseKey);
    }

    @Builder
    @Getter
    @ToString
    @KinesisClientInternalApi
    public static final class LeaseStats {
        /**
         * Lease key for which this leaseStats object is created.
         */
        private final String leaseKey;
        /**
         * Bytes that are processed for a lease
         */
        private final long bytes;
        /**
         * Wall time in epoch millis at which this leaseStats object was created. This time is used to determine the
         * expiry of the lease stats.
         */
        @Builder.Default
        private final long creationTimeMillis = System.currentTimeMillis();
    }
}
