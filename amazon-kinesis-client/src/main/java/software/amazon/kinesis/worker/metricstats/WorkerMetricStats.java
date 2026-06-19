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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.EntityDAO.Entity;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.utils.ExponentialMovingAverage;
import software.amazon.kinesis.worker.metric.WorkerMetricType;

import static java.util.Objects.isNull;
import static software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer.LEASE_KEY_KEY;

/**
 * DataModel for a WorkerMetric, this data model is used to store the current state of a Worker in terms of relevant
 * WorkerMetric(CPU, Memory, Network).
 *
 * workerId : unique worker identifier, this is equivalent to the owner attribute from the lease table.
 * entityType : discriminator written to DDB so shared-table scans can filter by entity type.
 * lastUpdateTime : wall epoch in seconds when the entry was last updated
 * metricStats : Map of WorkerMetric to last N values for it. e.g. entry "CPU" : [10,20,12,10] etc
 * operatingRange : Map of WorkerMetric to its operating range. First item in the list of values defines the max limit.
 * properties : Arbitrary key-value properties for extensibility.
 * supportCode : Tracks the highest "breaking" feature ordinal supported by this worker. Used by the leader to
 *               determine fleet-wide feature support for backward-compatibility and rollback safety.
 * supportCodeUpdateEpochSeconds : wall epoch in seconds when the support code was last heartbeated. Since older
 *                                 versions performing partial DDB updates would leave stale supportCode values
 *                                 after rollback, this timestamp lets the leader distinguish current from stale.
 * metricStatsMap : runtime computed WorkerMetric name to its average value map. This field is not stored in ddb
 *                        and is used during Lease assignment only
 *
 * Inner subclasses {@link LegacyWorkerMetricStats} (PK: wid) and {@link LeaseTableWorkerMetricStats} (PK: leaseKey)
 * override the partition key annotation on getWorkerId() for the legacy worker-metrics table vs. the unified lease
 * table respectively. The DAO delegates select the appropriate subclass for the DynamoDB Enhanced Client.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
@SuperBuilder
@DynamoDbBean
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
@KinesisClientInternalApi
public class WorkerMetricStats implements Entity {

    public static final String KEY_LAST_UPDATE_TIME = "lut";
    public static final String KEY_WORKER_ID = "wid";

    public static final String ENTITY_TYPE_ATTRIBUTE_NAME = "entityType";

    /**
     * Add any new "breaking" feature to the END of this list. Features list must maintain chronological order.
     */
    @Getter
    @RequiredArgsConstructor
    public enum Features {
        ZERO_INDEX_PLACEHOLDER("ZERO_INDEX_PLACEHOLDER"), // ordinals are 0-indexed, but 0 should mean no support
        SINGLE_TABLE_MIGRATION("SINGLE_TABLE_MIGRATION");

        private final String name;
    }

    /**
     * Immutable snapshot of a worker's support code information. Extracted from a
     * {@link WorkerMetricStats} entry to decouple support-code evaluation from the
     * full worker metrics model (which carries metric stats, operating ranges, etc.
     * that are irrelevant to feature-gate decisions).
     *
     * <p>Used by {@link FleetSupportCodeEvaluator} to determine fleet-wide minimum
     * support without needing access to the full WorkerMetricStats objects.</p>
     */
    @Builder
    @Getter
    @ToString
    @EqualsAndHashCode
    public static class WorkerSupportInfo {
        /**
         * The support code value reported by this worker. Null if the worker is on an
         * older version that predates support code tracking.
         */
        private final Integer supportCode;

        /**
         * Epoch seconds when the support code was last heartbeated. Null if never set.
         * This single timestamp determines both freshness of the support claim and whether
         * the worker is actively participating — if a worker is alive and on a version that
         * supports the feature, it will keep heartbeating this field. If stale, the worker
         * either rolled back or is dead, and in both cases we cannot trust the support code.
         */
        private final Long supportCodeUpdateEpochSeconds;

        /**
         * Returns true if the support code heartbeat is expired (stale) based on the given threshold.
         * A stale support code means we cannot trust this worker supports the feature — either
         * the worker rolled back to an older version or is dead.
         */
        public boolean isSupportCodeExpired(final Duration maxAge) {
            if (supportCodeUpdateEpochSeconds == null) {
                return true;
            }
            final Duration elapsed =
                    Duration.between(Instant.ofEpochSecond(supportCodeUpdateEpochSeconds), Instant.now());
            return elapsed.compareTo(maxAge) > 0;
        }
    }

    /**
     * This support code is incremented when adding "breaking" features whose fleetwide support must be
     * tracked by the leader to ensure backward-compatibility and rollback safety. The support last
     * update time field is the worker's way of "heartbeating" that the support code is up-to-date,
     * since versions before the support code's introduction will be doing a DynamoDB partial update
     * to the worker metrics item and would persist the support code field after rollback.
     */
    static final Integer SUPPORT_CODE = Features.values().length - 1; // 1-indexed

    @Getter
    private String workerId;

    /** entityType discriminator written to DDB so shared-table scans can filter by entity type.
     *  Stored as the {@link EntityType#getDdbValue()} string for consistency with the rest of
     *  the codebase (CoordinatorState, DynamoDBLeaseTableDao, unified scans). */
    @Builder.Default
    @Getter(onMethod_ = {@DynamoDbIgnore})
    private EntityType entityType = EntityType.WORKER_METRIC_STATS;

    /**
     * DDB Enhanced Client accessor: serializes entityType using {@link EntityType#getDdbValue()}.
     */
    @DynamoDbAttribute(ENTITY_TYPE_ATTRIBUTE_NAME)
    public String getEntityTypeDdbValue() {
        return entityType != null ? entityType.getDdbValue() : null;
    }

    /**
     * DDB Enhanced Client accessor: deserializes entityType from the DDB value string.
     */
    public void setEntityTypeDdbValue(final String ddbValue) {
        this.entityType = EntityType.fromDdbValue(ddbValue);
    }

    @Getter(onMethod_ = {@DynamoDbAttribute(KEY_LAST_UPDATE_TIME)})
    private Long lastUpdateTime;

    @Getter(onMethod_ = {@DynamoDbAttribute("sts")})
    private Map<String, List<Double>> metricStats;

    @Getter(onMethod_ = {@DynamoDbAttribute("opr")})
    private Map<String, List<Long>> operatingRange;

    @Getter(onMethod_ = {@DynamoDbAttribute("properties")})
    private Map<String, String> properties;

    @Getter(onMethod_ = {@DynamoDbAttribute("sup")})
    private Integer supportCode;

    @Getter(onMethod_ = {@DynamoDbAttribute("slu")})
    private Long supportCodeUpdateEpochSeconds;

    /**
     * This map contains the WorkerMetric to its metric stat value. Metric stat value stored in this is exponentially averaged over
     * available number of different datapoints.
     */
    @Getter(onMethod_ = {@DynamoDbIgnore})
    @EqualsAndHashCode.Exclude
    @Builder.Default
    private Map<String, Double> metricStatsMap = new HashMap<>();

    /**
     * Alpha value used to compute the exponential moving average for worker metrics values.
     */
    @Getter(onMethod_ = {@DynamoDbIgnore})
    @EqualsAndHashCode.Exclude
    @Builder.Default
    private double emaAlpha = 0.2;

    /**
     * Schema for worker stats table uses "wid" as the partition key.
     */
    @DynamoDbBean
    @NoArgsConstructor
    @SuperBuilder
    public static class LegacyWorkerMetricStats extends WorkerMetricStats {
        @Override
        @DynamoDbPartitionKey
        @DynamoDbAttribute(KEY_WORKER_ID)
        public String getWorkerId() {
            return super.getWorkerId();
        }
    }

    /**
     * Schema for lease table uses "leaseKey" as the partition key.
     */
    @DynamoDbBean
    @NoArgsConstructor
    @SuperBuilder
    public static class LeaseTableWorkerMetricStats extends WorkerMetricStats {
        @Override
        @DynamoDbPartitionKey
        @DynamoDbAttribute(LEASE_KEY_KEY)
        public String getWorkerId() {
            return super.getWorkerId();
        }
    }

    /**
     * Returns true if given {@param workerMetricName} is available for the current worker else false
     */
    public boolean containsMetricStat(final String workerMetricName) {
        return metricStats.containsKey(workerMetricName);
    }

    /**
     * Returns the value for given WorkerMetricStats name.
     */
    public double getMetricStat(final String workerMetricName) {
        return metricStatsMap.computeIfAbsent(workerMetricName, (key) -> computeAverage(metricStats.get(key)));
    }

    /**
     * Increase the WorkerMetricStats value by given increaseLoadPercentage. This is done during execution of LAM and
     * as assignments are happening the current metric stat value is increased based on increaseLoadPercentage.
     */
    public void extrapolateMetricStatValuesForAddedThroughput(
            final Map<String, Double> workerMetricsToFleetLevelAverageMap,
            final double averageThroughput,
            final double increaseThroughput,
            final double averageLeaseCount) {

        metricStatsMap.replaceAll((key, value) -> extrapolateMetricsValue(
                key,
                workerMetricsToFleetLevelAverageMap.get(key),
                averageThroughput,
                increaseThroughput,
                averageLeaseCount));
    }

    private double extrapolateMetricsValue(
            final String metricName,
            final double fleetLevelMetricAverage,
            final double averageThroughput,
            final double increaseThroughput,
            final double averageLeaseCount) {

        if (averageThroughput > 0) {
            return metricStatsMap.get(metricName) + increaseThroughput * fleetLevelMetricAverage / averageThroughput;
        } else {
            return metricStatsMap.get(metricName) + fleetLevelMetricAverage / averageLeaseCount;
        }
    }

    public boolean willAnyMetricStatsGoAboveAverageUtilizationOrOperatingRange(
            final Map<String, Double> workerMetricsToFleetLevelAverageMap,
            final double averageThroughput,
            final double increaseThroughput,
            final double averageLeaseCount) {
        for (final String metricStatName : metricStats.keySet()) {
            final double fleetLevelAverageForMetric = workerMetricsToFleetLevelAverageMap.get(metricStatName);
            final double updatedValueToBe = extrapolateMetricsValue(
                    metricStatName,
                    fleetLevelAverageForMetric,
                    averageThroughput,
                    increaseThroughput,
                    averageLeaseCount);

            if (updatedValueToBe > fleetLevelAverageForMetric
                    || updatedValueToBe > operatingRange.get(metricStatName).get(0)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Increase the metric stat value corresponding to the added single lease. This is done during execution of LAM and
     * as assignments are happening the load is increase for LAM to determine workers for assignment.
     * The increase is done considering that for a WorkerMetric the fleet level average would be met when fleet level
     * average leases are assigned to a worker and thus 1 lease addition increases the metric stat value by fleet level
     * average of metric stat by averageLeaseCount
     */
    public void extrapolateMetricStatValuesForAddedLease(
            final Map<String, Double> workerMetricToFleetLevelAverage, final int averageLeaseCount) {
        for (Map.Entry<String, Double> workerMetricToMetricStat : metricStatsMap.entrySet()) {
            final String workerMetricName = workerMetricToMetricStat.getKey();
            final Double updatedValue = workerMetricToMetricStat.getValue()
                    + workerMetricToFleetLevelAverage.get(workerMetricName) / averageLeaseCount;
            metricStatsMap.replace(workerMetricName, updatedValue);
        }
    }

    /**
     * Determines percentage of load to reach the mean for the worker. In case of multiple worker metrics the metric stat
     * value closest to mean is used to determine the percentage value. This value is indication of how much load in
     * percentage to current load the worker can take to reach mean value.
     * @param workerMetricToFleetLevelAverage : WorkerMetric to fleet level mean value.
     * @return percentage to reach mean based on the WorkerMetric closest to its corresponding average.
     */
    public double computePercentageToReachAverage(final Map<String, Double> workerMetricToFleetLevelAverage) {
        double minDifferencePercentage = Double.MAX_VALUE;
        for (final String workerMetricName : metricStats.keySet()) {
            final double metricStatValue = getMetricStat(workerMetricName);
            final double differenceRatio;
            if (metricStatValue == 0D) {
                // If metric stat value is 0 that means this worker does not have any load so we assume that this worker
                // can take 100% more load than the current to reach average.
                differenceRatio = 1;
            } else {
                differenceRatio =
                        (workerMetricToFleetLevelAverage.get(workerMetricName) - metricStatValue) / metricStatValue;
            }
            minDifferencePercentage = Math.min(minDifferencePercentage, differenceRatio);
        }
        return minDifferencePercentage;
    }

    private Double computeAverage(final List<Double> values) {
        if (values.isEmpty()) {
            return 0D;
        }
        final ExponentialMovingAverage average = new ExponentialMovingAverage(emaAlpha);
        // Ignore -1 which denotes the WorkerMetric failure when calculating average, as it possible in past
        // one of the value is -1 due to some intermediate failure, and it has recovered since.
        values.forEach(value -> {
            if (value != -1) {
                average.add(value);
            }
        });
        return average.getValue();
    }

    /**
     * Returns true if any of the metric stat values has -1 in last index which represents that the metric stat value
     * was not successfully fetched in last attempt by worker.
     *
     * @return true if any metric stat value has -1 in last index, false otherwise.
     */
    public boolean isAnyWorkerMetricFailing() {
        boolean response = false;
        if (isUsingDefaultWorkerMetric()) {
            return response;
        }
        for (final Map.Entry<String, List<Double>> resourceStatsEntry : metricStats.entrySet()) {
            if (resourceStatsEntry.getValue().isEmpty()) {
                continue;
            }
            final Double lastEntry = resourceStatsEntry
                    .getValue()
                    .get(resourceStatsEntry.getValue().size() - 1);
            if (lastEntry != null && lastEntry == -1D) {
                response = true;
                break;
            }
        }
        if (response) {
            log.warn("WorkerStats: {} has a WorkerMetric which is failing.", this);
        }
        return response;
    }

    /**
     * WorkerMetricStats entry is invalid
     * if any of the field from lastUpdateTime, operatingRange, resourcesStats are not present or
     * if resourcesStats is empty or
     * if any of the WorkerMetrics having resourceStats does not have operatingRange or
     * if operating range values are not present or
     * if maxUtilization is 0 for any WorkerMetric
     * @return true if the entry is valid false otherwise.
     */
    public boolean isValidWorkerMetric() {
        if (isNull(lastUpdateTime)) {
            return false;
        }
        if (isUsingDefaultWorkerMetric()) {
            return true;
        }
        if (isNull(metricStats) || isNull(operatingRange)) {
            return false;
        }
        for (final Map.Entry<String, List<Double>> entry : metricStats.entrySet()) {
            if (!operatingRange.containsKey(entry.getKey())) {
                return false;
            }
        }
        for (final Map.Entry<String, List<Long>> operatingRangeEntry : operatingRange.entrySet()) {
            // If operatingRange for a WorkerMetric is missing or if maxUtilization is 0 then its not valid entry.
            if (operatingRangeEntry.getValue().isEmpty()
                    || operatingRangeEntry.getValue().get(0) == 0) {
                return false;
            }
        }
        return true;
    }

    public boolean isAnyWorkerMetricAboveAverageUtilizationOrOperatingRange(
            final Map<String, Double> workerMetricToFleetLevelAverage) {
        for (final String workerMetricName : metricStats.keySet()) {
            final double value = getMetricStat(workerMetricName);
            if (value > workerMetricToFleetLevelAverage.get(workerMetricName)) {
                return true;
            }
        }
        // check if any metric stat value is above operating range.
        return workerMetricToFleetLevelAverage.keySet().stream().anyMatch(this::isWorkerMetricAboveOperatingRange);
    }

    /**
     * If a worker is not using an explicit WorkerMetric such as CPU, Memory, or Network, then it
     * is said to be using the default WorkerMetric. Load management then falls back to throughput.
     * @return true if the worker is not using an explicit WorkerMetric.
     */
    public boolean isUsingDefaultWorkerMetric() {
        if ((metricStats == null || metricStats.isEmpty()) && (operatingRange == null || operatingRange.isEmpty())) {
            return true;
        }
        if (metricStats != null) {
            return metricStats.entrySet().stream()
                    .anyMatch(entry -> entry.getKey().equals(WorkerMetricType.THROUGHPUT.name()));
        }
        return false;
    }

    /**
     * Evaluates if the given metric stat is above operatingRange for the given WorkerMetric name. If the WorkerMetric
     * does not exist returns false
     * @param workerMetricName WorkerMetric name to evaluate
     * @return true if metric stat exists and is above operatingRange for the WorkerMetric
     */
    public boolean isWorkerMetricAboveOperatingRange(final String workerMetricName) {
        return metricStatsMap.containsKey(workerMetricName)
                && metricStatsMap.get(workerMetricName)
                        > operatingRange.get(workerMetricName).get(0);
    }

    /**
     * Determines whether this worker metrics entry is expired (i.e., the worker is no longer active).
     * A worker is considered expired if the time elapsed since its {@code lastUpdateTime} exceeds
     * the given maximum allowed age.
     *
     * <p>For example, if the max age is {@code 2 * workerMetricsReporterFreqInMillis}, then a worker
     * that hasn't updated in more than that duration is considered expired/dead.</p>
     *
     * @param maxAge the maximum allowed age since lastUpdateTime. If the elapsed time since
     *        lastUpdateTime exceeds this duration, the worker is considered expired.
     * @return true if this worker's lastUpdateTime is older than maxAge from now, false otherwise.
     */
    public boolean isExpired(final Duration maxAge) {
        if (isNull(lastUpdateTime)) {
            return true;
        }
        final Duration elapsed = Duration.between(Instant.ofEpochSecond(lastUpdateTime), Instant.now());
        return elapsed.compareTo(maxAge) > 0;
    }

    /**
     * Determines if this worker metric entry is stale and eligible for cleanup/deletion.
     *
     * <p>A stale entry is one that hasn't been updated for longer than the
     * {@code staleCleanupThreshold}. Stale entries are typically much older than expired
     * entries — they represent workers that have been dead for a long time and whose entries
     * should be garbage-collected from the table.</p>
     *
     * @param staleCleanupThreshold the maximum allowed age. If the elapsed time since
     *        lastUpdateTime exceeds this duration, the entry is considered stale.
     * @return true if this entry's lastUpdateTime is older than staleCleanupThreshold from now.
     */
    public boolean isStale(final Duration staleCleanupThreshold) {
        if (isNull(lastUpdateTime)) {
            return true;
        }
        final Duration elapsed = Duration.between(Instant.ofEpochSecond(lastUpdateTime), Instant.now());
        return elapsed.compareTo(staleCleanupThreshold) > 0;
    }
}
