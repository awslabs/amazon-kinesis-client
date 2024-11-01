package software.amazon.kinesis.worker.metricstats;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.kinesis.utils.ExponentialMovingAverage;
import software.amazon.kinesis.worker.metric.WorkerMetricType;

import static java.util.Objects.isNull;

/**
 * DataModel for a WorkerMetric, this data model is used to store the current state of a Worker in terms of relevant
 * WorkerMetric(CPU, Memory, Network).
 *
 * workerId : unique worker identifier, this is equivalent to the owner attribute from the lease table.
 * lastUpdateTime : wall epoch in seconds when the entry was last updated
 * metricStats : Map of WorkerMetric to last N values for it. e.g. entry "CPU" : [10,20,12,10] etc
 * operatingRange : Map of WorkerMetric to its operating range. First item in the list of values defines the max limit.
 * metricStatsMap : runtime computed WorkerMetric name to its average value map. This field is not stored in ddb
 *                        and is used during Lease assignment only
 */
@Data
@Builder
@DynamoDbBean
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class WorkerMetricStats {

    static final String KEY_LAST_UPDATE_TIME = "lut";
    static final String KEY_WORKER_ID = "wid";

    @Getter(onMethod_ = {@DynamoDbPartitionKey, @DynamoDbAttribute(KEY_WORKER_ID)})
    private String workerId;

    @Getter(onMethod_ = {@DynamoDbAttribute(KEY_LAST_UPDATE_TIME)})
    private Long lastUpdateTime;

    @Getter(onMethod_ = {@DynamoDbAttribute("sts")})
    private Map<String, List<Double>> metricStats;

    @Getter(onMethod_ = {@DynamoDbAttribute("opr")})
    private Map<String, List<Long>> operatingRange;

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

    public static List<KeySchemaElement> getKeySchema() {
        return Collections.singletonList(KeySchemaElement.builder()
                .attributeName(KEY_WORKER_ID)
                .keyType(KeyType.HASH)
                .build());
    }

    public static List<AttributeDefinition> getAttributeDefinitions() {
        return Collections.singletonList(AttributeDefinition.builder()
                .attributeName(KEY_WORKER_ID)
                .attributeType(ScalarAttributeType.S)
                .build());
    }
}
