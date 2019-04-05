/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.metrics;

import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

/**
 * An IMetricsScope that accumulates data from multiple calls to addData with
 * the same name parameter. It tracks min, max, sample count, and sum for each
 * named metric.
 * 
 * @param <KeyType> can be a class or object defined by the user that stores information about a MetricDatum needed
 *        by the user.
 * 
 *        The following is a example of what a KeyType class might look like:
 *        class SampleKeyType {
 *              private long timeKeyCreated;
 *              private MetricDatum datum;
 *              public SampleKeyType(long timeKeyCreated, MetricDatum datum){
 *                  this.timeKeyCreated = timeKeyCreated;
 *                  this.datum = datum;
 *              }
 *        }
 */
public abstract class AccumulatingMetricsScope<KeyType> extends EndingMetricsScope {

    protected Map<KeyType, MetricDatum> data = new HashMap<>();

    @Override
    public void addData(String name, double value, StandardUnit unit) {
        addData(getKey(name), name, value, unit);
    }

    @Override
    public void addData(String name, double value, StandardUnit unit, MetricsLevel level) {
        addData(getKey(name), name, value, unit);
    }

    /**
     * @param name
     *            key name for a metric
     * @return the name of the key
     */
    protected abstract KeyType getKey(String name);

    /**
     * Adds data points to an IMetricsScope. Multiple calls to IMetricsScopes that have the 
     * same key will have their data accumulated.
     * 
     * @param key
     *        data point key
     * @param name
     *        data point name
     * @param value
     *        data point value
     * @param unit
     *        data point unit
     */
    public void addData(KeyType key, String name, double value, StandardUnit unit) {
        super.addData(name, value, unit);

        final MetricDatum datum = data.get(key);
        final MetricDatum metricDatum;
        if (datum == null) {
            metricDatum = MetricDatum.builder().metricName(name).unit(unit)
                    .statisticValues(
                            StatisticSet.builder().maximum(value).minimum(value).sampleCount(1.0).sum(value).build())
                    .build();
        } else {
            if (!datum.unit().equals(unit)) {
                throw new IllegalArgumentException("Cannot add to existing metric with different unit");
            }

            final StatisticSet oldStatisticSet = datum.statisticValues();
            final StatisticSet statisticSet = oldStatisticSet.toBuilder()
                    .maximum(Math.max(value, oldStatisticSet.maximum()))
                    .minimum(Math.min(value, oldStatisticSet.minimum())).sampleCount(oldStatisticSet.sampleCount() + 1)
                    .sum(oldStatisticSet.sum() + value).build();

            metricDatum = datum.toBuilder().statisticValues(statisticSet).build();
        }

        data.put(key, metricDatum);
    }
}
