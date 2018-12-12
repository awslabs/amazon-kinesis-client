/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.metrics.impl;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

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

    protected Map<KeyType, MetricDatum> data = new HashMap<KeyType, MetricDatum>();

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

        MetricDatum datum = data.get(key);
        if (datum == null) {
            data.put(key,
                    new MetricDatum().withMetricName(name)
                            .withUnit(unit)
                            .withStatisticValues(new StatisticSet().withMaximum(value)
                                    .withMinimum(value)
                                    .withSampleCount(1.0)
                                    .withSum(value)));
        } else {
            if (!datum.getUnit().equals(unit.name())) {
                throw new IllegalArgumentException("Cannot add to existing metric with different unit");
            }

            StatisticSet statistics = datum.getStatisticValues();
            statistics.setMaximum(Math.max(value, statistics.getMaximum()));
            statistics.setMinimum(Math.min(value, statistics.getMinimum()));
            statistics.setSampleCount(statistics.getSampleCount() + 1);
            statistics.setSum(statistics.getSum() + value);
        }
    }
}
