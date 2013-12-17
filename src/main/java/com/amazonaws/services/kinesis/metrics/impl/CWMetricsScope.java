/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.metrics.impl.AccumulateByNameMetricsScope;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;

public class CWMetricsScope extends AccumulateByNameMetricsScope implements IMetricsScope {

    private CWPublisherRunnable<CWMetricKey> publisher;

    /**
     * Each CWMetricsScope takes a publisher which contains the logic of when to publish metrics.
     * 
     * @param publisher publishing logic
     */

    public CWMetricsScope(CWPublisherRunnable<CWMetricKey> publisher) {
        this.publisher = publisher;
    }

    @Override
    public void addData(String name, double value, StandardUnit unit) {
        super.addData(name, value, unit);
    }

    @Override
    public void addDimension(String name, String value) {
        super.addDimension(name, value);
    }

    /*
     * Once we call this method, all MetricDatums added to the scope will be enqueued to the publisher runnable.
     * We enqueue MetricDatumWithKey because the publisher will aggregate similar metrics (i.e. MetricDatum with the
     * same metricName) in the background thread. Hence aggregation using MetricDatumWithKey will be especially useful
     * when aggregating across multiple MetricScopes.
     */
    @Override
    public void end() {
        super.end();

        List<MetricDatumWithKey<CWMetricKey>> dataWithKeys = new ArrayList<MetricDatumWithKey<CWMetricKey>>();

        for (MetricDatum datum : data.values()) {
            datum.setDimensions(getDimensions());
            dataWithKeys.add(new MetricDatumWithKey<CWMetricKey>(new CWMetricKey(datum), datum));
        }

        publisher.enqueue(dataWithKeys);
    }

}
