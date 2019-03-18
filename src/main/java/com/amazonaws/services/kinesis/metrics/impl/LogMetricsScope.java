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
package com.amazonaws.services.kinesis.metrics.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StatisticSet;

/**
 * An AccumulatingMetricsScope that outputs via log4j.
 */
public class LogMetricsScope extends AccumulateByNameMetricsScope {

    private static final Log LOG = LogFactory.getLog(LogMetricsScope.class);

    @Override
    public void end() {
        StringBuilder output = new StringBuilder();
        output.append("Metrics:\n");

        output.append("Dimensions: ");
        boolean needsComma = false;
        for (Dimension dimension : getDimensions()) {
            output.append(String.format("%s[%s: %s]", needsComma ? ", " : "", dimension.getName(), dimension.getValue()));
            needsComma = true;
        }
        output.append("\n");

        for (MetricDatum datum : data.values()) {
            StatisticSet statistics = datum.getStatisticValues();
            output.append(String.format("Name=%25s\tMin=%.2f\tMax=%.2f\tCount=%.2f\tSum=%.2f\tAvg=%.2f\tUnit=%s\n",
                    datum.getMetricName(),
                    statistics.getMinimum(),
                    statistics.getMaximum(),
                    statistics.getSampleCount(),
                    statistics.getSum(),
                    statistics.getSum() / statistics.getSampleCount(),
                    datum.getUnit()));
        }

        LOG.info(output.toString());
    }
}
