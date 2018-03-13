/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License. 
 */
package software.amazon.aws.services.kinesis.metrics.impl;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StatisticSet;

import lombok.extern.slf4j.Slf4j;

/**
 * An AccumulatingMetricsScope that outputs via log4j.
 */
@Slf4j
public class LogMetricsScope extends AccumulateByNameMetricsScope {
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

        log.info(output.toString());
    }
}
