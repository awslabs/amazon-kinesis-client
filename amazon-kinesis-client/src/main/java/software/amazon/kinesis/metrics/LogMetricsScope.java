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



import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

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
            output.append(String.format("%s[%s: %s]", needsComma ? ", " : "", dimension.name(), dimension.value()));
            needsComma = true;
        }
        output.append("\n");

        for (MetricDatum datum : data.values()) {
            StatisticSet statistics = datum.statisticValues();
            output.append(String.format("Name=%25s\tMin=%.2f\tMax=%.2f\tCount=%.2f\tSum=%.2f\tAvg=%.2f\tUnit=%s\n",
                    datum.metricName(),
                    statistics.minimum(),
                    statistics.maximum(),
                    statistics.sampleCount(),
                    statistics.sum(),
                    statistics.sum() / statistics.sampleCount(),
                    datum.unit()));
        }

        log.info(output.toString());
    }
}
