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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Metrics scope for CloudWatch metrics.
 */
public class CloudWatchMetricsScope extends FilteringMetricsScope implements MetricsScope {

    private CloudWatchPublisherRunnable publisher;

    /**
     * Creates a CloudWatch metrics scope with given metrics level and enabled dimensions.
     * @param publisher Publisher that emits CloudWatch metrics periodically.
     * @param metricsLevel Metrics level to enable. All data with level below this will be dropped.
     * @param metricsEnabledDimensions Enabled dimensions for CloudWatch metrics.
     */
    public CloudWatchMetricsScope(
            CloudWatchPublisherRunnable publisher, MetricsLevel metricsLevel, Set<String> metricsEnabledDimensions) {
        super(metricsLevel, metricsEnabledDimensions);
        this.publisher = publisher;
    }

    /**
     * Once we call this method, all MetricDatums added to the scope will be enqueued to the publisher runnable.
     * We enqueue MetricDatumWithKey because the publisher will aggregate similar metrics (i.e. MetricDatum with the
     * same metricName) in the background thread. Hence aggregation using MetricDatumWithKey will be especially useful
     * when aggregating across multiple MetricScopes.
     */
    @Override
    public void end() {
        super.end();

        final List<MetricDatumWithKey<CloudWatchMetricKey>> dataWithKeys = data.values().stream()
                .map(metricDatum ->
                        metricDatum.toBuilder().dimensions(getDimensions()).build())
                .map(metricDatum -> new MetricDatumWithKey<>(new CloudWatchMetricKey(metricDatum), metricDatum))
                .collect(Collectors.toList());

        publisher.enqueue(dataWithKeys);
    }
}
