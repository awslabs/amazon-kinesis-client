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

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

/**
 * An {@code IMetricsScope} that filters {@link #addData} calls based on the provided metrics level. If the provided
 * metrics level is less than enabled level, then data is dropped. This class also adds the dimension to the scope
 * if it is enabled.
 */
public class FilteringMetricsScope extends AccumulateByNameMetricsScope {

    /**
     * Enabled level for the metrics. All metrics below this level will be dropped.
     */
    private final MetricsLevel metricsLevel;
    /**
     * Set of dimensions that are allowed to be emitted.
     */
    private final Set<String> metricsEnabledDimensions;

    /**
     * Flag that indicates whether all metrics dimensions are allowed or not.
     */
    private final boolean metricsEnabledDimensionsAll;

    /**
     * Creates a metrics scope that allows all metrics data and dimensions.
     */
    public FilteringMetricsScope() {
        this(MetricsLevel.DETAILED, ImmutableSet.of(METRICS_DIMENSIONS_ALL));
    }

    /**
     * Creates a metrics scope that drops data with level below the given enabled level and only allows dimensions
     * that are part of the given enabled dimensions list.
     * @param metricsLevel Level of metrics that is enabled. All metrics below this level will be dropped.
     * @param metricsEnabledDimensions Enabled dimensions.
     */
    public FilteringMetricsScope(MetricsLevel metricsLevel, Set<String> metricsEnabledDimensions) {
          this.metricsLevel = metricsLevel;
          this.metricsEnabledDimensions = metricsEnabledDimensions;
          this.metricsEnabledDimensionsAll = (metricsEnabledDimensions != null &&
                  metricsEnabledDimensions.contains(METRICS_DIMENSIONS_ALL));
    }

    /**
     * Adds the data to the metrics scope at lowest metrics level.
     * @param name Metrics data name.
     * @param value Value of the metrics.
     * @param unit Unit of the metrics.
     */
    @Override
    public void addData(String name, double value, StandardUnit unit) {
        addData(name, value, unit, MetricsLevel.DETAILED);
    }

    /**
     * Adds the data to the metrics scope if the given level is equal to above the enabled metrics
     * level.
     * @param name Metrics data name.
     * @param value Value of the metrics.
     * @param unit Unit of the metrics.
     * @param level Metrics level for the data.
     */
    @Override
    public void addData(String name, double value, StandardUnit unit, MetricsLevel level) {
        if (level.getValue() < metricsLevel.getValue()) {
            // Drop the data.
            return;
        }
        super.addData(name, value, unit);
    }

    /**
     * Adds given dimension with value if allowed dimensions list contains this dimension's name.
     * @param name Name of the dimension.
     * @param value Value for the dimension.
     */
    @Override
    public void addDimension(String name, String value) {
        if (!metricsEnabledDimensionsAll &&
                (metricsEnabledDimensions == null || !metricsEnabledDimensions.contains(name))) {
            // Drop dimension.
            return;
        }
        super.addDimension(name, value);
    }
}
