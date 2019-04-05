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

import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

/**
 * An IMetricsScope represents a set of metric data that share a set of dimensions. IMetricsScopes know how to output
 * themselves (perhaps to disk, perhaps over service calls, etc).
 */
public interface MetricsScope {

    /**
     * Value that signifies that all dimensions are allowed for the metrics scope.
     */
    String METRICS_DIMENSIONS_ALL = "ALL";

    /**
     * Adds a data point to this IMetricsScope. Multiple calls against the same IMetricsScope with the same name
     * parameter will result in accumulation.
     * 
     * @param name data point name
     * @param value data point value
     * @param unit unit of data point
     */
    void addData(String name, double value, StandardUnit unit);

    /**
     * Adds a data point to this IMetricsScope if given metrics level is enabled. Multiple calls against the same
     * IMetricsScope with the same name parameter will result in accumulation.
     * 
     * @param name data point name
     * @param value data point value
     * @param unit unit of data point
     * @param level metrics level of this data point
     */
    void addData(String name, double value, StandardUnit unit, MetricsLevel level);

    /**
     * Adds a dimension that applies to all metrics in this IMetricsScope.
     * 
     * @param name dimension name
     * @param value dimension value
     */
    void addDimension(String name, String value);

    /**
     * Flushes the data from this IMetricsScope and causes future calls to addData and addDimension to fail.
     */
    void end();
}
