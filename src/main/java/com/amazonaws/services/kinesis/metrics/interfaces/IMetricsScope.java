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
package com.amazonaws.services.kinesis.metrics.interfaces;

import com.amazonaws.services.cloudwatch.model.StandardUnit;

/**
 * An IMetricsScope represents a set of metric data that share a set of dimensions. IMetricsScopes know how to output
 * themselves (perhaps to disk, perhaps over service calls, etc).
 */
public interface IMetricsScope {

    /**
     * Value that signifies that all dimensions are allowed for the metrics scope.
     */
    public static final String METRICS_DIMENSIONS_ALL = "ALL";

    /**
     * Adds a data point to this IMetricsScope. Multiple calls against the same IMetricsScope with the same name
     * parameter will result in accumulation.
     * 
     * @param name data point name
     * @param value data point value
     * @param unit unit of data point
     */
    public void addData(String name, double value, StandardUnit unit);

    /**
     * Adds a data point to this IMetricsScope if given metrics level is enabled. Multiple calls against the same
     * IMetricsScope with the same name parameter will result in accumulation.
     * 
     * @param name data point name
     * @param value data point value
     * @param unit unit of data point
     * @param level metrics level of this data point
     */
    public void addData(String name, double value, StandardUnit unit, MetricsLevel level);

    /**
     * Adds a dimension that applies to all metrics in this IMetricsScope.
     * 
     * @param name dimension name
     * @param value dimension value
     */
    public void addDimension(String name, String value);

    /**
     * Flushes the data from this IMetricsScope and causes future calls to addData and addDimension to fail.
     */
    public void end();
}
