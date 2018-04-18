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

package software.amazon.kinesis.metrics;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.google.common.collect.ImmutableSet;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.Set;

/**
 * Used by KCL to configure the metrics reported by the application.
 */
@Data
@Accessors(fluent = true)
public class MetricsConfig {
    /**
     * Metrics dimensions that always will be enabled regardless of the config provided by user.
     */
    public static final Set<String> METRICS_ALWAYS_ENABLED_DIMENSIONS = ImmutableSet.of(
            MetricsHelper.OPERATION_DIMENSION_NAME);

    /**
     * Allowed dimensions for CloudWatch metrics. By default, worker ID dimension will be disabled.
     */
    public static final Set<String> DEFAULT_METRICS_ENABLED_DIMENSIONS = ImmutableSet.<String>builder().addAll(
            METRICS_ALWAYS_ENABLED_DIMENSIONS).add(MetricsHelper.SHARD_ID_DIMENSION_NAME).build();

    /**
     * Metrics dimensions that signify all possible dimensions.
     */
    public static final Set<String> METRICS_DIMENSIONS_ALL = ImmutableSet.of(IMetricsScope.METRICS_DIMENSIONS_ALL);

    /**
     * Client used by the KCL to access the CloudWatch service for reporting metrics.
     *
     * @return {@link AmazonCloudWatch}
     */
    private final AmazonCloudWatch amazonCloudWatch;

    /**
     * Buffer metrics for at most this long before publishing to CloudWatch.
     *
     * <p>Default value: 10000L</p>
     */
    private long metricsBufferTimeMillis = 10000L;

    /**
     * Buffer at most this many metrics before publishing to CloudWatch.
     *
     * <p>Default value: 10000</p>
     */
    private int metricsMaxQueueSize = 10000;

    /**
     * Metrics level for which to enable CloudWatch metrics.
     *
     * <p>Default value: {@link MetricsLevel#DETAILED}</p>
     */
    private MetricsLevel metricsLevel = MetricsLevel.DETAILED;

    /**
     * Allowed dimensions for CloudWatchMetrics.
     *
     * <p>Default value: {@link MetricsConfig#DEFAULT_METRICS_ENABLED_DIMENSIONS}</p>
     */
    private Set<String> metricsEnabledDimensions = DEFAULT_METRICS_ENABLED_DIMENSIONS;

    private MetricsFactory metricsFactory;

    private IMetricsFactory iMetricsFactory;
}
