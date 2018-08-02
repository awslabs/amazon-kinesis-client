/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.kinesis.metrics;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import lombok.Data;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

/**
 * Used by KCL to configure the metrics reported by the application.
 */
@Data
@Accessors(fluent = true)
public class MetricsConfig {
    /**
     * Metrics dimensions that always will be enabled regardless of the config provided by user.
     */
    public static final Set<String> METRICS_ALWAYS_ENABLED_DIMENSIONS = ImmutableSet
            .of(MetricsUtil.OPERATION_DIMENSION_NAME);

    /**
     * Allowed dimensions for CloudWatch metrics. By default, worker ID dimension will be disabled.
     */
    public static final Set<String> METRICS_ALWAYS_ENABLED_DIMENSIONS_WITH_SHARD_ID = ImmutableSet.<String> builder()
            .addAll(METRICS_ALWAYS_ENABLED_DIMENSIONS).add(MetricsUtil.SHARD_ID_DIMENSION_NAME).build();

    /**
     * Metrics dimensions that signify all possible dimensions.
     */
    public static final Set<String> METRICS_DIMENSIONS_ALL = ImmutableSet.of(MetricsScope.METRICS_DIMENSIONS_ALL);

    /**
     * Client used by the KCL to access the CloudWatch service for reporting metrics.
     *
     * @return {@link CloudWatchAsyncClient}
     */
    private final CloudWatchAsyncClient cloudWatchClient;

    /**
     * Namespace for KCL metrics.
     *
     * @return String
     */
    private final String namespace;

    /**
     * Buffer metrics for at most this long before publishing to CloudWatch.
     *
     * <p>
     * Default value: 10000L
     * </p>
     */
    private long metricsBufferTimeMillis = 10000L;

    /**
     * Buffer at most this many metrics before publishing to CloudWatch.
     *
     * <p>
     * Default value: 10000
     * </p>
     */
    private int metricsMaxQueueSize = 10000;

    /**
     * Metrics level for which to enable CloudWatch metrics.
     *
     * <p>
     * Default value: {@link MetricsLevel#DETAILED}
     * </p>
     */
    private MetricsLevel metricsLevel = MetricsLevel.DETAILED;

    /**
     * Allowed dimensions for CloudWatchMetrics.
     *
     * <p>
     * Default value: {@link MetricsConfig#METRICS_DIMENSIONS_ALL}
     * </p>
     */
    private Set<String> metricsEnabledDimensions = METRICS_DIMENSIONS_ALL;

    /**
     * Buffer size for MetricDatums before publishing.
     *
     * <p>
     * Default value: 200
     * </p>
     */
    private int publisherFlushBuffer = 200;

    private MetricsFactory metricsFactory;

    public MetricsFactory metricsFactory() {
        if (metricsFactory == null) {
            metricsFactory = new CloudWatchMetricsFactory(cloudWatchClient(), namespace(), metricsBufferTimeMillis(),
                    metricsMaxQueueSize(), metricsLevel(), metricsEnabledDimensions(), publisherFlushBuffer());
        }
        return metricsFactory;
    }
}
