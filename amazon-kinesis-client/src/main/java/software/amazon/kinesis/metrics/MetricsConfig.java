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
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
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
    public static final Set<String> METRICS_ALWAYS_ENABLED_DIMENSIONS =
            ImmutableSet.of(MetricsUtil.OPERATION_DIMENSION_NAME);

    /**
     * Allowed dimensions for CloudWatch metrics. By default, worker ID dimension will be disabled.
     */
    public static final Set<String> METRICS_ALWAYS_ENABLED_DIMENSIONS_WITH_SHARD_ID = ImmutableSet.<String>builder()
            .addAll(METRICS_ALWAYS_ENABLED_DIMENSIONS)
            .add(MetricsUtil.SHARD_ID_DIMENSION_NAME)
            .build();

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
     * Number of datums that accumulate in the in-memory queue before a flush is triggered (independent of the
     * time-based flush governed by {@link #metricsBufferTimeMillis}).
     *
     * <p>
     * The default was raised from 200 to 1000 to match the CloudWatch {@code PutMetricData} per-request datum
     * limit, so that a full flush maps to a single API call instead of several. This is a throughput/cost win at
     * high volume, but it is also a latency tradeoff: at low volume more datums may buffer before the count
     * trigger fires. Worst-case metric latency is unchanged — it remains bounded by {@link #metricsBufferTimeMillis}
     * (the time-based flush) — but the typical publish cadence is longer than with the old default of 200. Lower
     * this value if you need fresher metrics at low volume.
     *
     * <p>
     * Default value: 1000
     * </p>
     */
    private int publisherFlushBuffer = 1000;

    /**
     * The metrics publishing backend to use.
     *
     * <p>
     * Default value: {@link MetricsBackend#CLOUDWATCH}
     * </p>
     */
    private MetricsBackend metricsBackend = MetricsBackend.CLOUDWATCH;

    /**
     * Optional custom {@link OpenTelemetry} instance to use when the {@link MetricsBackend#OTEL} backend is selected.
     * If null, {@link GlobalOpenTelemetry#getOrNoop()} will be used as the default.
     *
     * <p>
     * Default value: null
     * </p>
     */
    private OpenTelemetry openTelemetry;

    private MetricsFactory metricsFactory;

    public MetricsFactory metricsFactory() {
        if (metricsFactory == null) {
            switch (metricsBackend) {
                case OTEL:
                    OpenTelemetry otel = openTelemetry != null ? openTelemetry : GlobalOpenTelemetry.getOrNoop();
                    metricsFactory = new OtelMetricsFactory(otel, metricsLevel(), metricsEnabledDimensions());
                    break;
                case CLOUDWATCH:
                default:
                    metricsFactory = new CloudWatchMetricsFactory(
                            cloudWatchClient(),
                            namespace(),
                            metricsBufferTimeMillis(),
                            metricsMaxQueueSize(),
                            metricsLevel(),
                            metricsEnabledDimensions(),
                            publisherFlushBuffer());
                    break;
            }
        }
        return metricsFactory;
    }
}
