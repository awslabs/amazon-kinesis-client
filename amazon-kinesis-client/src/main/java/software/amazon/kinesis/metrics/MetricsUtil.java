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

import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.common.StreamIdentifier;

/**
 *
 */
public class MetricsUtil {
    public static final String OPERATION_DIMENSION_NAME = "Operation";
    public static final String SHARD_ID_DIMENSION_NAME = "ShardId";
    public static final String STREAM_IDENTIFIER = "StreamId";
    private static final String WORKER_IDENTIFIER_DIMENSION = "WorkerIdentifier";
    private static final String TIME_METRIC = "Time";
    private static final String SUCCESS_METRIC = "Success";

    public static MetricsScope createMetrics(@NonNull final MetricsFactory metricsFactory) {
        return createMetricScope(metricsFactory, null);
    }

    public static MetricsScope createMetricsWithOperation(@NonNull final MetricsFactory metricsFactory,
                                                          @NonNull final String operation) {
        return createMetricScope(metricsFactory, operation);
    }

    private static MetricsScope createMetricScope(final MetricsFactory metricsFactory, final String operation) {
        final MetricsScope metricsScope = metricsFactory.createMetrics();
        if (StringUtils.isNotEmpty(operation)) {
            metricsScope.addDimension(OPERATION_DIMENSION_NAME, operation);
        }
        return metricsScope;
    }

    public static void addShardId(@NonNull final MetricsScope metricsScope, @NonNull final String shardId) {
        addOperation(metricsScope, SHARD_ID_DIMENSION_NAME, shardId);
    }

    public static void addStreamId(@NonNull final MetricsScope metricsScope, @NonNull final StreamIdentifier streamId) {
        streamId.accountIdOptional()
                .ifPresent(acc -> addOperation(metricsScope, STREAM_IDENTIFIER, streamId.serialize()));
    }

    public static void addWorkerIdentifier(@NonNull final MetricsScope metricsScope,
            @NonNull final String workerIdentifier) {
        addOperation(metricsScope, WORKER_IDENTIFIER_DIMENSION, workerIdentifier);
    }

    public static void addOperation(@NonNull final MetricsScope metricsScope, @NonNull final String dimension,
                                    @NonNull final String value) {
        metricsScope.addDimension(dimension, value);
    }

    public static void addSuccessAndLatency(@NonNull final MetricsScope metricsScope, final boolean success,
                                            final long startTime, @NonNull final MetricsLevel metricsLevel) {
        addSuccessAndLatency(metricsScope, null, success, startTime, metricsLevel);
    }

    public static void addSuccessAndLatency(@NonNull final MetricsScope metricsScope, final String dimension,
                                            final boolean success, final long startTime, @NonNull final MetricsLevel metricsLevel) {
        addSuccess(metricsScope, dimension, success, metricsLevel);
        addLatency(metricsScope, dimension, startTime, metricsLevel);
    }

    public static void addLatency(@NonNull final MetricsScope metricsScope, final String dimension,
                                  final long startTime, @NonNull final MetricsLevel metricsLevel) {
        final String metricName = StringUtils.isEmpty(dimension) ? TIME_METRIC
                : String.format("%s.%s", dimension, TIME_METRIC);
        metricsScope.addData(metricName, System.currentTimeMillis() - startTime, StandardUnit.MILLISECONDS,
                metricsLevel);
    }

    public static void addSuccess(@NonNull final MetricsScope metricsScope, final String dimension,
                                  final boolean success, @NonNull final MetricsLevel metricsLevel) {
        final String metricName = StringUtils.isEmpty(dimension) ? SUCCESS_METRIC
                : String.format("%s.%s", dimension, SUCCESS_METRIC);
        metricsScope.addData(metricName, success ? 1 : 0, StandardUnit.COUNT, metricsLevel);
    }

    public static void addCount(@NonNull final MetricsScope metricsScope, final String dimension,
            final long count, @NonNull final MetricsLevel metricsLevel) {
        metricsScope.addData(dimension, count, StandardUnit.COUNT, metricsLevel);
    }

    public static void endScope(@NonNull final MetricsScope metricsScope) {
        metricsScope.end();
    }
}
