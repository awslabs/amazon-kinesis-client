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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

/**
 * Publisher that converts KCL MetricDatum objects into OTLP format and sends them
 * to the CloudWatch OTEL endpoint via the OTLP HTTP exporter.
 */
@Slf4j
public class OtelMetricsPublisher {

    private static final int BATCH_SIZE = 20;
    private static final long EXPORT_TIMEOUT_MILLIS = 5000;

    private static final Map<StandardUnit, String> UNIT_MAP;

    static {
        Map<StandardUnit, String> map = new HashMap<>();
        map.put(StandardUnit.SECONDS, "s");
        map.put(StandardUnit.MICROSECONDS, "us");
        map.put(StandardUnit.MILLISECONDS, "ms");
        map.put(StandardUnit.BYTES, "By");
        map.put(StandardUnit.KILOBYTES, "kBy");
        map.put(StandardUnit.MEGABYTES, "MBy");
        map.put(StandardUnit.GIGABYTES, "GBy");
        map.put(StandardUnit.TERABYTES, "TBy");
        map.put(StandardUnit.BITS, "bit");
        map.put(StandardUnit.KILOBITS, "kbit");
        map.put(StandardUnit.MEGABITS, "Mbit");
        map.put(StandardUnit.GIGABITS, "Gbit");
        map.put(StandardUnit.TERABITS, "Tbit");
        map.put(StandardUnit.PERCENT, "%");
        map.put(StandardUnit.COUNT, "1");
        map.put(StandardUnit.BYTES_SECOND, "By/s");
        map.put(StandardUnit.KILOBYTES_SECOND, "kBy/s");
        map.put(StandardUnit.MEGABYTES_SECOND, "MBy/s");
        map.put(StandardUnit.GIGABYTES_SECOND, "GBy/s");
        map.put(StandardUnit.TERABYTES_SECOND, "TBy/s");
        map.put(StandardUnit.BITS_SECOND, "bit/s");
        map.put(StandardUnit.KILOBITS_SECOND, "kbit/s");
        map.put(StandardUnit.MEGABITS_SECOND, "Mbit/s");
        map.put(StandardUnit.GIGABITS_SECOND, "Gbit/s");
        map.put(StandardUnit.TERABITS_SECOND, "Tbit/s");
        map.put(StandardUnit.COUNT_SECOND, "1/s");
        map.put(StandardUnit.NONE, "1");
        UNIT_MAP = Collections.unmodifiableMap(map);
    }

    /**
     * Mapping of KCL dimension names to OTEL semantic convention attribute names.
     */
    static final Map<String, String> DIMENSION_TO_ATTRIBUTE_MAP;

    static {
        Map<String, String> dimMap = new HashMap<>();
        dimMap.put(MetricsUtil.OPERATION_DIMENSION_NAME, "aws.kinesis.operation");
        dimMap.put(MetricsUtil.SHARD_ID_DIMENSION_NAME, "aws.kinesis.shard.id");
        dimMap.put(MetricsUtil.STREAM_IDENTIFIER, "messaging.destination.name");
        dimMap.put("WorkerIdentifier", "aws.kinesis.consumer.name");
        DIMENSION_TO_ATTRIBUTE_MAP = Collections.unmodifiableMap(dimMap);
    }

    private final String namespace;
    private final MetricExporter exporter;
    private final Resource resource;

    public OtelMetricsPublisher(
            String namespace,
            String otelEndpoint,
            Map<String, String> autoDetectedResourceAttributes,
            Map<String, String> customResourceAttributes) {
        this.namespace = namespace;
        this.exporter =
                OtlpHttpMetricExporter.builder().setEndpoint(otelEndpoint).build();
        this.resource = buildResource(autoDetectedResourceAttributes, customResourceAttributes);
    }

    /**
     * Package-private constructor for testing, allowing injection of a mock exporter.
     */
    OtelMetricsPublisher(String namespace, MetricExporter exporter, Resource resource) {
        this.namespace = namespace;
        this.exporter = exporter;
        this.resource = resource;
    }

    /**
     * Given a list of MetricDatumWithKey, this method converts each MetricDatum to OTLP format
     * and publishes them via the OTLP exporter.
     *
     * @param dataToPublish a list containing all the MetricDatums to publish
     */
    public void publishMetrics(List<MetricDatumWithKey<OtelMetricKey>> dataToPublish) {
        InstrumentationScopeInfo scopeInfo = InstrumentationScopeInfo.create(namespace);
        long nowEpochNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());

        for (int startIndex = 0; startIndex < dataToPublish.size(); startIndex += BATCH_SIZE) {
            int endIndex = Math.min(dataToPublish.size(), startIndex + BATCH_SIZE);

            List<MetricData> metricDataList = new ArrayList<>();
            for (int i = startIndex; i < endIndex; i++) {
                MetricDatum datum = dataToPublish.get(i).datum;
                MetricData metricData = convertToOtlpMetricData(datum, scopeInfo, nowEpochNanos);
                metricDataList.add(metricData);
            }

            try {
                CompletableResultCode result = exporter.export(metricDataList);
                result.join(EXPORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (!result.isSuccess()) {
                    log.warn("Could not publish {} datums to CloudWatch OTEL endpoint", endIndex - startIndex);
                }
            } catch (Exception e) {
                log.warn("Could not publish {} datums to CloudWatch OTEL endpoint", endIndex - startIndex, e);
            }
        }
    }

    /**
     * Flushes any buffered metrics and shuts down the OTLP exporter.
     */
    public void shutdown() {
        try {
            CompletableResultCode flushResult = exporter.flush();
            flushResult.join(EXPORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("Error flushing OTEL metrics exporter during shutdown", e);
        }
        try {
            CompletableResultCode shutdownResult = exporter.shutdown();
            shutdownResult.join(EXPORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("Error shutting down OTEL metrics exporter", e);
        }
    }

    /**
     * Converts a CloudWatch MetricDatum to an OTLP MetricData using the Histogram data model.
     * The Histogram type naturally supports sum, count, min, and max which maps directly
     * to CloudWatch's StatisticSet.
     */
    MetricData convertToOtlpMetricData(MetricDatum datum, InstrumentationScopeInfo scopeInfo, long nowEpochNanos) {
        StatisticSet stats = datum.statisticValues();
        Attributes attributes = convertDimensionsToAttributes(datum.dimensions());
        String unit = convertUnit(datum.unit());

        io.opentelemetry.sdk.metrics.data.HistogramPointData pointData = ImmutableHistogramPointData.create(
                nowEpochNanos, // startEpochNanos
                nowEpochNanos, // epochNanos
                attributes,
                stats.sum(),
                stats.minimum() != null, // hasMin
                stats.minimum() != null ? stats.minimum() : 0.0,
                stats.maximum() != null, // hasMax
                stats.maximum() != null ? stats.maximum() : 0.0,
                Collections.emptyList(), // boundaries
                Collections.singletonList(stats.sampleCount().longValue())); // counts

        io.opentelemetry.sdk.metrics.data.HistogramData histogramData =
                ImmutableHistogramData.create(AggregationTemporality.DELTA, Collections.singletonList(pointData));

        return ImmutableMetricData.createDoubleHistogram(
                resource, scopeInfo, datum.metricName(), "", unit, histogramData);
    }

    /**
     * Converts CloudWatch Dimensions to OTEL Attributes, mapping known KCL dimension names
     * to their OTEL semantic convention equivalents.
     */
    Attributes convertDimensionsToAttributes(List<Dimension> dimensions) {
        if (dimensions == null || dimensions.isEmpty()) {
            return Attributes.empty();
        }
        AttributesBuilder builder = Attributes.builder();
        for (Dimension dimension : dimensions) {
            String attributeName = DIMENSION_TO_ATTRIBUTE_MAP.getOrDefault(dimension.name(), dimension.name());
            builder.put(AttributeKey.stringKey(attributeName), dimension.value());
        }
        return builder.build();
    }

    /**
     * Converts a CloudWatch StandardUnit to an OTEL unit string.
     */
    static String convertUnit(StandardUnit unit) {
        if (unit == null) {
            return "1";
        }
        return UNIT_MAP.getOrDefault(unit, "1");
    }

    /**
     * Builds an OTEL Resource from merged auto-detected and custom resource attributes.
     * Custom attributes override auto-detected ones with the same key.
     */
    static Resource buildResource(Map<String, String> autoDetectedAttributes, Map<String, String> customAttributes) {
        Map<String, String> merged = new HashMap<>();
        if (autoDetectedAttributes != null) {
            merged.putAll(autoDetectedAttributes);
        }
        if (customAttributes != null) {
            merged.putAll(customAttributes);
        }

        AttributesBuilder builder = Attributes.builder();
        for (Map.Entry<String, String> entry : merged.entrySet()) {
            builder.put(AttributeKey.stringKey(entry.getKey()), entry.getValue());
        }
        return Resource.create(builder.build());
    }

    /**
     * Returns the resource for testing purposes.
     */
    Resource getResource() {
        return resource;
    }

    /**
     * Returns the namespace for testing purposes.
     */
    String getNamespace() {
        return namespace;
    }
}
