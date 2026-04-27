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
import java.util.Set;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

/**
 * OTel-native metrics scope that implements {@link MetricsScope} directly.
 * Records raw observations on OTel {@link Meter} instruments ({@link DoubleHistogram},
 * {@link LongCounter}) when {@link #end()} is called. The OTel SDK (configured by the
 * application owner) handles aggregation, batching, and export.
 */
public class OtelMetricsScope implements MetricsScope {

    /**
     * Mapping of KCL dimension names to OTel semantic convention attribute keys.
     */
    static final Map<String, String> DIMENSION_TO_ATTRIBUTE_MAP;

    static {
        Map<String, String> dimMap = new HashMap<>();
        dimMap.put(MetricsUtil.OPERATION_DIMENSION_NAME, "aws.kinesis.operation");
        dimMap.put(MetricsUtil.SHARD_ID_DIMENSION_NAME, "aws.kinesis.shard.id");
        dimMap.put(MetricsUtil.STREAM_IDENTIFIER, "aws.kinesis.stream_name");
        dimMap.put("WorkerIdentifier", "aws.kinesis.consumer.name");
        DIMENSION_TO_ATTRIBUTE_MAP = Collections.unmodifiableMap(dimMap);
    }

    /**
     * Mapping of CloudWatch StandardUnit to OTel UCUM unit strings.
     */
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

    private final Meter meter;
    private final MetricsLevel metricsLevel;
    private final Set<String> metricsEnabledDimensions;
    private final boolean allDimensionsEnabled;

    private final List<MetricObservation> observations = new ArrayList<>();
    private final AttributesBuilder attributesBuilder = Attributes.builder();
    private boolean ended = false;

    /**
     * Creates an OTel-native metrics scope.
     *
     * @param meter the OTel Meter to record observations on
     * @param metricsLevel the minimum metrics level threshold; data points below this are dropped
     * @param metricsEnabledDimensions the set of dimension names to include; use {@link MetricsScope#METRICS_DIMENSIONS_ALL} to include all
     */
    public OtelMetricsScope(Meter meter, MetricsLevel metricsLevel, Set<String> metricsEnabledDimensions) {
        this.meter = meter;
        this.metricsLevel = metricsLevel;
        this.metricsEnabledDimensions = metricsEnabledDimensions;
        this.allDimensionsEnabled = metricsEnabledDimensions.contains(METRICS_DIMENSIONS_ALL);
    }

    @Override
    public void addData(String name, double value, StandardUnit unit) {
        addData(name, value, unit, MetricsLevel.DETAILED);
    }

    @Override
    public void addData(String name, double value, StandardUnit unit, MetricsLevel level) {
        checkNotEnded();
        if (level.getValue() < metricsLevel.getValue()) {
            return;
        }
        observations.add(new MetricObservation(name, value, unit));
    }

    @Override
    public void addDimension(String name, String value) {
        checkNotEnded();
        if (!allDimensionsEnabled && !metricsEnabledDimensions.contains(name)) {
            return;
        }
        String attrKey = DIMENSION_TO_ATTRIBUTE_MAP.getOrDefault(name, name);
        attributesBuilder.put(AttributeKey.stringKey(attrKey), value);
    }

    @Override
    public void end() {
        checkNotEnded();
        ended = true;
        Attributes attrs = attributesBuilder.build();
        for (MetricObservation obs : observations) {
            recordObservation(obs, attrs);
        }
    }

    private void recordObservation(MetricObservation obs, Attributes attrs) {
        if (obs.unit == StandardUnit.COUNT) {
            LongCounter counter = meter.counterBuilder(obs.name)
                    .setUnit(convertUnit(obs.unit))
                    .build();
            counter.add((long) obs.value, attrs);
        } else {
            DoubleHistogram histogram = meter.histogramBuilder(obs.name)
                    .setUnit(convertUnit(obs.unit))
                    .build();
            histogram.record(obs.value, attrs);
        }
    }

    /**
     * Converts a CloudWatch StandardUnit to an OTel UCUM unit string.
     *
     * @param unit the StandardUnit to convert
     * @return the UCUM unit string, or "1" if null or unknown
     */
    static String convertUnit(StandardUnit unit) {
        if (unit == null) {
            return "1";
        }
        return UNIT_MAP.getOrDefault(unit, "1");
    }

    private void checkNotEnded() {
        if (ended) {
            throw new IllegalArgumentException("MetricsScope has already been ended");
        }
    }
}
