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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import lombok.NonNull;

/**
 * A {@link MetricsFactory} that creates {@link OtelMetricsScope} instances which record raw
 * observations on OTel {@link Meter} instruments. Follows OTel library instrumentation best
 * practices by depending only on the {@code opentelemetry-api} and letting the application
 * owner configure the SDK (exporters, resource attributes, batching).
 *
 * <p>This factory does not manage any background threads, custom batching, or export logic.
 * The OTel SDK (configured by the application owner) handles aggregation, batching, and export.
 */
public class OtelMetricsFactory implements MetricsFactory {

    private final Meter meter;
    private final MetricsLevel metricsLevel;
    private final Set<String> metricsEnabledDimensions;

    /**
     * Constructor.
     *
     * @param openTelemetry           the OpenTelemetry instance to obtain a Meter from
     * @param metricsLevel            metrics level threshold; observations below this level are dropped
     * @param metricsEnabledDimensions set of dimension names to include in recorded observations
     */
    public OtelMetricsFactory(
            @NonNull final OpenTelemetry openTelemetry,
            @NonNull final MetricsLevel metricsLevel,
            @NonNull final Set<String> metricsEnabledDimensions) {
        this.meter = openTelemetry.getMeter("software.amazon.kinesis");
        this.metricsLevel = metricsLevel;
        this.metricsEnabledDimensions = ImmutableSet.copyOf(metricsEnabledDimensions);
    }

    @Override
    public MetricsScope createMetrics() {
        return new OtelMetricsScope(meter, metricsLevel, metricsEnabledDimensions);
    }

    /**
     * No-op shutdown. The application owner is responsible for managing the OTel
     * {@code MeterProvider} lifecycle (including flushing and shutting down exporters).
     */
    public void shutdown() {
        // No-op: application owner manages MeterProvider lifecycle
    }
}
