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
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.exception.AbortedException;

/**
 * A MetricsFactory that creates MetricsScopes which publish metrics via the CloudWatch OTEL endpoint.
 * Batches MetricsScopes together to reduce API calls. Mirrors {@link CloudWatchMetricsFactory} structurally.
 */
@Slf4j
public class OtelMetricsFactory implements MetricsFactory {

    private final OtelPublisherRunnable runnable;
    private final Thread publicationThread;

    /**
     * Enabled metrics level. All metrics below this level will be dropped.
     */
    private final MetricsLevel metricsLevel;

    /**
     * List of enabled dimensions for metrics.
     */
    private final Set<String> metricsEnabledDimensions;

    /**
     * Constructor.
     *
     * @param publisher          the OTEL metrics publisher
     * @param bufferTimeMillis   time to buffer metrics before publishing to the OTEL endpoint
     * @param maxQueueSize       maximum number of metrics that we can have in a queue
     * @param metricsLevel       metrics level to enable
     * @param metricsEnabledDimensions metrics dimensions to allow
     * @param flushSize          size of batch that can be published at a time
     */
    public OtelMetricsFactory(
            @NonNull final OtelMetricsPublisher publisher,
            final long bufferTimeMillis,
            final int maxQueueSize,
            @NonNull final MetricsLevel metricsLevel,
            @NonNull final Set<String> metricsEnabledDimensions,
            final int flushSize) {
        this.metricsLevel = metricsLevel;
        this.metricsEnabledDimensions =
                (metricsEnabledDimensions == null ? ImmutableSet.of() : ImmutableSet.copyOf(metricsEnabledDimensions));

        runnable = new OtelPublisherRunnable(publisher, bufferTimeMillis, maxQueueSize, flushSize);
        publicationThread = new Thread(runnable);
        publicationThread.setName("otel-metrics-publisher");
        publicationThread.start();
    }

    @Override
    public MetricsScope createMetrics() {
        return new OtelMetricsScope(runnable, metricsLevel, metricsEnabledDimensions);
    }

    public void shutdown() {
        runnable.shutdown();
        try {
            publicationThread.join();
        } catch (InterruptedException e) {
            throw AbortedException.builder().message(e.getMessage()).cause(e).build();
        }
    }
}
