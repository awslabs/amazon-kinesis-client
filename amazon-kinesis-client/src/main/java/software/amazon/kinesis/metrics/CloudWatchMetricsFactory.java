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
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

/**
 * An IMetricsFactory that creates IMetricsScopes that output themselves via CloudWatch. Batches IMetricsScopes together
 * to reduce API calls.
 */
public class CloudWatchMetricsFactory implements MetricsFactory {

    /**
     * If the CloudWatchPublisherRunnable accumulates more than FLUSH_SIZE distinct metrics, it will call CloudWatch
     * immediately instead of waiting for the next scheduled call.
     */
    private final CloudWatchPublisherRunnable runnable;
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
     * @param cloudWatchClient
     *            Client used to make CloudWatch requests
     * @param namespace
     *            the namespace under which the metrics will appear in the CloudWatch console
     * @param bufferTimeMillis
     *            time to buffer metrics before publishing to CloudWatch
     * @param maxQueueSize
     *            maximum number of metrics that we can have in a queue
     * @param metricsLevel
     *            metrics level to enable
     * @param metricsEnabledDimensions
     *            metrics dimensions to allow
     * @param flushSize
     *            size of batch that can be published
     */
    public CloudWatchMetricsFactory(@NonNull final CloudWatchAsyncClient cloudWatchClient,
            @NonNull final String namespace, final long bufferTimeMillis, final int maxQueueSize,
            @NonNull final MetricsLevel metricsLevel, @NonNull final Set<String> metricsEnabledDimensions,
            final int flushSize) {
        this.metricsLevel = metricsLevel;
        this.metricsEnabledDimensions = (metricsEnabledDimensions == null ? ImmutableSet.of()
                : ImmutableSet.copyOf(metricsEnabledDimensions));

        runnable = new CloudWatchPublisherRunnable(new CloudWatchMetricsPublisher(cloudWatchClient, namespace),
                bufferTimeMillis, maxQueueSize, flushSize);
        publicationThread = new Thread(runnable);
        publicationThread.setName("cw-metrics-publisher");
        publicationThread.start();
    }

    @Override
    public MetricsScope createMetrics() {
        return new CloudWatchMetricsScope(runnable, metricsLevel, metricsEnabledDimensions);
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
