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

import io.opentelemetry.api.OpenTelemetry;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class OtelMetricsConfigTest {

    private static final String NAMESPACE = "TestApp";

    @Mock
    private CloudWatchAsyncClient cloudWatchClient;

    /**
     * Default backend is CLOUDWATCH, creates CloudWatchMetricsFactory.
     */
    @Test
    public void testDefaultBackendIsCloudWatch() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);

        MetricsFactory factory = config.metricsFactory();

        Assert.assertTrue(
                "Default backend should create CloudWatchMetricsFactory", factory instanceof CloudWatchMetricsFactory);
    }

    /**
     * CLOUDWATCH backend explicitly set still creates CloudWatchMetricsFactory.
     */
    @Test
    public void testCloudWatchBackendCreatesCloudWatchFactory() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.CLOUDWATCH);

        MetricsFactory factory = config.metricsFactory();

        Assert.assertTrue(
                "CLOUDWATCH backend should create CloudWatchMetricsFactory",
                factory instanceof CloudWatchMetricsFactory);
    }

    /**
     * OTEL backend creates OtelMetricsFactory without requiring endpoint or resource attributes.
     */
    @Test
    public void testOtelBackendCreatesOtelFactoryWithoutEndpointOrResourceAttributes() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.OTEL);
        // No otelEndpoint or otelResourceAttributes set

        MetricsFactory factory = config.metricsFactory();

        Assert.assertTrue(
                "OTEL backend should create OtelMetricsFactory", factory instanceof OtelMetricsFactory);
    }

    /**
     * OTEL backend uses provided OpenTelemetry instance.
     */
    @Test
    public void testOtelBackendUsesProvidedOpenTelemetryInstance() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.OTEL);
        config.openTelemetry(OpenTelemetry.noop());

        MetricsFactory factory = config.metricsFactory();

        Assert.assertTrue(
                "OTEL backend with provided OpenTelemetry instance should create OtelMetricsFactory",
                factory instanceof OtelMetricsFactory);
    }

    /**
     * OTEL backend uses GlobalOpenTelemetry.get() when no instance provided (just verify it doesn't throw).
     */
    @Test
    public void testOtelBackendUsesGlobalOpenTelemetryWhenNoInstanceProvided() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.OTEL);
        // openTelemetry is null — should fall back to GlobalOpenTelemetry.get()

        MetricsFactory factory = config.metricsFactory();

        Assert.assertNotNull("Factory should be created using GlobalOpenTelemetry fallback", factory);
        Assert.assertTrue(factory instanceof OtelMetricsFactory);
    }

    /**
     * CLOUDWATCH_OTEL backend now throws UnsupportedOperationException.
     */
    @SuppressWarnings("deprecation")
    @Test(expected = UnsupportedOperationException.class)
    public void testCloudWatchOtelBackendThrowsUnsupportedOperationException() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.CLOUDWATCH_OTEL);

        config.metricsFactory();
    }

    /**
     * Custom metricsFactory overrides backend selection.
     */
    @Test
    public void testCustomFactoryOverridesBackend() {
        MetricsFactory customFactory = mock(MetricsFactory.class);
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.OTEL);
        config.metricsFactory(customFactory);

        MetricsFactory factory = config.metricsFactory();

        Assert.assertSame("Custom factory should be used when explicitly set", customFactory, factory);
    }

    /**
     * Custom metricsFactory overrides default CLOUDWATCH backend too.
     */
    @Test
    public void testCustomFactoryOverridesDefaultBackend() {
        MetricsFactory customFactory = mock(MetricsFactory.class);
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsFactory(customFactory);

        MetricsFactory factory = config.metricsFactory();

        Assert.assertSame("Custom factory should override default CLOUDWATCH backend", customFactory, factory);
    }
}
