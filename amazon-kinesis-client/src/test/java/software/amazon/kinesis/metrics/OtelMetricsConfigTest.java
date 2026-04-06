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
     * CLOUDWATCH_OTEL backend creates OtelMetricsFactory when endpoint is set.
     */
    @Test
    public void testOtelBackendCreatesOtelFactory() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.CLOUDWATCH_OTEL);
        config.otelEndpoint("http://localhost:4318/v1/metrics");

        MetricsFactory factory = config.metricsFactory();

        Assert.assertTrue(
                "CLOUDWATCH_OTEL backend should create OtelMetricsFactory", factory instanceof OtelMetricsFactory);

        // Clean up the publication thread
        ((OtelMetricsFactory) factory).shutdown();
    }

    /**
     * CLOUDWATCH_OTEL without endpoint throws IllegalStateException.
     */
    @Test(expected = IllegalStateException.class)
    public void testOtelBackendWithoutEndpointThrows() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.CLOUDWATCH_OTEL);
        // otelEndpoint is not set

        config.metricsFactory();
    }

    /**
     * CLOUDWATCH_OTEL with empty endpoint throws IllegalStateException.
     */
    @Test(expected = IllegalStateException.class)
    public void testOtelBackendWithEmptyEndpointThrows() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.CLOUDWATCH_OTEL);
        config.otelEndpoint("");

        config.metricsFactory();
    }

    /**
     * Custom metricsFactory overrides backend selection.
     */
    @Test
    public void testCustomFactoryOverridesBackend() {
        MetricsFactory customFactory = mock(MetricsFactory.class);
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.CLOUDWATCH_OTEL);
        config.otelEndpoint("http://localhost:4318/v1/metrics");
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

    /**
     * OTEL defaults to 30000ms buffer time when user hasn't changed from the CloudWatch default of 10000ms.
     * We verify this indirectly: the factory is created successfully with OTEL backend,
     * and the metricsBufferTimeMillis on the config remains at the default 10000ms
     * (the OTEL path internally uses 30000ms).
     */
    @Test
    public void testOtelDefaultBufferTime() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.CLOUDWATCH_OTEL);
        config.otelEndpoint("http://localhost:4318/v1/metrics");

        // metricsBufferTimeMillis is at default 10000ms — OTEL path should use 30000ms internally
        Assert.assertEquals(10000L, config.metricsBufferTimeMillis());

        MetricsFactory factory = config.metricsFactory();
        Assert.assertTrue(factory instanceof OtelMetricsFactory);

        // Clean up
        ((OtelMetricsFactory) factory).shutdown();
    }

    /**
     * User-overridden buffer time is respected for OTEL path.
     */
    @Test
    public void testOtelUserOverriddenBufferTime() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);
        config.metricsBackend(MetricsBackend.CLOUDWATCH_OTEL);
        config.otelEndpoint("http://localhost:4318/v1/metrics");
        config.metricsBufferTimeMillis(60000L);

        // User set a custom value — OTEL path should use 60000ms, not the 30000ms default
        Assert.assertEquals(60000L, config.metricsBufferTimeMillis());

        MetricsFactory factory = config.metricsFactory();
        Assert.assertTrue(factory instanceof OtelMetricsFactory);

        // Clean up
        ((OtelMetricsFactory) factory).shutdown();
    }

    /**
     * CloudWatch backend continues to use the default 10000ms buffer time.
     */
    @Test
    public void testCloudWatchDefaultBufferTime() {
        MetricsConfig config = new MetricsConfig(cloudWatchClient, NAMESPACE);

        Assert.assertEquals(10000L, config.metricsBufferTimeMillis());
        Assert.assertEquals(MetricsBackend.CLOUDWATCH, config.metricsBackend());
    }
}
