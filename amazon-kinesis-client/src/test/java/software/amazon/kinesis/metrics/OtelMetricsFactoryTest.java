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

import java.util.Collections;
import java.util.Set;

import io.opentelemetry.api.OpenTelemetry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link OtelMetricsFactory}.
 * Uses {@link OpenTelemetry#noop()} since the OTel SDK is not available at test scope.
 */
@RunWith(MockitoJUnitRunner.class)
public class OtelMetricsFactoryTest {

    private static final Set<String> ALL_DIMENSIONS =
            Collections.singleton(MetricsScope.METRICS_DIMENSIONS_ALL);

    // -----------------------------------------------------------------------
    // createMetrics returns OtelMetricsScope
    // -----------------------------------------------------------------------

    @Test
    public void testCreateMetrics_returnsOtelMetricsScope() {
        OtelMetricsFactory factory =
                new OtelMetricsFactory(OpenTelemetry.noop(), MetricsLevel.DETAILED, ALL_DIMENSIONS);

        MetricsScope scope = factory.createMetrics();

        assertNotNull("createMetrics should return a non-null scope", scope);
        assertTrue(
                "createMetrics should return an OtelMetricsScope instance",
                scope instanceof OtelMetricsScope);
    }

    @Test
    public void testCreateMetrics_multipleCalls_returnDistinctScopes() {
        OtelMetricsFactory factory =
                new OtelMetricsFactory(OpenTelemetry.noop(), MetricsLevel.DETAILED, ALL_DIMENSIONS);

        MetricsScope scope1 = factory.createMetrics();
        MetricsScope scope2 = factory.createMetrics();

        assertNotNull(scope1);
        assertNotNull(scope2);
        assertTrue("Each call should return a new scope instance", scope1 != scope2);
    }

    // -----------------------------------------------------------------------
    // shutdown is no-op — completes without error
    // -----------------------------------------------------------------------

    @Test
    public void testShutdown_completesWithoutError() {
        OtelMetricsFactory factory =
                new OtelMetricsFactory(OpenTelemetry.noop(), MetricsLevel.DETAILED, ALL_DIMENSIONS);

        // Should not throw
        factory.shutdown();
    }

    @Test
    public void testShutdown_canBeCalledMultipleTimes() {
        OtelMetricsFactory factory =
                new OtelMetricsFactory(OpenTelemetry.noop(), MetricsLevel.DETAILED, ALL_DIMENSIONS);

        factory.shutdown();
        factory.shutdown();
        // No exception means success
    }

    // -----------------------------------------------------------------------
    // Noop OpenTelemetry — factory works with noop instance
    // -----------------------------------------------------------------------

    @Test
    public void testNoopOpenTelemetry_createMetricsWorks() {
        OtelMetricsFactory factory =
                new OtelMetricsFactory(OpenTelemetry.noop(), MetricsLevel.DETAILED, ALL_DIMENSIONS);

        MetricsScope scope = factory.createMetrics();
        assertNotNull(scope);

        // Exercise the scope to verify it works end-to-end with noop
        scope.addDimension("Operation", "GetRecords");
        scope.addData("RecordsProcessed", 10.0,
                software.amazon.awssdk.services.cloudwatch.model.StandardUnit.COUNT);
        scope.end();
    }

    @Test
    public void testNoopOpenTelemetry_withSummaryLevel() {
        OtelMetricsFactory factory =
                new OtelMetricsFactory(OpenTelemetry.noop(), MetricsLevel.SUMMARY, ALL_DIMENSIONS);

        MetricsScope scope = factory.createMetrics();
        assertNotNull(scope);
        assertTrue(scope instanceof OtelMetricsScope);
    }

    @Test
    public void testNoopOpenTelemetry_withEmptyDimensions() {
        Set<String> emptyDimensions = Collections.emptySet();
        OtelMetricsFactory factory =
                new OtelMetricsFactory(OpenTelemetry.noop(), MetricsLevel.DETAILED, emptyDimensions);

        MetricsScope scope = factory.createMetrics();
        assertNotNull(scope);
        assertTrue(scope instanceof OtelMetricsScope);
    }

    @Test
    public void testNoopOpenTelemetry_withNoneLevel() {
        OtelMetricsFactory factory =
                new OtelMetricsFactory(OpenTelemetry.noop(), MetricsLevel.NONE, ALL_DIMENSIONS);

        MetricsScope scope = factory.createMetrics();
        assertNotNull(scope);

        // With NONE level, all data should be dropped but no exceptions
        scope.addData("SomeMetric", 1.0,
                software.amazon.awssdk.services.cloudwatch.model.StandardUnit.COUNT,
                MetricsLevel.SUMMARY);
        scope.end();
    }

    // -----------------------------------------------------------------------
    // Constructor validation — null arguments
    // -----------------------------------------------------------------------

    @Test(expected = NullPointerException.class)
    public void testConstructor_nullOpenTelemetry_throws() {
        new OtelMetricsFactory(null, MetricsLevel.DETAILED, ALL_DIMENSIONS);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_nullMetricsLevel_throws() {
        new OtelMetricsFactory(OpenTelemetry.noop(), null, ALL_DIMENSIONS);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_nullDimensions_throws() {
        new OtelMetricsFactory(OpenTelemetry.noop(), MetricsLevel.DETAILED, null);
    }
}
