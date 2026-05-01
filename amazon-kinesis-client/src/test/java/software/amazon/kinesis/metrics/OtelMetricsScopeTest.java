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
import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleCounterBuilder;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link OtelMetricsScope}.
 * Uses Mockito mocks for verifying instrument type selection and attribute mapping,
 * and {@link OpenTelemetry#noop()} for integration-style lifecycle tests.
 */
@RunWith(MockitoJUnitRunner.class)
public class OtelMetricsScopeTest {

    @Mock
    private Meter mockMeter;

    @Mock
    private DoubleGaugeBuilder mockGaugeBuilder;

    @Mock
    private DoubleGauge mockGauge;

    @Mock
    private LongCounterBuilder mockLongCounterBuilder;

    @Mock
    private DoubleCounterBuilder mockDoubleCounterBuilder;

    @Mock
    private DoubleCounter mockCounter;

    @Mock
    private DoubleHistogramBuilder mockHistogramBuilder;

    @Mock
    private DoubleHistogram mockHistogram;

    private Set<String> allDimensions;

    @Before
    public void setUp() {
        allDimensions = Collections.singleton(MetricsScope.METRICS_DIMENSIONS_ALL);

        // Wire up gauge builder chain
        when(mockMeter.gaugeBuilder(anyString())).thenReturn(mockGaugeBuilder);
        when(mockGaugeBuilder.setUnit(anyString())).thenReturn(mockGaugeBuilder);
        when(mockGaugeBuilder.build()).thenReturn(mockGauge);

        // Wire up counter builder chain
        when(mockMeter.counterBuilder(anyString())).thenReturn(mockLongCounterBuilder);
        when(mockLongCounterBuilder.ofDoubles()).thenReturn(mockDoubleCounterBuilder);
        when(mockDoubleCounterBuilder.setUnit(anyString())).thenReturn(mockDoubleCounterBuilder);
        when(mockDoubleCounterBuilder.build()).thenReturn(mockCounter);

        // Wire up histogram builder chain
        when(mockMeter.histogramBuilder(anyString())).thenReturn(mockHistogramBuilder);
        when(mockHistogramBuilder.setUnit(anyString())).thenReturn(mockHistogramBuilder);
        when(mockHistogramBuilder.build()).thenReturn(mockHistogram);
    }

    // -----------------------------------------------------------------------
    // Gauge selection — metrics in GAUGE_METRIC_NAMES use gaugeBuilder
    // -----------------------------------------------------------------------

    @Test
    public void testGaugeSelection_CurrentLeases() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("CurrentLeases", 5.0, StandardUnit.COUNT);

        verify(mockMeter).gaugeBuilder(anyString());
        verify(mockGauge).set(anyDouble(), any());
    }

    @Test
    public void testGaugeSelection_TotalLeases() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("TotalLeases", 10.0, StandardUnit.COUNT);

        verify(mockMeter).gaugeBuilder(anyString());
        verify(mockGauge).set(anyDouble(), any());
    }

    @Test
    public void testGaugeSelection_NumWorkers() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("NumWorkers", 3.0, StandardUnit.COUNT);

        verify(mockMeter).gaugeBuilder(anyString());
        verify(mockGauge).set(anyDouble(), any());
    }

    @Test
    public void testGaugeSelection_ExpiredLeases() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("ExpiredLeases", 2.0, StandardUnit.COUNT);

        verify(mockMeter).gaugeBuilder(anyString());
    }

    @Test
    public void testGaugeSelection_QueueSize() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("QueueSize", 100.0, StandardUnit.COUNT);

        verify(mockMeter).gaugeBuilder(anyString());
    }

    // -----------------------------------------------------------------------
    // Counter selection — StandardUnit.COUNT metrics NOT in gauge set
    // -----------------------------------------------------------------------

    @Test
    public void testCounterSelection_countUnitNonGauge() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        // "RecordsProcessed" is not in GAUGE_METRIC_NAMES
        scope.addData("RecordsProcessed", 42.0, StandardUnit.COUNT);

        verify(mockMeter).counterBuilder(anyString());
        verify(mockCounter).add(anyDouble(), any());
    }

    @Test
    public void testCounterSelection_Success() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("Success", 1.0, StandardUnit.COUNT);

        verify(mockMeter).counterBuilder(anyString());
        verify(mockCounter).add(anyDouble(), any());
    }

    // -----------------------------------------------------------------------
    // Histogram selection — non-COUNT units use histogramBuilder
    // -----------------------------------------------------------------------

    @Test
    public void testHistogramSelection_milliseconds() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("RenewLease.Time", 150.0, StandardUnit.MILLISECONDS);

        verify(mockMeter).histogramBuilder(anyString());
        verify(mockHistogram).record(anyDouble(), any());
    }

    @Test
    public void testHistogramSelection_bytes() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("PayloadSize", 1024.0, StandardUnit.BYTES);

        verify(mockMeter).histogramBuilder(anyString());
        verify(mockHistogram).record(anyDouble(), any());
    }

    @Test
    public void testHistogramSelection_seconds() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("Latency", 2.5, StandardUnit.SECONDS);

        verify(mockMeter).histogramBuilder(anyString());
    }

    // -----------------------------------------------------------------------
    // Dimension-to-attribute mapping — known dimensions
    // -----------------------------------------------------------------------

    @Test
    public void testDimensionMapping_Operation() {
        assertEquals("aws.kinesis.operation", OtelMetricsScope.DIMENSION_TO_ATTRIBUTE_MAP.get("Operation"));
    }

    @Test
    public void testDimensionMapping_ShardId() {
        assertEquals("aws.kinesis.shard.id", OtelMetricsScope.DIMENSION_TO_ATTRIBUTE_MAP.get("ShardId"));
    }

    @Test
    public void testDimensionMapping_StreamId() {
        assertEquals("aws.kinesis.stream_name", OtelMetricsScope.DIMENSION_TO_ATTRIBUTE_MAP.get("StreamId"));
    }

    @Test
    public void testDimensionMapping_WorkerIdentifier() {
        assertEquals("aws.kinesis.consumer.name", OtelMetricsScope.DIMENSION_TO_ATTRIBUTE_MAP.get("WorkerIdentifier"));
    }

    // -----------------------------------------------------------------------
    // Unknown dimension passthrough
    // -----------------------------------------------------------------------

    @Test
    public void testUnknownDimensionPassthrough() {
        // Unknown dimensions are not in the map, so they pass through as-is
        assertTrue(
                "Unknown dimension should not be in the attribute map",
                !OtelMetricsScope.DIMENSION_TO_ATTRIBUTE_MAP.containsKey("CustomDimension"));
    }

    @Test
    public void testUnknownDimension_addDimensionDoesNotThrow() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        // Should not throw — unknown dimensions pass through as-is
        scope.addDimension("CustomDimension", "customValue");
    }

    // -----------------------------------------------------------------------
    // MetricsLevel filtering — data below configured level is dropped
    // -----------------------------------------------------------------------

    @Test
    public void testMetricsLevelFiltering_detailedDroppedWhenSummary() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.SUMMARY, allDimensions);
        // DETAILED level (9000) is below SUMMARY (10000), so this should be dropped
        scope.addData("SomeMetric", 1.0, StandardUnit.COUNT, MetricsLevel.DETAILED);

        verify(mockMeter, never()).counterBuilder(anyString());
        verify(mockMeter, never()).gaugeBuilder(anyString());
        verify(mockMeter, never()).histogramBuilder(anyString());
    }

    @Test
    public void testMetricsLevelFiltering_summaryPassesWhenSummary() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.SUMMARY, allDimensions);
        scope.addData("RecordsProcessed", 1.0, StandardUnit.COUNT, MetricsLevel.SUMMARY);

        verify(mockMeter).counterBuilder(anyString());
    }

    @Test
    public void testMetricsLevelFiltering_summaryPassesWhenDetailed() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("RecordsProcessed", 1.0, StandardUnit.COUNT, MetricsLevel.SUMMARY);

        verify(mockMeter).counterBuilder(anyString());
    }

    @Test
    public void testMetricsLevelFiltering_noneDropsEverything() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.NONE, allDimensions);
        scope.addData("SomeMetric", 1.0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        scope.addData("AnotherMetric", 2.0, StandardUnit.MILLISECONDS, MetricsLevel.DETAILED);

        verify(mockMeter, never()).counterBuilder(anyString());
        verify(mockMeter, never()).gaugeBuilder(anyString());
        verify(mockMeter, never()).histogramBuilder(anyString());
    }

    // -----------------------------------------------------------------------
    // Dimension filtering — dimensions not in enabled set are excluded
    // -----------------------------------------------------------------------

    @Test
    public void testDimensionFiltering_enabledDimensionIncluded() {
        Set<String> enabledDimensions = Collections.singleton("Operation");
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, enabledDimensions);

        // "Operation" is in the enabled set, so addDimension should work without error
        scope.addDimension("Operation", "GetRecords");
        // Adding data should succeed and use the dimension
        scope.addData("RecordsProcessed", 5.0, StandardUnit.COUNT);

        verify(mockMeter).counterBuilder(anyString());
    }

    @Test
    public void testDimensionFiltering_disabledDimensionExcluded() {
        Set<String> enabledDimensions = Collections.singleton("Operation");
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, enabledDimensions);

        // "ShardId" is NOT in the enabled set, so it should be silently excluded
        scope.addDimension("ShardId", "shard-001");
        scope.addData("RecordsProcessed", 5.0, StandardUnit.COUNT);

        // The metric is still recorded, just without the ShardId dimension
        verify(mockMeter).counterBuilder(anyString());
    }

    @Test
    public void testDimensionFiltering_emptyEnabledSet() {
        Set<String> enabledDimensions = Collections.emptySet();
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, enabledDimensions);

        // No dimensions should be included
        scope.addDimension("Operation", "GetRecords");
        scope.addDimension("ShardId", "shard-001");
        scope.addData("RecordsProcessed", 5.0, StandardUnit.COUNT);

        verify(mockMeter).counterBuilder(anyString());
    }

    // -----------------------------------------------------------------------
    // ALL dimensions — METRICS_DIMENSIONS_ALL includes everything
    // -----------------------------------------------------------------------

    @Test
    public void testAllDimensions_includesEverything() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);

        // All dimensions should be accepted
        scope.addDimension("Operation", "GetRecords");
        scope.addDimension("ShardId", "shard-001");
        scope.addDimension("StreamId", "stream-123");
        scope.addDimension("WorkerIdentifier", "worker-1");
        scope.addDimension("CustomDimension", "customValue");
        scope.addData("RecordsProcessed", 5.0, StandardUnit.COUNT);

        verify(mockMeter).counterBuilder(anyString());
    }

    // -----------------------------------------------------------------------
    // End-state enforcement
    // -----------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void testAddDataAfterEnd_throws() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.end();
        scope.addData("SomeMetric", 1.0, StandardUnit.COUNT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDoubleEnd_throws() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.end();
        scope.end();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddDimensionAfterEnd_throws() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.end();
        scope.addDimension("Operation", "GetRecords");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddDataWithLevelAfterEnd_throws() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.end();
        scope.addData("SomeMetric", 1.0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
    }

    // -----------------------------------------------------------------------
    // Unit conversion — convertUnit for key units
    // -----------------------------------------------------------------------

    @Test
    public void testConvertUnit_COUNT() {
        assertEquals("1", OtelMetricsScope.convertUnit(StandardUnit.COUNT));
    }

    @Test
    public void testConvertUnit_MILLISECONDS() {
        assertEquals("ms", OtelMetricsScope.convertUnit(StandardUnit.MILLISECONDS));
    }

    @Test
    public void testConvertUnit_BYTES() {
        assertEquals("By", OtelMetricsScope.convertUnit(StandardUnit.BYTES));
    }

    @Test
    public void testConvertUnit_SECONDS() {
        assertEquals("s", OtelMetricsScope.convertUnit(StandardUnit.SECONDS));
    }

    @Test
    public void testConvertUnit_PERCENT() {
        assertEquals("%", OtelMetricsScope.convertUnit(StandardUnit.PERCENT));
    }

    @Test
    public void testConvertUnit_NONE() {
        assertEquals("1", OtelMetricsScope.convertUnit(StandardUnit.NONE));
    }

    @Test
    public void testConvertUnit_null() {
        assertEquals("1", OtelMetricsScope.convertUnit(null));
    }

    @Test
    public void testConvertUnit_KILOBYTES() {
        assertEquals("kBy", OtelMetricsScope.convertUnit(StandardUnit.KILOBYTES));
    }

    @Test
    public void testConvertUnit_MEGABYTES() {
        assertEquals("MBy", OtelMetricsScope.convertUnit(StandardUnit.MEGABYTES));
    }

    @Test
    public void testConvertUnit_BITS_SECOND() {
        assertEquals("bit/s", OtelMetricsScope.convertUnit(StandardUnit.BITS_SECOND));
    }

    @Test
    public void testConvertUnit_COUNT_SECOND() {
        assertEquals("1/s", OtelMetricsScope.convertUnit(StandardUnit.COUNT_SECOND));
    }

    // -----------------------------------------------------------------------
    // Noop lifecycle — full lifecycle with OpenTelemetry.noop()
    // -----------------------------------------------------------------------

    @Test
    public void testNoopLifecycle_fullCycle() {
        Meter noopMeter = OpenTelemetry.noop().getMeter("test");
        OtelMetricsScope scope = new OtelMetricsScope(noopMeter, MetricsLevel.DETAILED, allDimensions);

        scope.addDimension("Operation", "GetRecords");
        scope.addDimension("ShardId", "shard-001");
        scope.addData("RecordsProcessed", 42.0, StandardUnit.COUNT);
        scope.addData("MillisBehindLatest", 100.0, StandardUnit.MILLISECONDS);
        scope.addData("CurrentLeases", 5.0, StandardUnit.COUNT);
        scope.end();

        // If we get here without exceptions, the noop lifecycle works
    }

    @Test
    public void testNoopLifecycle_addDataWithLevel() {
        Meter noopMeter = OpenTelemetry.noop().getMeter("test");
        OtelMetricsScope scope = new OtelMetricsScope(noopMeter, MetricsLevel.DETAILED, allDimensions);

        scope.addData("SomeMetric", 1.0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        scope.addData("AnotherMetric", 2.0, StandardUnit.MILLISECONDS, MetricsLevel.DETAILED);
        scope.end();
    }

    @Test
    public void testNoopLifecycle_noDataNoException() {
        Meter noopMeter = OpenTelemetry.noop().getMeter("test");
        OtelMetricsScope scope = new OtelMetricsScope(noopMeter, MetricsLevel.DETAILED, allDimensions);
        scope.end();
    }

    @Test
    public void testNoopLifecycle_dimensionsOnly() {
        Meter noopMeter = OpenTelemetry.noop().getMeter("test");
        OtelMetricsScope scope = new OtelMetricsScope(noopMeter, MetricsLevel.DETAILED, allDimensions);

        scope.addDimension("Operation", "GetRecords");
        scope.addDimension("ShardId", "shard-001");
        scope.end();
    }

    // -----------------------------------------------------------------------
    // addData without explicit level defaults to DETAILED
    // -----------------------------------------------------------------------

    @Test
    public void testAddDataDefaultLevel_passesWhenDetailed() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.DETAILED, allDimensions);
        scope.addData("RecordsProcessed", 1.0, StandardUnit.COUNT);

        verify(mockMeter).counterBuilder(anyString());
    }

    @Test
    public void testAddDataDefaultLevel_droppedWhenSummary() {
        OtelMetricsScope scope = new OtelMetricsScope(mockMeter, MetricsLevel.SUMMARY, allDimensions);
        // Default level is DETAILED, which is below SUMMARY threshold
        scope.addData("RecordsProcessed", 1.0, StandardUnit.COUNT);

        verify(mockMeter, never()).counterBuilder(anyString());
    }

    // -----------------------------------------------------------------------
    // GAUGE_METRIC_NAMES set membership
    // -----------------------------------------------------------------------

    @Test
    public void testGaugeMetricNames_containsExpectedEntries() {
        assertTrue(OtelMetricsScope.GAUGE_METRIC_NAMES.contains("CurrentLeases"));
        assertTrue(OtelMetricsScope.GAUGE_METRIC_NAMES.contains("TotalLeases"));
        assertTrue(OtelMetricsScope.GAUGE_METRIC_NAMES.contains("NumWorkers"));
        assertTrue(OtelMetricsScope.GAUGE_METRIC_NAMES.contains("ExpiredLeases"));
        assertTrue(OtelMetricsScope.GAUGE_METRIC_NAMES.contains("VeryOldLeases"));
        assertTrue(OtelMetricsScope.GAUGE_METRIC_NAMES.contains("ActiveStreams.Count"));
        assertTrue(OtelMetricsScope.GAUGE_METRIC_NAMES.contains("StreamsPendingDeletion.Count"));
        assertTrue(OtelMetricsScope.GAUGE_METRIC_NAMES.contains("QueueSize"));
        assertTrue(OtelMetricsScope.GAUGE_METRIC_NAMES.contains("NumStreamsToSync"));
    }

    @Test
    public void testGaugeMetricNames_doesNotContainCounterMetrics() {
        assertTrue(!OtelMetricsScope.GAUGE_METRIC_NAMES.contains("RecordsProcessed"));
        assertTrue(!OtelMetricsScope.GAUGE_METRIC_NAMES.contains("Success"));
        assertTrue(!OtelMetricsScope.GAUGE_METRIC_NAMES.contains("DataBytesProcessed"));
    }
}
