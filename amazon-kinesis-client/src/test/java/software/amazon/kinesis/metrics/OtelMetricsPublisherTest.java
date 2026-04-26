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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OtelMetricsPublisherTest {

    private static final String NAMESPACE = "TestNamespace";

    @Mock
    private MetricExporter exporter;

    private OtelMetricsPublisher publisher;

    @Before
    public void setup() {
        when(exporter.export(anyCollectionOf(MetricData.class))).thenReturn(CompletableResultCode.ofSuccess());
        publisher = new OtelMetricsPublisher(NAMESPACE, exporter, Resource.empty());
    }

    /**
     * Test that publishMetrics correctly batches data (batch size 20) and calls the exporter.
     * 25 items should result in 2 export calls (20 + 5).
     */
    @Test
    public void testPublishMetricsBatching() {
        List<MetricDatumWithKey<OtelMetricKey>> data = constructOtelDatumList(25);
        publisher.publishMetrics(data);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<MetricData>> captor = ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(exporter, Mockito.times(2)).export(captor.capture());

        List<Collection<MetricData>> batches = captor.getAllValues();
        Assert.assertEquals(20, batches.get(0).size());
        Assert.assertEquals(5, batches.get(1).size());
    }

    /**
     * Test that a single batch of exactly 20 items results in 1 export call.
     */
    @Test
    public void testPublishMetricsExactBatch() {
        List<MetricDatumWithKey<OtelMetricKey>> data = constructOtelDatumList(20);
        publisher.publishMetrics(data);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<MetricData>> captor = ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(exporter, Mockito.times(1)).export(captor.capture());
        Assert.assertEquals(20, captor.getValue().size());
    }

    /**
     * Test OTLP conversion preserves metric name, unit, and StatisticSet values.
     */
    @Test
    public void testOtlpConversionPreservesMetricData() {
        MetricDatum datum = TestHelper.constructDatum("TestMetric", StandardUnit.MILLISECONDS, 100.0, 1.0, 250.0, 5);
        InstrumentationScopeInfo scopeInfo = InstrumentationScopeInfo.create(NAMESPACE);
        long nowNanos = System.nanoTime();

        MetricData metricData = publisher.convertToOtlpMetricData(datum, scopeInfo, nowNanos);

        Assert.assertEquals("TestMetric", metricData.getName());
        Assert.assertEquals("ms", metricData.getUnit());

        HistogramPointData point =
                metricData.getHistogramData().getPoints().iterator().next();
        Assert.assertEquals(250.0, point.getSum(), 0.0001);
        Assert.assertEquals(5L, point.getCount());
        Assert.assertEquals(1.0, point.getMin(), 0.0001);
        Assert.assertEquals(100.0, point.getMax(), 0.0001);
    }

    /**
     * Test dimension-to-attribute mapping for known KCL dimensions.
     */
    @Test
    public void testDimensionToAttributeMapping() {
        List<Dimension> dimensions = new ArrayList<>();
        dimensions.add(TestHelper.constructDimension("Operation", "ProcessTask"));
        dimensions.add(TestHelper.constructDimension("ShardId", "shardId-000000000001"));
        dimensions.add(
                TestHelper.constructDimension("StreamId", "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"));
        dimensions.add(TestHelper.constructDimension("WorkerIdentifier", "worker-1"));

        Attributes attributes = publisher.convertDimensionsToAttributes(dimensions);

        Assert.assertEquals("ProcessTask", attributes.get(AttributeKey.stringKey("aws.kinesis.operation")));
        Assert.assertEquals("shardId-000000000001", attributes.get(AttributeKey.stringKey("aws.kinesis.shard.id")));
        Assert.assertEquals(
                "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
                attributes.get(AttributeKey.stringKey("aws.kinesis.stream_name")));
        Assert.assertEquals("worker-1", attributes.get(AttributeKey.stringKey("aws.kinesis.consumer.name")));
    }

    /**
     * Test that unknown dimensions are passed through with their original name.
     */
    @Test
    public void testUnknownDimensionPassthrough() {
        List<Dimension> dimensions = new ArrayList<>();
        dimensions.add(TestHelper.constructDimension("CustomDimension", "customValue"));

        Attributes attributes = publisher.convertDimensionsToAttributes(dimensions);

        Assert.assertEquals("customValue", attributes.get(AttributeKey.stringKey("CustomDimension")));
    }

    /**
     * Test unit conversion for various StandardUnit values.
     */
    @Test
    public void testUnitConversion() {
        Assert.assertEquals("1", OtelMetricsPublisher.convertUnit(StandardUnit.COUNT));
        Assert.assertEquals("ms", OtelMetricsPublisher.convertUnit(StandardUnit.MILLISECONDS));
        Assert.assertEquals("By", OtelMetricsPublisher.convertUnit(StandardUnit.BYTES));
        Assert.assertEquals("s", OtelMetricsPublisher.convertUnit(StandardUnit.SECONDS));
        Assert.assertEquals("us", OtelMetricsPublisher.convertUnit(StandardUnit.MICROSECONDS));
        Assert.assertEquals("%", OtelMetricsPublisher.convertUnit(StandardUnit.PERCENT));
        Assert.assertEquals("1", OtelMetricsPublisher.convertUnit(StandardUnit.NONE));
        Assert.assertEquals("1", OtelMetricsPublisher.convertUnit(null));
        Assert.assertEquals("kBy", OtelMetricsPublisher.convertUnit(StandardUnit.KILOBYTES));
        Assert.assertEquals("MBy", OtelMetricsPublisher.convertUnit(StandardUnit.MEGABYTES));
        Assert.assertEquals("GBy", OtelMetricsPublisher.convertUnit(StandardUnit.GIGABYTES));
        Assert.assertEquals("TBy", OtelMetricsPublisher.convertUnit(StandardUnit.TERABYTES));
        Assert.assertEquals("bit", OtelMetricsPublisher.convertUnit(StandardUnit.BITS));
        Assert.assertEquals("By/s", OtelMetricsPublisher.convertUnit(StandardUnit.BYTES_SECOND));
        Assert.assertEquals("1/s", OtelMetricsPublisher.convertUnit(StandardUnit.COUNT_SECOND));
    }

    /**
     * Test resource attribute merging: custom attributes override auto-detected ones.
     */
    @Test
    public void testResourceAttributeMerging() {
        Map<String, String> autoDetected = new HashMap<>();
        autoDetected.put("service.name", "auto-app");
        autoDetected.put("cloud.provider", "aws");

        Map<String, String> custom = new HashMap<>();
        custom.put("service.name", "custom-app");
        custom.put("custom.key", "custom-value");

        Resource resource = OtelMetricsPublisher.buildResource(autoDetected, custom);
        Attributes attrs = resource.getAttributes();

        Assert.assertEquals("custom-app", attrs.get(AttributeKey.stringKey("service.name")));
        Assert.assertEquals("aws", attrs.get(AttributeKey.stringKey("cloud.provider")));
        Assert.assertEquals("custom-value", attrs.get(AttributeKey.stringKey("custom.key")));
    }

    /**
     * Test that shutdown() calls flush and shutdown on the exporter.
     */
    @Test
    public void testShutdown() {
        when(exporter.flush()).thenReturn(CompletableResultCode.ofSuccess());
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        publisher.shutdown();

        Mockito.verify(exporter).flush();
        Mockito.verify(exporter).shutdown();
    }

    /**
     * Test that empty dimensions produce empty attributes.
     */
    @Test
    public void testEmptyDimensions() {
        Attributes attributes = publisher.convertDimensionsToAttributes(Collections.emptyList());
        Assert.assertEquals(Attributes.empty(), attributes);

        Attributes nullAttributes = publisher.convertDimensionsToAttributes(null);
        Assert.assertEquals(Attributes.empty(), nullAttributes);
    }

    /**
     * Test that the instrumentation scope name equals the namespace.
     */
    @Test
    public void testInstrumentationScopeNameIsNamespace() {
        MetricDatum datum = TestHelper.constructDatum("ScopeTest", StandardUnit.COUNT, 10.0, 1.0, 50.0, 5);
        InstrumentationScopeInfo scopeInfo = InstrumentationScopeInfo.create(NAMESPACE);
        long nowNanos = System.nanoTime();

        MetricData metricData = publisher.convertToOtlpMetricData(datum, scopeInfo, nowNanos);

        Assert.assertEquals(NAMESPACE, metricData.getInstrumentationScopeInfo().getName());
    }

    // --- Helper methods ---

    public static List<MetricDatumWithKey<OtelMetricKey>> constructOtelDatumList(int count) {
        List<MetricDatumWithKey<OtelMetricKey>> data = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            MetricDatum datum =
                    TestHelper.constructDatum("datum" + Integer.toString(i), StandardUnit.COUNT, i, i, i, 1);
            data.add(new MetricDatumWithKey<OtelMetricKey>(new OtelMetricKey(datum), datum));
        }
        return data;
    }
}
