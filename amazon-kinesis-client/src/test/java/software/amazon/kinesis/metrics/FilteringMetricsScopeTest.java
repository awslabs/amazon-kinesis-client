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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

public class FilteringMetricsScopeTest {

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class TestScope extends FilteringMetricsScope {
        private TestScope(MetricsLevel metricsLevel, Set<String> metricsEnabledDimensions) {
            super(metricsLevel, metricsEnabledDimensions);
        }

        void assertMetrics(MetricDatum... expectedData) {
            for (MetricDatum expected : expectedData) {
                MetricDatum actual = data.remove(expected.metricName());
                Assert.assertEquals(expected, actual);
            }

            Assert.assertEquals("Data should be empty at the end of assertMetrics", 0, data.size());
        }

        void assertDimensions(Dimension... dimensions) {
            for (Dimension dimension : dimensions) {
                Assert.assertTrue(getDimensions().remove(dimension));
            }

            Assert.assertTrue("Dimensions should be empty at the end of assertDimensions", getDimensions().isEmpty());
        }
    }

    @Test
    public void testDefaultAddAll() {
        TestScope scope = new TestScope();
        scope.addData("detailedDataName", 2.0, StandardUnit.COUNT, MetricsLevel.DETAILED);
        scope.addData("noLevelDataName", 3.0, StandardUnit.MILLISECONDS);
        scope.addDimension("dimensionName", "dimensionValue");

        // By default all metrics and dimensions should be allowed.
        scope.assertMetrics(
                TestHelper.constructDatum("detailedDataName", StandardUnit.COUNT, 2.0, 2.0, 2.0, 1),
                TestHelper.constructDatum("noLevelDataName", StandardUnit.MILLISECONDS, 3.0, 3.0, 3.0, 1.0));
        scope.assertDimensions(TestHelper.constructDimension("dimensionName", "dimensionValue"));
    }

    @Test
    public void testMetricsLevel() {
        TestScope scope = new TestScope(MetricsLevel.SUMMARY, null);
        scope.addData("summaryDataName", 2.0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        scope.addData("summaryDataName", 10.0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        scope.addData("detailedDataName", 4.0, StandardUnit.BYTES, MetricsLevel.DETAILED);
        scope.addData("noLevelDataName", 3.0, StandardUnit.MILLISECONDS);

        scope.assertMetrics(TestHelper.constructDatum("summaryDataName", StandardUnit.COUNT, 10.0, 2.0, 12.0, 2.0));
    }

    @Test
    public void testMetricsLevelNone() {
        TestScope scope = new TestScope(MetricsLevel.NONE, null);
        scope.addData("summaryDataName", 2.0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        scope.addData("summaryDataName", 10.0, StandardUnit.COUNT, MetricsLevel.SUMMARY);
        scope.addData("detailedDataName", 4.0, StandardUnit.BYTES, MetricsLevel.DETAILED);
        scope.addData("noLevelDataName", 3.0, StandardUnit.MILLISECONDS);

        // No metrics should be emitted.
        scope.assertMetrics();
    }

    @Test
    public void testMetricsDimensions() {
        TestScope scope = new TestScope(MetricsLevel.DETAILED, ImmutableSet.of("ShardId"));
        scope.addDimension("ShardId", "shard-0001");
        scope.addDimension("Operation", "ProcessRecords");
        scope.addDimension("ShardId", "shard-0001");
        scope.addDimension("ShardId", "shard-0002");
        scope.addDimension("WorkerIdentifier", "testworker");

        scope.assertDimensions(
                TestHelper.constructDimension("ShardId", "shard-0001"),
                TestHelper.constructDimension("ShardId", "shard-0002"));
    }

    @Test
    public void testMetricsDimensionsAll() {
        TestScope scope = new TestScope(MetricsLevel.DETAILED, ImmutableSet.of(
                "ThisDoesNotMatter", MetricsScope.METRICS_DIMENSIONS_ALL, "ThisAlsoDoesNotMatter"));
        scope.addDimension("ShardId", "shard-0001");
        scope.addDimension("Operation", "ProcessRecords");
        scope.addDimension("ShardId", "shard-0001");
        scope.addDimension("ShardId", "shard-0002");
        scope.addDimension("WorkerIdentifier", "testworker");

        scope.assertDimensions(
                TestHelper.constructDimension("ShardId", "shard-0001"),
                TestHelper.constructDimension("ShardId", "shard-0002"),
                TestHelper.constructDimension("Operation", "ProcessRecords"),
                TestHelper.constructDimension("WorkerIdentifier", "testworker"));
    }
}
