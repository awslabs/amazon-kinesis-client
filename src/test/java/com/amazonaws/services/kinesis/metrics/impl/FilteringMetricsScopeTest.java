/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.metrics.impl;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.google.common.collect.ImmutableSet;

public class FilteringMetricsScopeTest {

    private static class TestScope extends FilteringMetricsScope {

        private TestScope() {
        }

        private TestScope(MetricsLevel metricsLevel, Set<String> metricsEnabledDimensions) {
            super(metricsLevel, metricsEnabledDimensions);
        }

        public void assertMetrics(MetricDatum... expectedData) {
            for (MetricDatum expected : expectedData) {
                MetricDatum actual = data.remove(expected.getMetricName());
                Assert.assertEquals(expected, actual);
            }

            Assert.assertEquals("Data should be empty at the end of assertMetrics", 0, data.size());
        }

        public void assertDimensions(Dimension... dimensions) {
            for (Dimension dimension : dimensions) {
                Assert.assertTrue(getDimensions().remove(dimension));
            }

            Assert.assertTrue("Dimensions should be empty at the end of assertDimensions", getDimensions().isEmpty());
        }
    }

    @Test
    public void testDefaultAddAll() {
        TestScope scope = new TestScope();
        scope.addData("detailedDataName", 2.0, StandardUnit.Count, MetricsLevel.DETAILED);
        scope.addData("noLevelDataName", 3.0, StandardUnit.Milliseconds);
        scope.addDimension("dimensionName", "dimensionValue");

        // By default all metrics and dimensions should be allowed.
        scope.assertMetrics(
                TestHelper.constructDatum("detailedDataName", StandardUnit.Count, 2.0, 2.0, 2.0, 1),
                TestHelper.constructDatum("noLevelDataName", StandardUnit.Milliseconds, 3.0, 3.0, 3.0, 1.0));
        scope.assertDimensions(TestHelper.constructDimension("dimensionName", "dimensionValue"));
    }

    @Test
    public void testMetricsLevel() {
        TestScope scope = new TestScope(MetricsLevel.SUMMARY, null);
        scope.addData("summaryDataName", 2.0, StandardUnit.Count, MetricsLevel.SUMMARY);
        scope.addData("summaryDataName", 10.0, StandardUnit.Count, MetricsLevel.SUMMARY);
        scope.addData("detailedDataName", 4.0, StandardUnit.Bytes, MetricsLevel.DETAILED);
        scope.addData("noLevelDataName", 3.0, StandardUnit.Milliseconds);

        scope.assertMetrics(TestHelper.constructDatum("summaryDataName", StandardUnit.Count, 10.0, 2.0, 12.0, 2.0));
    }

    @Test
    public void testMetricsLevelNone() {
        TestScope scope = new TestScope(MetricsLevel.NONE, null);
        scope.addData("summaryDataName", 2.0, StandardUnit.Count, MetricsLevel.SUMMARY);
        scope.addData("summaryDataName", 10.0, StandardUnit.Count, MetricsLevel.SUMMARY);
        scope.addData("detailedDataName", 4.0, StandardUnit.Bytes, MetricsLevel.DETAILED);
        scope.addData("noLevelDataName", 3.0, StandardUnit.Milliseconds);

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
                "ThisDoesNotMatter", IMetricsScope.METRICS_DIMENSIONS_ALL, "ThisAlsoDoesNotMatter"));
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
