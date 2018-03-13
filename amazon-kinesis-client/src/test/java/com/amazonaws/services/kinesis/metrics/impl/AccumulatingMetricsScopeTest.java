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

import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.metrics.impl.AccumulateByNameMetricsScope;

public class AccumulatingMetricsScopeTest {

    private static class TestScope extends AccumulateByNameMetricsScope {

        @Override
        public void end() {

        }

        public void assertMetrics(MetricDatum... expectedData) {
            for (MetricDatum expected : expectedData) {
                MetricDatum actual = data.remove(expected.getMetricName());
                Assert.assertEquals(expected, actual);
            }

            Assert.assertEquals("Data should be empty at the end of assertMetrics", 0, data.size());
        }
    }

    @Test
    public void testSingleAdd() {
        TestScope scope = new TestScope();

        scope.addData("name", 2.0, StandardUnit.Count);
        scope.assertMetrics(TestHelper.constructDatum("name", StandardUnit.Count, 2.0, 2.0, 2.0, 1));
    }

    @Test
    public void testAccumulate() {
        TestScope scope = new TestScope();

        scope.addData("name", 2.0, StandardUnit.Count);
        scope.addData("name", 3.0, StandardUnit.Count);
        scope.assertMetrics(TestHelper.constructDatum("name", StandardUnit.Count, 3.0, 2.0, 5.0, 2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAccumulateWrongUnit() {
        TestScope scope = new TestScope();

        scope.addData("name", 2.0, StandardUnit.Count);
        scope.addData("name", 3.0, StandardUnit.Megabits);
    }
}
