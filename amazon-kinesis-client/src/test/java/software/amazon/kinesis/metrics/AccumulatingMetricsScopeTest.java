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

import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;


public class AccumulatingMetricsScopeTest {

    private static class TestScope extends AccumulateByNameMetricsScope {
        public void assertMetrics(MetricDatum... expectedData) {
            for (MetricDatum expected : expectedData) {
                MetricDatum actual = data.remove(expected.metricName());
                Assert.assertEquals(expected, actual);
            }

            Assert.assertEquals("Data should be empty at the end of assertMetrics", 0, data.size());
        }
    }

    @Test
    public void testSingleAdd() {
        TestScope scope = new TestScope();

        scope.addData("name", 2.0, StandardUnit.COUNT);
        scope.assertMetrics(TestHelper.constructDatum("name", StandardUnit.COUNT, 2.0, 2.0, 2.0, 1));
    }

    @Test
    public void testAccumulate() {
        TestScope scope = new TestScope();

        scope.addData("name", 2.0, StandardUnit.COUNT);
        scope.addData("name", 3.0, StandardUnit.COUNT);
        scope.assertMetrics(TestHelper.constructDatum("name", StandardUnit.COUNT, 3.0, 2.0, 5.0, 2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAccumulateWrongUnit() {
        TestScope scope = new TestScope();

        scope.addData("name", 2.0, StandardUnit.COUNT);
        scope.addData("name", 3.0, StandardUnit.MEGABITS);
    }
}
