/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.metrics;

import org.junit.Test;

import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.metrics.EndingMetricsScope;

public class EndingMetricsScopeTest {

    private static class TestScope extends EndingMetricsScope {

    }

    @Test
    public void testAddDataNotEnded() {
        TestScope scope = new TestScope();
        scope.addData("foo", 1.0, StandardUnit.COUNT);
    }

    @Test
    public void testAddDimensionNotEnded() {
        TestScope scope = new TestScope();
        scope.addDimension("foo", "bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddDataEnded() {
        TestScope scope = new TestScope();
        scope.end();
        scope.addData("foo", 1.0, StandardUnit.COUNT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddDimensionEnded() {
        TestScope scope = new TestScope();
        scope.end();
        scope.addDimension("foo", "bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDoubleEnd() {
        TestScope scope = new TestScope();
        scope.end();
        scope.end();
    }
}
