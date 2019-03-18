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
package com.amazonaws.services.kinesis.metrics.impl;

import org.junit.Test;

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.metrics.impl.EndingMetricsScope;

public class EndingMetricsScopeTest {

    private static class TestScope extends EndingMetricsScope {

    }

    @Test
    public void testAddDataNotEnded() {
        TestScope scope = new TestScope();
        scope.addData("foo", 1.0, StandardUnit.Count);
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
        scope.addData("foo", 1.0, StandardUnit.Count);
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
