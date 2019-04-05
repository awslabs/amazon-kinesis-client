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


import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

public abstract class InterceptingMetricsFactory implements MetricsFactory {

    private final MetricsFactory other;

    public InterceptingMetricsFactory(MetricsFactory other) {
        this.other = other;
    }

    @Override
    public MetricsScope createMetrics() {
        MetricsScope otherScope = other.createMetrics();
        interceptCreateMetrics(otherScope);
        return new InterceptingMetricsScope(otherScope);
    }

    protected void interceptCreateMetrics(MetricsScope scope) {
        // Default implementation does nothing;
    }

    protected void interceptAddData(String name, double value, StandardUnit unit, MetricsScope scope) {
        scope.addData(name, value, unit);
    }

    protected void interceptAddData(String name, double value, StandardUnit unit, MetricsLevel level, MetricsScope scope) {
        scope.addData(name, value, unit, level);
    }

    protected void interceptAddDimension(String name, String value, MetricsScope scope) {
        scope.addDimension(name, value);
    }

    protected void interceptEnd(MetricsScope scope) {
        scope.end();
    }

    private class InterceptingMetricsScope implements MetricsScope {

        private MetricsScope other;

        public InterceptingMetricsScope(MetricsScope other) {
            this.other = other;
        }

        @Override
        public void addData(String name, double value, StandardUnit unit) {
            interceptAddData(name, value, unit, other);
        }

        @Override
        public void addData(String name, double value, StandardUnit unit, MetricsLevel level) {
            interceptAddData(name, value, unit, level, other);
        }

        @Override
        public void addDimension(String name, String value) {
            interceptAddDimension(name, value, other);
        }

        @Override
        public void end() {
            interceptEnd(other);
        }

    }

}
