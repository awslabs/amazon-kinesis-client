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

public abstract class EndingMetricsScope extends DimensionTrackingMetricsScope {

    private boolean ended = false;

    @Override
    public void addData(String name, double value, StandardUnit unit) {
        if (ended) {
            throw new IllegalArgumentException("Cannot call addData after calling IMetricsScope.end()");
        }
    }

    @Override
    public void addData(String name, double value, StandardUnit unit, MetricsLevel level) {
        if (ended) {
            throw new IllegalArgumentException("Cannot call addData after calling IMetricsScope.end()");
        }
    }

    @Override
    public void addDimension(String name, String value) {
        super.addDimension(name, value);
        if (ended) {
            throw new IllegalArgumentException("Cannot call addDimension after calling IMetricsScope.end()");
        }
    }

    @Override
    public void end() {
        if (ended) {
            throw new IllegalArgumentException("Cannot call IMetricsScope.end() more than once on the same instance");
        }
        ended = true;
    }
}
