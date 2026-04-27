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

/**
 * A simple value object buffering a single raw metric observation within an {@code OtelMetricsScope}.
 */
class MetricObservation {
    final String name;
    final double value;
    final StandardUnit unit;

    MetricObservation(String name, double value, StandardUnit unit) {
        this.name = name;
        this.value = value;
        this.unit = unit;
    }
}
