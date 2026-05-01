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

/**
 * Enum representing the available metrics publishing backends for the KCL.
 */
public enum MetricsBackend {
    /**
     * Publish metrics via the existing CloudWatch PutMetricData API.
     */
    CLOUDWATCH,

    /**
     * Native OpenTelemetry library instrumentation backend. Records raw observations on OTel
     * {@code Meter} instruments (counters, histograms) using only the {@code opentelemetry-api}
     * dependency. The application owner configures exporters and resource attributes through
     * standard OTel SDK autoconfiguration.
     */
    OTEL
}
