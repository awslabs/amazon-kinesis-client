/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.worker.metric;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

public interface WorkerMetric {
    /**
     * WorkerMetricStats short name that is used as attribute name for it in storage.
     * @return short name for the WorkerMetricStats
     */
    String getShortName();

    /**
     * Current WorkerMetricValue. WorkerMetricValue is a normalized percentage value to its max configured limits.
     * E.g., if for a worker max network bandwidth is 10Gbps and current used bandwidth is 2Gbps, then WorkerMetricValue for
     * NetworkWorkerMetrics will be 20 (%).
     *
     * @return WorkerMetricValue between 0 and 100 (both inclusive)
     */
    WorkerMetricValue capture();

    /**
     * Gets the operating range for this workerMetrics
     * @return Operating range for this workerMetrics
     */
    OperatingRange getOperatingRange();

    /**
     * Type of the current WorkerMetricStats.
     * @return WorkerMetricType
     */
    WorkerMetricType getWorkerMetricType();

    /**
     * WorkerMetricValue model class is used as return type for the capture() method to have a strong checks at the build
     * time of the object itself.
     */
    @Builder
    class WorkerMetricValue {

        @Getter
        private final Double value;

        private WorkerMetricValue(@NonNull final Double value) {
            Preconditions.checkArgument(
                    !(value < 0 || value > 100), value + " is either less than 0 or greater than 100");
            this.value = value;
        }
    }
}
