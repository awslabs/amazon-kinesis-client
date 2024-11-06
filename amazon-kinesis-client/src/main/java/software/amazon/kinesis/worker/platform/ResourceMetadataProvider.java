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

package software.amazon.kinesis.worker.platform;

import java.util.Optional;

import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Interface for providing resource metadata for worker.
 */
@KinesisClientInternalApi
public interface ResourceMetadataProvider {
    /**
     * Enum representing the different compute platforms.
     */
    enum ComputePlatform {
        EC2,
        ECS,
        EKS,
        UNKNOWN
    }

    /**
     * Check if the worker is running on the specific platform.
     *
     * @return true if the worker is running on the specific platform, false otherwise.
     */
    boolean isOnPlatform();

    /**
     * Get the name of the compute platform.
     *
     * @return the platform represent by the class.
     */
    ComputePlatform getPlatform();

    /**
     * Get the operating range data provider.
     *
     * @return the operating range data provider.
     */
    Optional<OperatingRangeDataProvider> getOperatingRangeDataProvider();
}
