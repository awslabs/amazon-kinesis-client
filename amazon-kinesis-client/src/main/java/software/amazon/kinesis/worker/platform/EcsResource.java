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

import java.util.Map;
import java.util.Optional;

import org.jetbrains.annotations.VisibleForTesting;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

import static software.amazon.kinesis.worker.platform.OperatingRangeDataProvider.LINUX_ECS_METADATA_KEY_V4;

/**
 * Provides resource metadata for ECS.
 */
@KinesisClientInternalApi
public class EcsResource implements ResourceMetadataProvider {
    static final String ECS_METADATA_KEY_V3 = "ECS_CONTAINER_METADATA_URI";
    static final String ECS_METADATA_KEY_V4 = "ECS_CONTAINER_METADATA_URI_V4";

    private final Map<String, String> sysEnv;

    @VisibleForTesting
    EcsResource(Map<String, String> sysEnv) {
        this.sysEnv = sysEnv;
    }

    /**
     * Factory method to create an instance of EcsResource.
     *
     * @return an instance of EcsResource
     */
    public static EcsResource create() {
        return new EcsResource(System.getenv());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOnPlatform() {
        return !sysEnv.getOrDefault(ECS_METADATA_KEY_V3, "").isEmpty()
                || !sysEnv.getOrDefault(ECS_METADATA_KEY_V4, "").isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ComputePlatform getPlatform() {
        return ComputePlatform.ECS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<OperatingRangeDataProvider> getOperatingRangeDataProvider() {
        return Optional.of(LINUX_ECS_METADATA_KEY_V4).filter(OperatingRangeDataProvider::isProvider);
    }
}
