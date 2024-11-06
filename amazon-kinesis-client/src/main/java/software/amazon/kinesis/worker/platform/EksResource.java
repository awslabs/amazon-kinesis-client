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

import java.io.File;
import java.util.Optional;
import java.util.stream.Stream;

import org.jetbrains.annotations.VisibleForTesting;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

import static software.amazon.kinesis.worker.platform.OperatingRangeDataProvider.LINUX_EKS_CGROUP_V1;
import static software.amazon.kinesis.worker.platform.OperatingRangeDataProvider.LINUX_EKS_CGROUP_V2;

/**
 * Provides resource metadata for EKS.
 */
@KinesisClientInternalApi
public class EksResource implements ResourceMetadataProvider {
    private static final String K8S_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token";
    private final String k8sTokenPath;

    @VisibleForTesting
    EksResource(String k8sTokenPath) {
        this.k8sTokenPath = k8sTokenPath;
    }

    /**
     * Factory method to create an instance of EksResource.
     *
     * @return an instance of EksResource
     */
    public static EksResource create() {
        return new EksResource(K8S_TOKEN_PATH);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOnPlatform() {
        return new File(this.k8sTokenPath).exists();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ComputePlatform getPlatform() {
        return ComputePlatform.EKS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<OperatingRangeDataProvider> getOperatingRangeDataProvider() {
        // It is only possible that either cgroupv1 or cgroupv2 is mounted
        return Stream.of(LINUX_EKS_CGROUP_V2, LINUX_EKS_CGROUP_V1)
                .filter(OperatingRangeDataProvider::isProvider)
                .findFirst();
    }
}
