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

import software.amazon.kinesis.annotations.KinesisClientInternalApi;

import static software.amazon.kinesis.worker.platform.EcsResource.ECS_METADATA_KEY_V4;

/**
 * Enum representing the different operating range metadata providers.
 */
@KinesisClientInternalApi
public enum OperatingRangeDataProvider {
    LINUX_EKS_CGROUP_V1 {
        @Override
        public boolean isProvider() {
            if (!OperatingRangeDataProvider.isLinux()) {
                return false;
            }
            // Check if the cgroup v2 specific file does NOT exist
            final File cgroupV2File = new File("/sys/fs/cgroup/cgroup.controllers");
            if (cgroupV2File.exists()) {
                return false;
            }

            // Check for common cgroup v1 directories like memory or cpu
            final File memoryCgroup = new File("/sys/fs/cgroup/memory");
            final File cpuCgroup = new File("/sys/fs/cgroup/cpu");

            return memoryCgroup.exists() || cpuCgroup.exists();
        }
    },
    LINUX_EKS_CGROUP_V2 {
        @Override
        public boolean isProvider() {
            if (!OperatingRangeDataProvider.isLinux()) {
                return false;
            }

            // Check if the cgroup v2 specific file exists
            final File cgroupV2File = new File("/sys/fs/cgroup/cgroup.controllers");

            return cgroupV2File.exists();
        }
    },
    LINUX_ECS_METADATA_KEY_V4 {
        @Override
        public boolean isProvider() {
            if (!OperatingRangeDataProvider.isLinux()) {
                return false;
            }
            return !System.getenv().getOrDefault(ECS_METADATA_KEY_V4, "").isEmpty();
        }
    },
    LINUX_PROC {
        @Override
        public boolean isProvider() {
            if (!OperatingRangeDataProvider.isLinux()) {
                return false;
            }
            // Check if /proc directory exists (common in Linux environments)
            return new File("/proc").exists();
        }
    };

    private static boolean isLinux() {
        return System.getProperty("os.name").toLowerCase().contains("linux");
    }

    /**
     * Abstract method to check if the provider is supported on the current platform.
     *
     * @return true if the provider is supported, false otherwise.
     */
    public abstract boolean isProvider();
}
