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

package software.amazon.kinesis.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetric;
import software.amazon.kinesis.worker.metric.impl.container.Cgroupv1CpuWorkerMetric;
import software.amazon.kinesis.worker.metric.impl.container.Cgroupv2CpuWorkerMetric;
import software.amazon.kinesis.worker.metric.impl.container.EcsCpuWorkerMetric;
import software.amazon.kinesis.worker.metric.impl.linux.LinuxCpuWorkerMetric;
import software.amazon.kinesis.worker.platform.Ec2Resource;
import software.amazon.kinesis.worker.platform.EcsResource;
import software.amazon.kinesis.worker.platform.EksResource;
import software.amazon.kinesis.worker.platform.OperatingRangeDataProvider;
import software.amazon.kinesis.worker.platform.ResourceMetadataProvider;

/**
 * Class to select appropriate WorkerMetricStats based on the operating range provider that is available on the instance.
 */
@Slf4j
@RequiredArgsConstructor
@KinesisClientInternalApi
public class WorkerMetricsSelector {

    private static final OperatingRange DEFAULT_100_PERC_UTILIZED_OPERATING_RANGE =
            OperatingRange.builder().maxUtilization(100).build();

    private final List<ResourceMetadataProvider> workerComputePlatforms;

    /**
     * Factory method to create an instance of WorkerMetricsSelector.
     *
     * @return WorkerMetricsSelector instance
     */
    public static WorkerMetricsSelector create() {
        final List<ResourceMetadataProvider> resourceMetadataProviders = new ArrayList<>();
        resourceMetadataProviders.add(EcsResource.create());
        resourceMetadataProviders.add(EksResource.create());
        // ec2 has to be the last one to check
        resourceMetadataProviders.add(Ec2Resource.create());
        return new WorkerMetricsSelector(resourceMetadataProviders);
    }

    private Optional<OperatingRangeDataProvider> getOperatingRangeDataProvider() {
        for (ResourceMetadataProvider platform : workerComputePlatforms) {
            if (platform.isOnPlatform()) {
                final ResourceMetadataProvider.ComputePlatform computePlatform = platform.getPlatform();
                log.info("Worker is running on {}", computePlatform);
                return platform.getOperatingRangeDataProvider();
            }
        }
        return Optional.empty();
    }

    /**
     * Returns a list of WorkerMetricStats based on the operating range provider the worker uses.
     *
     * @return List of WorkerMetricStats
     */
    public List<WorkerMetric> getDefaultWorkerMetrics() {
        final List<WorkerMetric> workerMetrics = new ArrayList<>();
        final Optional<OperatingRangeDataProvider> optionalProvider = getOperatingRangeDataProvider();
        if (!optionalProvider.isPresent()) {
            log.warn("Did not find an operating range metadata provider.");
            return workerMetrics;
        }
        final OperatingRangeDataProvider dataProvider = optionalProvider.get();
        log.info("Worker has operating range metadata provider {} ", dataProvider);
        switch (dataProvider) {
            case LINUX_PROC:
                workerMetrics.add(new LinuxCpuWorkerMetric(DEFAULT_100_PERC_UTILIZED_OPERATING_RANGE));
                break;
            case LINUX_ECS_METADATA_KEY_V4:
                workerMetrics.add(new EcsCpuWorkerMetric(DEFAULT_100_PERC_UTILIZED_OPERATING_RANGE));
                break;
            case LINUX_EKS_CGROUP_V2:
                workerMetrics.add(new Cgroupv2CpuWorkerMetric(DEFAULT_100_PERC_UTILIZED_OPERATING_RANGE));
                break;
            case LINUX_EKS_CGROUP_V1:
                workerMetrics.add(new Cgroupv1CpuWorkerMetric(DEFAULT_100_PERC_UTILIZED_OPERATING_RANGE));
                break;
            default:
                break;
        }
        return workerMetrics;
    }
}
