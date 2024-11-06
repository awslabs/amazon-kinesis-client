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

package software.amazon.kinesis.worker.metric.impl.container;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetric;
import software.amazon.kinesis.worker.metric.WorkerMetricType;

/**
 * Queries the Amazon ECS task metadata endpoint version 4 to get CPU metric stats as well as allocated CPU to the ECS task and
 * containers to calculate percent CPU utilization. This works for all ECS containers running on the following
 * platforms:
 *
 * Fargate agent version 1.4.0
 * EC2 instance running at least 1.39.0 of the Amazon ECS container agent
 *
 * For more information, see
 * https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4.html
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@KinesisClientInternalApi
public class EcsCpuWorkerMetric implements WorkerMetric {

    private static final WorkerMetricType CPU_WORKER_METRICS_TYPE = WorkerMetricType.CPU;
    private static final String SYS_VAR_ECS_METADATA_URI = "ECS_CONTAINER_METADATA_URI_V4";
    private final OperatingRange operatingRange;
    private final String containerStatsUri;
    private final String taskMetadataUri;
    private final String containerMetadataUri;
    private double containerCpuLimit = -1;
    private double onlineCpus = -1;

    public EcsCpuWorkerMetric(final OperatingRange operatingRange) {
        this.operatingRange = operatingRange;

        final String ecsMetadataRootUri = System.getenv(SYS_VAR_ECS_METADATA_URI);
        if (ecsMetadataRootUri != null) {
            this.containerStatsUri = ecsMetadataRootUri + "/stats";
            this.taskMetadataUri = ecsMetadataRootUri + "/task";
            this.containerMetadataUri = ecsMetadataRootUri;
        } else {
            this.containerStatsUri = null;
            this.taskMetadataUri = null;
            this.containerMetadataUri = null;
        }
    }

    @Override
    public String getShortName() {
        return CPU_WORKER_METRICS_TYPE.getShortName();
    }

    @Override
    public WorkerMetricValue capture() {
        return WorkerMetricValue.builder().value(calculateCpuUsage()).build();
    }

    private double calculateCpuUsage() {
        // Read current container metrics
        final JsonNode containerStatsRootNode = readEcsMetadata(containerStatsUri);

        final long cpuUsage = containerStatsRootNode
                .path("cpu_stats")
                .path("cpu_usage")
                .path("total_usage")
                .asLong();
        final long systemCpuUsage = containerStatsRootNode
                .path("cpu_stats")
                .path("system_cpu_usage")
                .asLong();
        final long prevCpuUsage = containerStatsRootNode
                .path("precpu_stats")
                .path("cpu_usage")
                .path("total_usage")
                .asLong();
        final long prevSystemCpuUsage = containerStatsRootNode
                .path("precpu_stats")
                .path("system_cpu_usage")
                .asLong();

        if (containerCpuLimit == -1 && onlineCpus == -1) {
            onlineCpus =
                    containerStatsRootNode.path("cpu_stats").path("online_cpus").asDouble();
            containerCpuLimit = calculateContainerCpuLimit(onlineCpus);
        }

        // precpu_stats values will be 0 if it is the first call
        if (prevCpuUsage == 0 && prevSystemCpuUsage == 0) {
            return 0D;
        }

        final long cpuUsageDiff = cpuUsage - prevCpuUsage;
        final long systemCpuUsageDiff = systemCpuUsage - prevSystemCpuUsage;

        // Edge case when there is no systemCpu usage, then that means that 100% of the cpu is used.
        if (systemCpuUsageDiff == 0) {
            return 100D;
        }

        // This value is not a percent, but rather how much CPU core time was consumed. i.e. this number can be
        // 2.2 which stands for 2.2 CPU cores were fully utilized. If this number is less than 1 than that means
        // that less than 1 CPU core was used.
        final double cpuCoreTimeUsed = ((double) cpuUsageDiff) / systemCpuUsageDiff * onlineCpus;

        // This calculated value is cpu utilization percent. This can burst past 100%, but we will take min with 100%
        // because only this amount is guaranteed CPU time to the container
        return Math.min(100.0, cpuCoreTimeUsed / containerCpuLimit * 100.0);
    }

    /**
     * All containers in an ECS task can use up to the task level CPU limit. However, CPU is shared among all containers
     * in the task according to the relative ratio of CPU shares allocated to each container.
     * i.e.
     * CPU limit of task is 8 cores
     * Container 1 with 10 CPU shares
     * Container 2 with 30 CPU shares
     * Sum of CPU shares is 40
     * Container 1 can use 25% of the 8 cores in CPU core time, so this function returns 2
     * Container 2 can use 75% of the 8 cores in CPU core time, so this function returns 6
     * @return the CPU core time allocated to the container
     */
    private double calculateContainerCpuLimit(double onlineCpus) {
        // Read task metadata
        final JsonNode taskStatsRootNode = readEcsMetadata(taskMetadataUri);
        double taskCpuLimit = calculateTaskCpuLimit(taskStatsRootNode, onlineCpus);

        // Read current container metadata
        final String currentContainerId =
                readEcsMetadata(containerMetadataUri).path("DockerId").asText();
        final Iterator<JsonNode> containersIterator =
                taskStatsRootNode.path("Containers").iterator();

        // The default if this value is not provided is 2 CPU shares (in ECS agent versions >= 1.2.0)
        int currentContainerCpuShare = 2;
        int containersCpuShareSum = 0;
        while (containersIterator.hasNext()) {
            final JsonNode containerNode = containersIterator.next();
            final int containerCpuShare =
                    containerNode.path("Limits").path("CPU").asInt();
            if (containerNode.path("DockerId").asText().equals(currentContainerId)) {
                currentContainerCpuShare = containerCpuShare;
            }
            containersCpuShareSum += containerCpuShare;
        }
        return ((double) currentContainerCpuShare) / containersCpuShareSum * taskCpuLimit;
    }

    private double calculateTaskCpuLimit(JsonNode taskStatsRootNode, double onlineCpus) {
        final JsonNode limitsNode = taskStatsRootNode.path("Limits");
        if (limitsNode.isMissingNode()) {
            // Neither a memory limit nor cpu limit is set at the task level (possible on EC2 instances)
            return onlineCpus;
        }
        final JsonNode cpuLimitsNode = limitsNode.path("CPU");
        if (cpuLimitsNode.isMissingNode()) {
            // When only a memory limit is set at the task level (possible on ec2 instances)
            return onlineCpus;
        }
        return cpuLimitsNode.asDouble();
    }

    private JsonNode readEcsMetadata(String uri) {
        if (this.containerMetadataUri == null) {
            throw new IllegalArgumentException("No ECS metadata endpoint found from environment variables.");
        }

        URL url;
        try {
            url = new URL(uri);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                    "CpuWorkerMetrics is not configured properly. ECS metadata url is malformed", e);
        }
        try {
            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode rootNode =
                    mapper.readValue(new InputStreamReader(url.openStream(), Charset.defaultCharset()), JsonNode.class);
            return rootNode;
        } catch (IOException e) {
            throw new IllegalArgumentException("Error in parsing ECS metadata", e);
        }
    }

    @Override
    public OperatingRange getOperatingRange() {
        return operatingRange;
    }

    @Override
    public WorkerMetricType getWorkerMetricType() {
        return CPU_WORKER_METRICS_TYPE;
    }
}
