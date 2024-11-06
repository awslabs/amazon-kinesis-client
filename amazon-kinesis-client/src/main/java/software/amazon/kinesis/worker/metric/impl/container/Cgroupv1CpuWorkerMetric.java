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

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetric;
import software.amazon.kinesis.worker.metric.WorkerMetricType;

import static software.amazon.kinesis.utils.Cgroup.getAvailableCpusFromEffectiveCpuSet;
import static software.amazon.kinesis.utils.Cgroup.readSingleLineFile;

/**
 * Utilizes Linux Control Groups by reading cpu time and available cpu from cgroup directory.This works for Elastic
 * Kubernetes Service (EKS) containers running on Linux instances which use cgroupv1.
 *
 * EC2 instances must use a Linux instance that uses cgroupv1. Amazon Linux 2 uses cgroupv1.
 * Fargate versions 1.4.0 and 1.3.0 use Amazon Linux 2 and can use this.
 *
 * CPU time is measured in CPU cores time. A container is limited by amount of CPU core time it is allocated. So if over
 * a second the container uses 0.5 CPU core time and is allocated 2 CPU cores, the cpu utilization would be 25%.
 *
 * When this is invoked for the first time, the value returned is always 0 as the prev values are not available
 * to calculate the diff.
 * In case the file is not present or any other exception occurs, this throws IllegalArgumentException.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@KinesisClientInternalApi
public class Cgroupv1CpuWorkerMetric implements WorkerMetric {

    private static final Object LOCK_OBJECT = new Object();
    private static final WorkerMetricType CPU_WORKER_METRICS_TYPE = WorkerMetricType.CPU;
    private static final String CGROUP_ROOT = "/sys/fs/cgroup/";
    private static final String CPU_TIME_FILE = CGROUP_ROOT + "cpu/cpuacct.usage";
    private static final String CPU_CFS_QUOTA_FILE = CGROUP_ROOT + "cpu/cpu.cfs_quota_us";
    private static final String CPU_CFS_PERIOD_FILE = CGROUP_ROOT + "cpu/cpu.cfs_period_us";
    private static final String EFFECTIVE_CPU_SET_FILE = CGROUP_ROOT + "cpuset/cpuset.effective_cpus";
    private final OperatingRange operatingRange;
    private final String cpuTimeFile;
    private final String cfsQuotaFile;
    private final String cfsPeriodFile;
    private final String effectiveCpuSetFile;
    private final Clock clock;
    private double cpuLimit = -1;
    private long lastCpuUseTimeNanos = 0;
    private long lastSystemTimeNanos = 0;

    public Cgroupv1CpuWorkerMetric(final OperatingRange operatingRange) {
        this(
                operatingRange,
                CPU_TIME_FILE,
                CPU_CFS_QUOTA_FILE,
                CPU_CFS_PERIOD_FILE,
                EFFECTIVE_CPU_SET_FILE,
                Clock.systemUTC());
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
        if (cpuLimit == -1) {
            cpuLimit = calculateCpuLimit();
        }

        final long cpuTimeNanos = Long.parseLong(readSingleLineFile(cpuTimeFile));
        final long currentTimeNanos = TimeUnit.MILLISECONDS.toNanos(clock.millis());

        boolean skip = false;
        double cpuCoreTimeUsed;
        synchronized (LOCK_OBJECT) {
            if (lastCpuUseTimeNanos == 0 && lastSystemTimeNanos == 0) {
                // Case where this is a first call so no diff available
                skip = true;
            }

            final long nanoTimeDiff = currentTimeNanos - lastSystemTimeNanos;
            final long cpuUseDiff = cpuTimeNanos - lastCpuUseTimeNanos;
            // This value is not a percent, but rather how much CPU core time was consumed. i.e. this number can be
            // 2.2 which stands for 2.2 CPU cores were fully utilized. If this number is less than 1 than that means
            // that less than 1 CPU core was used.
            cpuCoreTimeUsed = ((double) cpuUseDiff / nanoTimeDiff);

            lastCpuUseTimeNanos = cpuTimeNanos;
            lastSystemTimeNanos = currentTimeNanos;
        }

        if (skip) {
            return 0D;
        } else {
            // In case of rounding error, treat everything above 100% as 100%
            return Math.min(100.0, cpuCoreTimeUsed / cpuLimit * 100.0);
        }
    }

    private double calculateCpuLimit() {
        // Documentation on these values:
        // https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/6/html/resource_management_guide/sec-cpu#sect-cfs
        final long cfsQuota = Long.parseLong(readSingleLineFile(cfsQuotaFile));
        final long cfsPeriod = Long.parseLong(readSingleLineFile(cfsPeriodFile));
        if (cfsQuota == -1) {
            // If quota is -1, a limit is not set on the container. The container can use all available cores.
            return getAvailableCpusFromEffectiveCpuSet(readSingleLineFile(effectiveCpuSetFile));
        } else {
            return ((double) cfsQuota) / cfsPeriod;
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
