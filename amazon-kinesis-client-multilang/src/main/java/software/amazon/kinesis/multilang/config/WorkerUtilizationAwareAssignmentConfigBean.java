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

package software.amazon.kinesis.multilang.config;

import java.time.Duration;

import lombok.Getter;
import lombok.Setter;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig;

@Getter
@Setter
public class WorkerUtilizationAwareAssignmentConfigBean {

    interface WorkerUtilizationAwareAssignmentConfigBeanDelegate {
        long getInMemoryWorkerMetricsCaptureFrequencyMillis();

        void setInMemoryWorkerMetricsCaptureFrequencyMillis(long value);

        long getWorkerMetricsReporterFreqInMillis();

        void setWorkerMetricsReporterFreqInMillis(long value);

        int getNoOfPersistedMetricsPerWorkerMetrics();

        void setNoOfPersistedMetricsPerWorkerMetrics(int value);

        Boolean getDisableWorkerMetrics();

        void setDisableWorkerMetrics(Boolean value);

        double getMaxThroughputPerHostKBps();

        void setMaxThroughputPerHostKBps(double value);

        int getDampeningPercentage();

        void setDampeningPercentage(int value);

        int getReBalanceThresholdPercentage();

        void setReBalanceThresholdPercentage(int value);

        Boolean getAllowThroughputOvershoot();

        void setAllowThroughputOvershoot(Boolean value);

        int getVarianceBalancingFrequency();

        void setVarianceBalancingFrequency(int value);

        double getWorkerMetricsEMAAlpha();

        void setWorkerMetricsEMAAlpha(double value);

        void setStaleWorkerMetricsEntryCleanupDuration(Duration value);

        Duration getStaleWorkerMetricsEntryCleanupDuration();
    }

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private long inMemoryWorkerMetricsCaptureFrequencyMillis;

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private long workerMetricsReporterFreqInMillis;

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private int noOfPersistedMetricsPerWorkerMetrics;

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private Boolean disableWorkerMetrics;

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private double maxThroughputPerHostKBps;

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private int dampeningPercentage;

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private int reBalanceThresholdPercentage;

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private Boolean allowThroughputOvershoot;

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private int varianceBalancingFrequency;

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private double workerMetricsEMAAlpha;

    @ConfigurationSettable(configurationClass = WorkerUtilizationAwareAssignmentConfig.class)
    private Duration staleWorkerMetricsEntryCleanupDuration;
}
