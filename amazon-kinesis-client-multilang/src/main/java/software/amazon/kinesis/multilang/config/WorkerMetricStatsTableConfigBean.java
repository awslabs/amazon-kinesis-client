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

import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;
import software.amazon.kinesis.multilang.config.converter.TagConverter.TagCollection;

@Getter
@Setter
public class WorkerMetricStatsTableConfigBean {

    interface WorkerMetricsTableConfigBeanDelegate {
        String getWorkerMetricsTableName();

        void setWorkerMetricsTableName(String value);

        BillingMode getWorkerMetricsBillingMode();

        void setWorkerMetricsBillingMode(BillingMode value);

        long getWorkerMetricsReadCapacity();

        void setWorkerMetricsReadCapacity(long value);

        long getWorkerMetricsWriteCapacity();

        void setWorkerMetricsWriteCapacity(long value);

        Boolean getWorkerMetricsPointInTimeRecoveryEnabled();

        void setWorkerMetricsPointInTimeRecoveryEnabled(Boolean value);

        Boolean getWorkerMetricsDeletionProtectionEnabled();

        void setWorkerMetricsDeletionProtectionEnabled(Boolean value);

        TagCollection getWorkerMetricsTags();

        void setWorkerMetricsTags(TagCollection value);
    }

    @ConfigurationSettable(configurationClass = WorkerMetricsTableConfig.class, methodName = "tableName")
    private String workerMetricsTableName;

    @ConfigurationSettable(configurationClass = WorkerMetricsTableConfig.class, methodName = "billingMode")
    private BillingMode workerMetricsBillingMode;

    @ConfigurationSettable(configurationClass = WorkerMetricsTableConfig.class, methodName = "readCapacity")
    private long workerMetricsReadCapacity;

    @ConfigurationSettable(configurationClass = WorkerMetricsTableConfig.class, methodName = "writeCapacity")
    private long workerMetricsWriteCapacity;

    @ConfigurationSettable(
            configurationClass = WorkerMetricsTableConfig.class,
            methodName = "pointInTimeRecoveryEnabled")
    private Boolean workerMetricsPointInTimeRecoveryEnabled;

    @ConfigurationSettable(
            configurationClass = WorkerMetricsTableConfig.class,
            methodName = "deletionProtectionEnabled")
    private Boolean workerMetricsDeletionProtectionEnabled;

    @ConfigurationSettable(configurationClass = WorkerMetricsTableConfig.class, methodName = "tags")
    private TagCollection workerMetricsTags;
}
