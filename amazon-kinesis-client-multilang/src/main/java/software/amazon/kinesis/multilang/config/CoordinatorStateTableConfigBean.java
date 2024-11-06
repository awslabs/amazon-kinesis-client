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
import software.amazon.kinesis.coordinator.CoordinatorConfig.CoordinatorStateTableConfig;
import software.amazon.kinesis.multilang.config.converter.TagConverter.TagCollection;

@Getter
@Setter
public class CoordinatorStateTableConfigBean {

    interface CoordinatorStateConfigBeanDelegate {
        String getCoordinatorStateTableName();

        void setCoordinatorStateTableName(String value);

        BillingMode getCoordinatorStateBillingMode();

        void setCoordinatorStateBillingMode(BillingMode value);

        long getCoordinatorStateReadCapacity();

        void setCoordinatorStateReadCapacity(long value);

        long getCoordinatorStateWriteCapacity();

        void setCoordinatorStateWriteCapacity(long value);

        Boolean getCoordinatorStatePointInTimeRecoveryEnabled();

        void setCoordinatorStatePointInTimeRecoveryEnabled(Boolean value);

        Boolean getCoordinatorStateDeletionProtectionEnabled();

        void setCoordinatorStateDeletionProtectionEnabled(Boolean value);

        TagCollection getCoordinatorStateTags();

        void setCoordinatorStateTags(TagCollection value);
    }

    @ConfigurationSettable(configurationClass = CoordinatorStateTableConfig.class, methodName = "tableName")
    private String coordinatorStateTableName;

    @ConfigurationSettable(configurationClass = CoordinatorStateTableConfig.class, methodName = "billingMode")
    private BillingMode coordinatorStateBillingMode;

    @ConfigurationSettable(configurationClass = CoordinatorStateTableConfig.class, methodName = "readCapacity")
    private long coordinatorStateReadCapacity;

    @ConfigurationSettable(configurationClass = CoordinatorStateTableConfig.class, methodName = "writeCapacity")
    private long coordinatorStateWriteCapacity;

    @ConfigurationSettable(
            configurationClass = CoordinatorStateTableConfig.class,
            methodName = "pointInTimeRecoveryEnabled")
    private Boolean coordinatorStatePointInTimeRecoveryEnabled;

    @ConfigurationSettable(
            configurationClass = CoordinatorStateTableConfig.class,
            methodName = "deletionProtectionEnabled")
    private Boolean coordinatorStateDeletionProtectionEnabled;

    @ConfigurationSettable(configurationClass = CoordinatorStateTableConfig.class, methodName = "tags")
    private TagCollection coordinatorStateTags;
}
