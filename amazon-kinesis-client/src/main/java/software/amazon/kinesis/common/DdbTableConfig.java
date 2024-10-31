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
package software.amazon.kinesis.common;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;

/**
 * Configurations of a DDB table created by KCL for its internal operations.
 */
@Data
@Accessors(fluent = true)
@NoArgsConstructor
public class DdbTableConfig {

    protected DdbTableConfig(final String applicationName, final String tableSuffix) {
        this.tableName = applicationName + "-" + tableSuffix;
    }

    /**
     * name to use for the DDB table. If null, it will default to
     * applicationName-tableSuffix. If multiple KCL applications
     * run in the same account, a unique tableName must be provided.
     */
    private String tableName;

    /**
     * Billing mode used to create the DDB table.
     */
    private BillingMode billingMode = BillingMode.PAY_PER_REQUEST;

    /**
     * read capacity to provision during DDB table creation,
     * if billing mode is PROVISIONED.
     */
    private long readCapacity;

    /**
     * write capacity to provision during DDB table creation,
     * if billing mode is PROVISIONED.
     */
    private long writeCapacity;
}
