/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.leases;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;

@Slf4j
public class LeaseIntegrationBillingModePayPerRequestTest extends LeaseIntegrationTest {
    @Override
    protected DynamoDBLeaseRefresher getLeaseRefresher() {
        return new DynamoDBLeaseRefresher(
                tableName + "Per-Request",
                ddbClient,
                leaseSerializer,
                true,
                tableCreatorCallback,
                LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
                new DdbTableConfig().billingMode(BillingMode.PAY_PER_REQUEST),
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
                DefaultSdkAutoConstructList.getInstance());
    }
}
