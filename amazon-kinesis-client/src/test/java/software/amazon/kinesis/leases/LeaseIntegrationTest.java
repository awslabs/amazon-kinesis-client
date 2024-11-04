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
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mock;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;

@Slf4j
public class LeaseIntegrationTest {
    protected LeaseSerializer leaseSerializer = new DynamoDBLeaseSerializer();

    protected static DynamoDBLeaseRefresher leaseRefresher;
    protected static DynamoDbAsyncClient ddbClient = DynamoDbAsyncClient.builder()
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    protected String tableName = "nagl_ShardProgress";

    @Mock
    protected TableCreatorCallback tableCreatorCallback;

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            if (leaseRefresher == null) {
                // Do some static setup once per class.

                leaseRefresher = getLeaseRefresher();
            }

            try {
                if (!leaseRefresher.leaseTableExists()) {
                    log.info("Creating lease table");
                    leaseRefresher.createLeaseTableIfNotExists(10L, 10L);

                    leaseRefresher.waitUntilLeaseTableExists(10, 500);
                }

                log.info("Beginning test case {}", description.getMethodName());
                for (Lease lease : leaseRefresher.listLeases()) {
                    leaseRefresher.deleteLease(lease);
                }
            } catch (Exception e) {
                String message =
                        "Test case " + description.getMethodName() + " fails because of exception during init: " + e;
                log.error(message);
                throw new RuntimeException(message, e);
            }
        }
    };

    protected DynamoDBLeaseRefresher getLeaseRefresher() {
        return new DynamoDBLeaseRefresher(
                tableName,
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
