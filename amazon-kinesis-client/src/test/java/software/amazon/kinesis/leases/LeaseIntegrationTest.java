/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License. 
 */
package software.amazon.kinesis.leases;

import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mock;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;

@Slf4j
public class LeaseIntegrationTest {
    private LeaseSerializer leaseSerializer = new DynamoDBLeaseSerializer();

    protected static DynamoDBLeaseRefresher leaseRefresher;
    protected static DynamoDbAsyncClient ddbClient = DynamoDbAsyncClient.builder()
            .credentialsProvider(DefaultCredentialsProvider.create()).build();

    protected String tableName = "nagl_ShardProgress";

    @Mock
    protected TableCreatorCallback tableCreatorCallback;

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            if (leaseRefresher == null) {
                // Do some static setup once per class.

                leaseRefresher = new DynamoDBLeaseRefresher(tableName, ddbClient, leaseSerializer, true,
                        tableCreatorCallback);
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

}

