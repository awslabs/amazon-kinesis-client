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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import software.amazon.kinesis.metrics.MetricsHelper;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import lombok.extern.slf4j.Slf4j;

@Ignore
@Slf4j
public class LeaseIntegrationTest {

    protected static KinesisClientDynamoDBLeaseManager leaseManager;
    protected static AmazonDynamoDBClient ddbClient =
            new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            if (leaseManager == null) {
                // Do some static setup once per class.

                leaseManager = new KinesisClientDynamoDBLeaseManager("nagl_ShardProgress", ddbClient, true);

                MetricsHelper.startScope(new NullMetricsFactory());
            }

            try {
                if (!leaseManager.leaseTableExists()) {
                    log.info("Creating lease table");
                    leaseManager.createLeaseTableIfNotExists(10L, 10L);

                    leaseManager.waitUntilLeaseTableExists(10, 500);
                }

                log.info("Beginning test case {}", description.getMethodName());
                for (KinesisClientLease lease : leaseManager.listLeases()) {
                    leaseManager.deleteLease(lease);
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
