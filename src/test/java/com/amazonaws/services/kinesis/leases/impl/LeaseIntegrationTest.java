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
package com.amazonaws.services.kinesis.leases.impl;

import java.util.logging.Logger;

import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;

@Ignore
public class LeaseIntegrationTest {

    protected static KinesisClientLeaseManager leaseManager;
    protected static AmazonDynamoDBClient ddbClient =
            new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());

    private static final Log LOG = LogFactory.getLog(LeaseIntegrationTest.class);

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            if (leaseManager == null) {
                // Do some static setup once per class.

                leaseManager = new KinesisClientLeaseManager("nagl_ShardProgress", ddbClient, true);

                MetricsHelper.startScope(new NullMetricsFactory());
            }

            try {
                if (!leaseManager.leaseTableExists()) {
                    LOG.info("Creating lease table");
                    leaseManager.createLeaseTableIfNotExists(10L, 10L);

                    leaseManager.waitUntilLeaseTableExists(10, 500);
                }

                LOG.info("Beginning test case " + description.getMethodName());
                for (KinesisClientLease lease : leaseManager.listLeases()) {
                    leaseManager.deleteLease(lease);
                }
            } catch (Exception e) {
                String message =
                        "Test case " + description.getMethodName() + " fails because of exception during init: " + e;
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
        }
    };

}
