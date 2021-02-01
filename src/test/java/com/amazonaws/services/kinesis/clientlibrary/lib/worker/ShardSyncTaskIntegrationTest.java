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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.leases.interfaces.IKinesisClientLeaseManager;
import com.amazonaws.services.kinesis.model.StreamStatus;

import static junit.framework.TestCase.fail;

/**
 * WARN: to run this integration test you'll have to provide a AwsCredentials.properties file on the classpath.
 */
public class ShardSyncTaskIntegrationTest {

    private static final String STREAM_NAME = "IntegrationTestStream02";
    private static final String KINESIS_ENDPOINT = "http://localhost:4566";

    private static AWSCredentialsProvider credentialsProvider;
    private IKinesisClientLeaseManager leaseManager;
    private IKinesisProxy kinesisProxy;
    private final KinesisShardSyncer shardSyncer = new KinesisShardSyncer(new KinesisLeaseCleanupValidator());

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        System.setProperty("com.amazonaws.sdk.disableCbor", "true");

        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566","us-east-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secretKey")))
                .build();

        try {
            kinesis.createStream(STREAM_NAME, 1);
        } catch (AmazonServiceException ase) {

        }
        StreamStatus status;
        do {
            status = StreamStatus.fromValue(kinesis.describeStream(STREAM_NAME).getStreamDescription().getStreamStatus());
        } while (status != StreamStatus.ACTIVE);

    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    public void setUp(BillingMode billingMode, String tableName) throws Exception {
        boolean useConsistentReads = true;
        leaseManager =
                new KinesisClientLeaseManager(tableName,
                        AmazonDynamoDBClientBuilder.standard()
                                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-east-1"))
                                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secretKey")))
                                .build(),
                        useConsistentReads,
                        billingMode);

        kinesisProxy =
                new KinesisProxy(STREAM_NAME,
                        new DefaultAWSCredentialsProviderChain(),
                        KINESIS_ENDPOINT);
    }

    /**
     * Test method for call().
     *
     * @throws Exception
     */
    @Test
    public final void testCall_ProvisionedDDB() throws Exception {
        BillingMode billingMode = BillingMode.PROVISIONED;
        String tableName = "ShardSyncTaskIntegrationTest" + billingMode.name();
        try {
            setUp(billingMode, tableName);
            runTest();
            checkBillingMode(billingMode, tableName);
        }
        finally {
            cleanUpTable(tableName);
        }
    }

    /**
     * Test method for call().
     *
     * @throws Exception
     */
    @Test
    public final void testCall_PayPerRequestDDB() throws Exception {
        BillingMode billingMode = BillingMode.PAY_PER_REQUEST;
        String tableName = "ShardSyncTaskIntegrationTest" + billingMode.name();
        try {
            setUp(billingMode, tableName);
            runTest();
            checkBillingMode(billingMode, tableName);
        } finally {
            cleanUpTable(tableName);
        }
    }

    private void cleanUpTable(String tableName) throws DependencyException {
        AmazonDynamoDBClient client = (AmazonDynamoDBClient) AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566","us-east-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secretKey")))
                .build();
        ListTablesResult tables = client.listTables();
        if(tables.getTableNames().contains(tableName)){
            leaseManager.waitUntilLeaseTableExists(2,20);
            client.deleteTable(tableName);
            DateTime endTime = DateTime.now().plusSeconds(30);
            while(client.listTables().getTableNames().contains(tableName)){
                if( endTime.isBeforeNow()){
                    fail("Could not clean up DDB tables in time. Please retry. If these failures continue increase the endTime.");
                }
                try {
                    Thread.sleep(333L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void checkBillingMode(BillingMode billingMode, String tableName) {
        AmazonDynamoDBClient client = (AmazonDynamoDBClient) AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566","us-east-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secretKey")))
                .build();
        DescribeTableResult tableDetails = client.describeTable(tableName);
        if(BillingMode.PAY_PER_REQUEST.equals(billingMode)) {
            Assert.assertEquals(tableDetails.getTable().getBillingModeSummary().getBillingMode(), billingMode.name());
        }else{
            Assert.assertTrue(tableDetails.getTable().getProvisionedThroughput().getWriteCapacityUnits() == 10);
            Assert.assertTrue(tableDetails.getTable().getProvisionedThroughput().getReadCapacityUnits() == 10);
        }
    }

    public void runTest() throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        if (!leaseManager.leaseTableExists()) {
            final Long readCapacity = 10L;
            final Long writeCapacity = 10L;
            leaseManager.createLeaseTableIfNotExists(readCapacity, writeCapacity);
            leaseManager.waitUntilLeaseTableExists(2,20);
        }
        leaseManager.deleteAll();
        Set<String> shardIds = kinesisProxy.getAllShardIds();
        ShardSyncTask syncTask = new ShardSyncTask(kinesisProxy,
                leaseManager,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST),
                false,
                false,
                0L,
                shardSyncer,
                null);
        syncTask.call();
        List<KinesisClientLease> leases = leaseManager.listLeases();
        Set<String> leaseKeys = new HashSet<String>();
        for (KinesisClientLease lease : leases) {
            leaseKeys.add(lease.getLeaseKey());
        }

        // Verify that all shardIds had leases for them
        Assert.assertEquals(shardIds.size(), leases.size());
        shardIds.removeAll(leaseKeys);
        Assert.assertTrue(shardIds.isEmpty());
    }

}
