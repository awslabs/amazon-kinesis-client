/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
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

/**
 * WARN: to run this integration test you'll have to provide a AwsCredentials.properties file on the classpath.
 */
public class ShardSyncTaskIntegrationTest {

    private static final String STREAM_NAME = "IntegrationTestStream02";
    private static final String KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";

    private static AWSCredentialsProvider credentialsProvider;
    private IKinesisClientLeaseManager leaseManager;
    private IKinesisProxy kinesisProxy;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        AmazonKinesis kinesis = new AmazonKinesisClient(credentialsProvider);

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
    @Before
    public void setUp() throws Exception {
        boolean useConsistentReads = true;
        leaseManager =
                new KinesisClientLeaseManager("ShardSyncTaskIntegrationTest",
                        new AmazonDynamoDBClient(credentialsProvider),
                        useConsistentReads);

        kinesisProxy =
                new KinesisProxy(STREAM_NAME,
                        new DefaultAWSCredentialsProviderChain(),
                        KINESIS_ENDPOINT);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for call().
     * 
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     */
    @Test
    public final void testCall() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (!leaseManager.leaseTableExists()) {
            final Long readCapacity = 10L;
            final Long writeCapacity = 10L;
            leaseManager.createLeaseTableIfNotExists(readCapacity, writeCapacity);
        }
        leaseManager.deleteAll();
        Set<String> shardIds = kinesisProxy.getAllShardIds();
        ShardSyncTask syncTask = new ShardSyncTask(kinesisProxy,
                leaseManager,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST),
                false,
                false,
                0L);
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
