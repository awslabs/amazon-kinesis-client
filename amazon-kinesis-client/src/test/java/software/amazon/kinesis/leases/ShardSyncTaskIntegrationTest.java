/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamStatus;

import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 * WARN: to run this integration test you'll have to provide a AwsCredentials.properties file on the classpath.
 */
public class ShardSyncTaskIntegrationTest {
    private static final String STREAM_NAME = "IntegrationTestStream02";
    private static AmazonKinesis amazonKinesis;

    private KinesisClientLeaseManager leaseManager;
    private LeaseManagerProxy leaseManagerProxy;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        amazonKinesis = AmazonKinesisClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

        try {
            amazonKinesis.createStream(STREAM_NAME, 1);
        } catch (AmazonServiceException ase) {

        }
        StreamStatus status;
        do {
            status = StreamStatus.fromValue(amazonKinesis.describeStreamSummary(
                    new DescribeStreamSummaryRequest().withStreamName(STREAM_NAME))
                    .getStreamDescriptionSummary().getStreamStatus());
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
                new KinesisClientDynamoDBLeaseManager("ShardSyncTaskIntegrationTest",
                        AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build(),
                        useConsistentReads);

        leaseManagerProxy = new KinesisLeaseManagerProxy(amazonKinesis, STREAM_NAME, 500L, 50);
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
        Set<String> shardIds = leaseManagerProxy.listShards().stream().map(Shard::getShardId).collect(Collectors.toSet());
        ShardSyncTask syncTask = new ShardSyncTask(leaseManagerProxy,
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
