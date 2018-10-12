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

//import java.net.URI;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

//import software.amazon.awssdk.core.client.builder.ClientAsyncHttpConfiguration;
//import software.amazon.awssdk.http.nio.netty.NettySdkHttpClientFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;

/**
 * WARN: to run this integration test you'll have to provide a AwsCredentials.properties file on the classpath.
 */
// TODO: fix tests
@Ignore
public class ShardSyncTaskIntegrationTest {
    private static final String STREAM_NAME = "IntegrationTestStream02";
    private static final boolean USE_CONSISTENT_READS = true;
    private static final int MAX_CACHE_MISSES_BEFORE_RELOAD = 1000;
    private static final long LIST_SHARDS_CACHE_ALLOWED_AGE_IN_SECONDS = 30;
    private static final int CACHE_MISS_WARNING_MODULUS = 250;
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();
    private static KinesisAsyncClient kinesisClient;

    private LeaseRefresher leaseRefresher;
    private ShardDetector shardDetector;
    private HierarchicalShardSyncer hierarchicalShardSyncer;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
//        ClientAsyncHttpConfiguration configuration = ClientAsyncHttpConfiguration.builder().httpClientFactory(
//                NettySdkHttpClientFactory.builder().trustAllCertificates(true).maxConnectionsPerEndpoint(10).build())
//                .build();
//        kinesisClient = KinesisAsyncClient.builder().asyncHttpConfiguration(configuration)
//                .endpointOverride(new URI("https://aws-kinesis-alpha.corp.amazon.com")).region(Region.US_EAST_1)
//                .build();
//
        try {
            CreateStreamRequest req = CreateStreamRequest.builder().streamName(STREAM_NAME).shardCount(1).build();
            kinesisClient.createStream(req);
        } catch (KinesisException ase) {
            ase.printStackTrace();
        }
        StreamStatus status;
//        do {
//            status = StreamStatus.fromValue(kinesisClient.describeStreamSummary(
//                    DescribeStreamSummaryRequest.builder().streamName(STREAM_NAME).build()).get()
//                    .streamDescriptionSummary().streamStatusString());
//        } while (status != StreamStatus.ACTIVE);
//
    }

    @Before
    public void setup() {
        DynamoDbAsyncClient client = DynamoDbAsyncClient.builder().region(Region.US_EAST_1).build();
        leaseRefresher =
                new DynamoDBLeaseRefresher("ShardSyncTaskIntegrationTest", client, new DynamoDBLeaseSerializer(),
                        USE_CONSISTENT_READS, TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK);

        shardDetector = new KinesisShardDetector(kinesisClient, STREAM_NAME, 500L, 50,
                LIST_SHARDS_CACHE_ALLOWED_AGE_IN_SECONDS, MAX_CACHE_MISSES_BEFORE_RELOAD, CACHE_MISS_WARNING_MODULUS);
        hierarchicalShardSyncer = new HierarchicalShardSyncer();
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
        if (!leaseRefresher.leaseTableExists()) {
            final Long readCapacity = 10L;
            final Long writeCapacity = 10L;
            leaseRefresher.createLeaseTableIfNotExists(readCapacity, writeCapacity);
        }
        leaseRefresher.deleteAll();
        Set<String> shardIds = shardDetector.listShards().stream().map(Shard::shardId).collect(Collectors.toSet());
        ShardSyncTask syncTask = new ShardSyncTask(shardDetector, leaseRefresher,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST), false, false, 0L,
                hierarchicalShardSyncer, NULL_METRICS_FACTORY);
        syncTask.call();
        List<Lease> leases = leaseRefresher.listLeases();
        Set<String> leaseKeys = new HashSet<>();
        for (Lease lease : leases) {
            leaseKeys.add(lease.leaseKey());
        }

        // Verify that all shardIds had leases for them
        Assert.assertEquals(shardIds.size(), leases.size());
        shardIds.removeAll(leaseKeys);
        Assert.assertTrue(shardIds.isEmpty());
    }

}
