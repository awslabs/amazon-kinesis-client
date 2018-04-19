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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.KinesisClientLibIOException;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;

import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.leases.ExceptionThrowingLeaseManager.ExceptionThrowingLeaseManagerMethods;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 *
 */
// CHECKSTYLE:IGNORE JavaNCSS FOR NEXT 800 LINES
@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class ShardSyncerTest {
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_AT_TIMESTAMP =
            InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(1000L));
    private final boolean cleanupLeasesOfCompletedShards = true;
    private AmazonDynamoDB ddbClient = DynamoDBEmbedded.create().amazonDynamoDB();
    private DynamoDBLeaseManager<KinesisClientLease> dynamoDBLeaseManager = new KinesisClientDynamoDBLeaseManager("tempTestTable", ddbClient);
    private static final int EXPONENT = 128;
    /**
     * Old/Obsolete max value of a sequence number (2^128 -1).
     */
    public static final BigInteger MAX_SEQUENCE_NUMBER = new BigInteger("2").pow(EXPONENT).subtract(BigInteger.ONE);

    @Mock
    private LeaseManagerProxy leaseManagerProxy;

    @Before
    public void setUp() throws Exception {
        boolean created = dynamoDBLeaseManager.createLeaseTableIfNotExists(1L, 1L);
        if (created) {
            log.info("New table created.");
        }
        dynamoDBLeaseManager.deleteAll();
    }

    @After
    public void tearDown() throws Exception {
        dynamoDBLeaseManager.deleteAll();
    }

    /**
     * Test determineNewLeasesToCreate() where there are no shards
     */
    @Test
    public final void testDetermineNewLeasesToCreateNoShards() {
        List<Shard> shards = new ArrayList<>();
        List<KinesisClientLease> leases = new ArrayList<>();

        assertTrue(ShardSyncer.determineNewLeasesToCreate(shards, leases, INITIAL_POSITION_LATEST).isEmpty());
    }

    /**
     * Test determineNewLeasesToCreate() where there are no leases and no resharding operations have been performed
     */
    @Test
    public final void testDetermineNewLeasesToCreate0Leases0Reshards() {
        List<Shard> shards = new ArrayList<>();
        List<KinesisClientLease> currentLeases = new ArrayList<>();
        SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        String shardId0 = "shardId-0";
        shards.add(ShardObjectHelper.newShard(shardId0, null, null, sequenceRange));

        String shardId1 = "shardId-1";
        shards.add(ShardObjectHelper.newShard(shardId1, null, null, sequenceRange));

        List<KinesisClientLease> newLeases =
                ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_LATEST);
        assertEquals(2, newLeases.size());
        Set<String> expectedLeaseShardIds = new HashSet<>();
        expectedLeaseShardIds.add(shardId0);
        expectedLeaseShardIds.add(shardId1);
        for (KinesisClientLease lease : newLeases) {
            assertTrue(expectedLeaseShardIds.contains(lease.getLeaseKey()));
        }
    }

    /**
     * Test determineNewLeasesToCreate() where there are no leases and no resharding operations have been performed, but one of
     * the shards was marked as inconsistent.
     */
    @Test
    public final void testDetermineNewLeasesToCreate0Leases0Reshards1Inconsistent() {
        List<Shard> shards = new ArrayList<>();
        List<KinesisClientLease> currentLeases = new ArrayList<>();
        SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        String shardId0 = "shardId-0";
        shards.add(ShardObjectHelper.newShard(shardId0, null, null, sequenceRange));

        String shardId1 = "shardId-1";
        shards.add(ShardObjectHelper.newShard(shardId1, null, null, sequenceRange));

        String shardId2 = "shardId-2";
        shards.add(ShardObjectHelper.newShard(shardId2, shardId1, null, sequenceRange));

        Set<String> inconsistentShardIds = new HashSet<>();
        inconsistentShardIds.add(shardId2);

        List<KinesisClientLease> newLeases =
            ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_LATEST, inconsistentShardIds);
        assertEquals(2, newLeases.size());
        Set<String> expectedLeaseShardIds = new HashSet<>();
        expectedLeaseShardIds.add(shardId0);
        expectedLeaseShardIds.add(shardId1);
        for (KinesisClientLease lease : newLeases) {
            assertTrue(expectedLeaseShardIds.contains(lease.getLeaseKey()));
        }
    }

    /**
     * Test bootstrapShardLeases() starting at TRIM_HORIZON ("beginning" of stream)
     * 
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     * @throws IOException
     * @throws KinesisClientLibIOException
     */
    @Test
    public final void testBootstrapShardLeasesAtTrimHorizon()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException, IOException,
        KinesisClientLibIOException {
        testBootstrapShardLeasesAtStartingPosition(INITIAL_POSITION_TRIM_HORIZON);
    }

    /**
     * Test bootstrapShardLeases() starting at LATEST (tip of stream)
     * 
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     * @throws IOException
     * @throws KinesisClientLibIOException
     */
    @Test
    public final void testBootstrapShardLeasesAtLatest()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException, IOException,
        KinesisClientLibIOException {
        testBootstrapShardLeasesAtStartingPosition(INITIAL_POSITION_LATEST);
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtLatest()
        throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
        IOException {
        List<Shard> shards = constructShardListForGraphA();

        when(leaseManagerProxy.listShards()).thenReturn(shards);

        ShardSyncer.checkAndCreateLeasesForNewShards(leaseManagerProxy, dynamoDBLeaseManager, INITIAL_POSITION_LATEST,
                cleanupLeasesOfCompletedShards);
        List<KinesisClientLease> newLeases = dynamoDBLeaseManager.listLeases();
        Set<String> expectedLeaseShardIds = new HashSet<>();
        expectedLeaseShardIds.add("shardId-4");
        expectedLeaseShardIds.add("shardId-8");
        expectedLeaseShardIds.add("shardId-9");
        expectedLeaseShardIds.add("shardId-10");
        assertEquals(expectedLeaseShardIds.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            assertEquals(ExtendedSequenceNumber.LATEST, lease1.getCheckpoint());
        }
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    // TODO: Remove @Ignore once build is fixed
    @Ignore
    public final void testCheckAndCreateLeasesForNewShardsAtTrimHorizon()
        throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
        IOException {
        List<Shard> shards = constructShardListForGraphA();

        when(leaseManagerProxy.listShards()).thenReturn(shards);

        ShardSyncer.checkAndCreateLeasesForNewShards(leaseManagerProxy, dynamoDBLeaseManager, INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards);
        List<KinesisClientLease> newLeases = dynamoDBLeaseManager.listLeases();
        Set<String> expectedLeaseShardIds = new HashSet<>();
        for (int i = 0; i < 11; i++) {
            expectedLeaseShardIds.add("shardId-" + i);
        }
        assertEquals(expectedLeaseShardIds.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            assertEquals(ExtendedSequenceNumber.TRIM_HORIZON, lease1.getCheckpoint());
        }
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtTimestamp()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException,
            ProvisionedThroughputException, IOException {
        List<Shard> shards = constructShardListForGraphA();

        when(leaseManagerProxy.listShards()).thenReturn(shards);

        ShardSyncer.checkAndCreateLeasesForNewShards(leaseManagerProxy, dynamoDBLeaseManager, INITIAL_POSITION_AT_TIMESTAMP,
                cleanupLeasesOfCompletedShards);
        List<KinesisClientLease> newLeases = dynamoDBLeaseManager.listLeases();
        Set<String> expectedLeaseShardIds = new HashSet<>();
        for (int i = 0; i < 11; i++) {
            expectedLeaseShardIds.add("shardId-" + i);
        }
        assertEquals(expectedLeaseShardIds.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            assertEquals(ExtendedSequenceNumber.AT_TIMESTAMP, lease1.getCheckpoint());
        }
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test(expected = KinesisClientLibIOException.class)
    public final void testCheckAndCreateLeasesForNewShardsWhenParentIsOpen()
        throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
        IOException {
        List<Shard> shards = constructShardListForGraphA();
        SequenceNumberRange range = shards.get(0).getSequenceNumberRange();
        range.setEndingSequenceNumber(null);
        shards.get(3).setSequenceNumberRange(range);

        when(leaseManagerProxy.listShards()).thenReturn(shards);

        ShardSyncer.checkAndCreateLeasesForNewShards(leaseManagerProxy, dynamoDBLeaseManager, INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards);
    }

    /**
     * Test checkAndCreateLeasesForNewShards() when a parent is open and children of open parents are being ignored.
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsWhenParentIsOpenAndIgnoringInconsistentChildren()
        throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
        IOException {
        List<Shard> shards = constructShardListForGraphA();
        Shard shard = shards.get(5);
        assertEquals("shardId-5", shard.getShardId());
        SequenceNumberRange range = shard.getSequenceNumberRange();
        // shardId-5 in graph A has two children (shardId-9 and shardId-10).  if shardId-5
        // is not closed, those children should be ignored when syncing shards, no leases
        // should be obtained for them, and we should obtain a lease on the still-open
        // parent.
        range.setEndingSequenceNumber(null);
        shard.setSequenceNumberRange(range);

        when(leaseManagerProxy.listShards()).thenReturn(shards);

        ShardSyncer.checkAndCreateLeasesForNewShards(leaseManagerProxy, dynamoDBLeaseManager, INITIAL_POSITION_LATEST,
                                                     cleanupLeasesOfCompletedShards, true);
        List<KinesisClientLease> newLeases = dynamoDBLeaseManager.listLeases();
        Set<String> expectedLeaseShardIds = new HashSet<>();
        expectedLeaseShardIds.add("shardId-4");
        expectedLeaseShardIds.add("shardId-5");
        expectedLeaseShardIds.add("shardId-8");
        assertEquals(expectedLeaseShardIds.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            assertEquals(ExtendedSequenceNumber.LATEST, lease1.getCheckpoint());
        }
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtTrimHorizonAndClosedShard()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException,
            ProvisionedThroughputException, IOException {
        testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(null,
                Integer.MAX_VALUE, INITIAL_POSITION_TRIM_HORIZON);
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    // TODO: Remove @Ignore once build is fixed
    @Ignore
    public final void testCheckAndCreateLeasesForNewShardsAtTrimHorizonAndClosedShardWithDeleteLeaseExceptions()
        throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
        IOException {
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 10;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.DELETELEASE, c, INITIAL_POSITION_TRIM_HORIZON);
            // Need to clean up lease manager every time after calling ShardSyncer
            dynamoDBLeaseManager.deleteAll();
        }
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    // TODO: Remove @Ignore once build is fixed
    @Ignore
    public final void testCheckAndCreateLeasesForNewShardsAtTrimHorizonAndClosedShardWithListLeasesExceptions()
        throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
        IOException {
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 10;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.LISTLEASES, c, INITIAL_POSITION_TRIM_HORIZON);
            // Need to clean up lease manager every time after calling ShardSyncer
            dynamoDBLeaseManager.deleteAll();
        }
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    // TODO: Remove @Ignore once build is fixed
    @Ignore
    public final void testCheckAndCreateLeasesForNewShardsAtTrimHorizonAndClosedShardWithCreateLeaseExceptions()
        throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
        IOException {
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 5;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.CREATELEASEIFNOTEXISTS, c,INITIAL_POSITION_TRIM_HORIZON);
            // Need to clean up lease manager every time after calling ShardSyncer
            dynamoDBLeaseManager.deleteAll();
        }
    }

    // Try catch leaseException for different lease manager methods and eventually let it succeed.
    // This would not throw any exceptions if:
    // 1). exceptionMethod equals to null or NONE.
    // 2). exceptionTime is a very big or negative value.
    private void retryCheckAndCreateLeaseForNewShards(ExceptionThrowingLeaseManagerMethods exceptionMethod,
            int exceptionTime, InitialPositionInStreamExtended position)
        throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (exceptionMethod != null) {
            ExceptionThrowingLeaseManager exceptionThrowingLeaseManager =
                    new ExceptionThrowingLeaseManager(dynamoDBLeaseManager);
            // Set exception and throwing time for exceptionThrowingManager.
            exceptionThrowingLeaseManager.setLeaseLeaseManagerThrowingExceptionScenario(exceptionMethod, exceptionTime);
            // Only need to try two times.
            for (int i = 1; i <= 2; i++) {
                try {
                    ShardSyncer.checkAndCreateLeasesForNewShards(leaseManagerProxy,
                            exceptionThrowingLeaseManager,
                            position,
                            cleanupLeasesOfCompletedShards);
                    return;
                } catch (LeasingException e) {
                    log.debug("Catch leasing exception", e);
                }
                // Clear throwing exception scenario every time after calling ShardSyncer
                exceptionThrowingLeaseManager.clearLeaseManagerThrowingExceptionScenario();
            }
        } else {
            ShardSyncer.checkAndCreateLeasesForNewShards(leaseManagerProxy,
                    dynamoDBLeaseManager,
                    position,
                    cleanupLeasesOfCompletedShards);
        }
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtTimestampAndClosedShard()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException,
            ProvisionedThroughputException, IOException {
        testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(null,
                Integer.MAX_VALUE, INITIAL_POSITION_AT_TIMESTAMP);
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    // TODO: Remove @Ignore once build is fixed
    @Ignore
    public final void testCheckAndCreateLeasesForNewShardsAtTimestampAndClosedShardWithDeleteLeaseExceptions()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 10;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.DELETELEASE,
                    c, INITIAL_POSITION_AT_TIMESTAMP);
            // Need to clean up lease manager every time after calling ShardSyncer
            dynamoDBLeaseManager.deleteAll();
        }
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtTimestampAndClosedShardWithListLeasesExceptions()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 10;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.LISTLEASES,
                    c, INITIAL_POSITION_AT_TIMESTAMP);
            // Need to clean up lease manager every time after calling ShardSyncer
            dynamoDBLeaseManager.deleteAll();
        }
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtTimestampAndClosedShardWithCreateLeaseExceptions()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 5;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.CREATELEASEIFNOTEXISTS,
                    c, INITIAL_POSITION_AT_TIMESTAMP);
            // Need to clean up lease manager every time after calling ShardSyncer
            dynamoDBLeaseManager.deleteAll();
        }
    }

    // Real implementation of testing CheckAndCreateLeasesForNewShards with different leaseManager types.
    private void testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
            ExceptionThrowingLeaseManagerMethods exceptionMethod,
            int exceptionTime,
            InitialPositionInStreamExtended position)
        throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
        IOException {
        ExtendedSequenceNumber extendedSequenceNumber =
                new ExtendedSequenceNumber(position.getInitialPositionInStream().toString());
        List<Shard> shards = constructShardListForGraphA();

        when(leaseManagerProxy.listShards()).thenReturn(shards);

        retryCheckAndCreateLeaseForNewShards(exceptionMethod, exceptionTime, position);

        List<KinesisClientLease> newLeases = dynamoDBLeaseManager.listLeases();
        Map<String, ExtendedSequenceNumber> expectedShardIdToCheckpointMap = new HashMap<>();
        for (int i = 0; i < 11; i++) {
            expectedShardIdToCheckpointMap.put("shardId-" + i, extendedSequenceNumber);
        }
        assertEquals(expectedShardIdToCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            ExtendedSequenceNumber expectedCheckpoint = expectedShardIdToCheckpointMap.get(lease1.getLeaseKey());
            assertNotNull(expectedCheckpoint);
            assertEquals(expectedCheckpoint, lease1.getCheckpoint());
        }

        KinesisClientLease closedShardLease = dynamoDBLeaseManager.getLease("shardId-0");
        closedShardLease.setCheckpoint(ExtendedSequenceNumber.SHARD_END);
        dynamoDBLeaseManager.updateLease(closedShardLease);
        expectedShardIdToCheckpointMap.remove(closedShardLease.getLeaseKey());
        KinesisClientLease childShardLease = dynamoDBLeaseManager.getLease("shardId-6");
        childShardLease.setCheckpoint(new ExtendedSequenceNumber("34290"));
        dynamoDBLeaseManager.updateLease(childShardLease);
        expectedShardIdToCheckpointMap.put(childShardLease.getLeaseKey(), new ExtendedSequenceNumber("34290"));

        retryCheckAndCreateLeaseForNewShards(exceptionMethod, exceptionTime, position);

        newLeases = dynamoDBLeaseManager.listLeases();
        assertEquals(expectedShardIdToCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            ExtendedSequenceNumber expectedCheckpoint = expectedShardIdToCheckpointMap.get(lease1.getLeaseKey());
            assertNotNull(expectedCheckpoint);
            assertEquals(expectedCheckpoint, lease1.getCheckpoint());
        }
    }

    /**
     * Test bootstrapShardLeases() - cleanup garbage leases.
     * 
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     * @throws IOException
     * @throws KinesisClientLibIOException
     */
    @Test
    public final void testBootstrapShardLeasesCleanupGarbage()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException, IOException,
        KinesisClientLibIOException {
        String garbageShardId = "shardId-garbage-001";
        KinesisClientLease garbageLease = ShardSyncer.newKCLLease(ShardObjectHelper.newShard(garbageShardId,
                null,
                null,
                ShardObjectHelper.newSequenceNumberRange("101", null)));
        garbageLease.setCheckpoint(new ExtendedSequenceNumber("999"));
        dynamoDBLeaseManager.createLeaseIfNotExists(garbageLease);
        assertEquals(garbageShardId, dynamoDBLeaseManager.getLease(garbageShardId).getLeaseKey());
        testBootstrapShardLeasesAtStartingPosition(INITIAL_POSITION_LATEST);
        assertNull(dynamoDBLeaseManager.getLease(garbageShardId));
    }

    private void testBootstrapShardLeasesAtStartingPosition(InitialPositionInStreamExtended initialPosition)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException, IOException,
        KinesisClientLibIOException {
        List<Shard> shards = new ArrayList<>();
        SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        String shardId0 = "shardId-0";
        shards.add(ShardObjectHelper.newShard(shardId0, null, null, sequenceRange));
        String shardId1 = "shardId-1";
        shards.add(ShardObjectHelper.newShard(shardId1, null, null, sequenceRange));

        when(leaseManagerProxy.listShards()).thenReturn(shards);

        ShardSyncer.bootstrapShardLeases(leaseManagerProxy, dynamoDBLeaseManager, initialPosition, cleanupLeasesOfCompletedShards,
                                         false);
        List<KinesisClientLease> newLeases = dynamoDBLeaseManager.listLeases();
        assertEquals(2, newLeases.size());
        Set<String> expectedLeaseShardIds = new HashSet<String>();
        expectedLeaseShardIds.add(shardId0);
        expectedLeaseShardIds.add(shardId1);
        for (KinesisClientLease lease1 : newLeases) {
            assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            assertEquals(new ExtendedSequenceNumber(initialPosition.getInitialPositionInStream().toString()),
                    lease1.getCheckpoint());
        }
    }

    /**
     * Test determineNewLeasesToCreate() starting at latest and at trim horizon ("beginning" of shard)
     */
    @Test
    public final void testDetermineNewLeasesToCreateStartingPosition() {
        List<Shard> shards = new ArrayList<>();
        List<KinesisClientLease> currentLeases = new ArrayList<>();
        SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        String shardId0 = "shardId-0";
        shards.add(ShardObjectHelper.newShard(shardId0, null, null, sequenceRange));

        String shardId1 = "shardId-1";
        shards.add(ShardObjectHelper.newShard(shardId1, null, null, sequenceRange));

        Set<InitialPositionInStreamExtended> initialPositions = new HashSet<InitialPositionInStreamExtended>();
        initialPositions.add(INITIAL_POSITION_LATEST);
        initialPositions.add(INITIAL_POSITION_TRIM_HORIZON);

        for (InitialPositionInStreamExtended initialPosition : initialPositions) {
            List<KinesisClientLease> newLeases =
                    ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, initialPosition);
            assertEquals(2, newLeases.size());
            Set<String> expectedLeaseShardIds = new HashSet<String>();
            expectedLeaseShardIds.add(shardId0);
            expectedLeaseShardIds.add(shardId1);
            for (KinesisClientLease lease : newLeases) {
                assertTrue(expectedLeaseShardIds.contains(lease.getLeaseKey()));
                assertEquals(new ExtendedSequenceNumber(initialPosition.getInitialPositionInStream().toString()),
                        lease.getCheckpoint());
            }
        }
    }

    /**
     * Test determineNewLeasesToCreate() - 1 closed and 1 open shard (ignore closed shard)
     */
    @Test
    public final void testDetermineNewLeasesToCreateIgnoreClosedShard() {
        List<Shard> shards = new ArrayList<>();
        List<KinesisClientLease> currentLeases = new ArrayList<>();

        shards.add(ShardObjectHelper.newShard("shardId-0",
                null,
                null,
                ShardObjectHelper.newSequenceNumberRange("303", "404")));
        String lastShardId = "shardId-1";
        shards.add(ShardObjectHelper.newShard(lastShardId,
                null,
                null,
                ShardObjectHelper.newSequenceNumberRange("405", null)));

        List<KinesisClientLease> newLeases =
                ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_LATEST);
        assertEquals(1, newLeases.size());
        assertEquals(lastShardId, newLeases.get(0).getLeaseKey());
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position Latest)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4 5- shards till epoch 102
     * \ / \ / | |
     * 6 7 4 5- shards from epoch 103 - 205
     * \ / | /\
     * 8 4 9 10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeLatest1() {
        List<Shard> shards = constructShardListForGraphA();
        List<KinesisClientLease> currentLeases = new ArrayList<>();

        currentLeases.add(newLease("shardId-3"));
        currentLeases.add(newLease("shardId-4"));
        currentLeases.add(newLease("shardId-5"));

        List<KinesisClientLease> newLeases =
                ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_LATEST);
        Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.TRIM_HORIZON);

        assertEquals(expectedShardIdCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease : newLeases) {
            assertTrue("Unexpected lease: " + lease,
                    expectedShardIdCheckpointMap.containsKey(lease.getLeaseKey()));
            assertEquals(expectedShardIdCheckpointMap.get(lease.getLeaseKey()), lease.getCheckpoint());
        }
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position Latest)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4 5- shards till epoch 102
     * \ / \ / | |
     * 6 7 4 5- shards from epoch 103 - 205
     * \ / | /\
     * 8 4 9 10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 7)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeLatest2() {
        List<Shard> shards = constructShardListForGraphA();
        List<KinesisClientLease> currentLeases = new ArrayList<>();

        currentLeases.add(newLease("shardId-4"));
        currentLeases.add(newLease("shardId-5"));
        currentLeases.add(newLease("shardId-7"));

        List<KinesisClientLease> newLeases =
                ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_LATEST);
        Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.LATEST);

        assertEquals(expectedShardIdCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease : newLeases) {
            assertTrue("Unexpected lease: " + lease,
                    expectedShardIdCheckpointMap.containsKey(lease.getLeaseKey()));
            assertEquals(expectedShardIdCheckpointMap.get(lease.getLeaseKey()), lease.getCheckpoint());
        }
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position TrimHorizon)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4 5- shards till epoch 102
     * \ / \ / | |
     * 6 7 4 5- shards from epoch 103 - 205
     * \ / | /\
     * 8 4 9 10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeHorizon1() {
        List<Shard> shards = constructShardListForGraphA();
        List<KinesisClientLease> currentLeases = new ArrayList<>();

        currentLeases.add(newLease("shardId-3"));
        currentLeases.add(newLease("shardId-4"));
        currentLeases.add(newLease("shardId-5"));

        List<KinesisClientLease> newLeases =
                ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_TRIM_HORIZON);
        Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);

        assertEquals(expectedShardIdCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease : newLeases) {
            assertTrue("Unexpected lease: " + lease,
                    expectedShardIdCheckpointMap.containsKey(lease.getLeaseKey()));
            assertEquals(expectedShardIdCheckpointMap.get(lease.getLeaseKey()), lease.getCheckpoint());
        }
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position TrimHorizon)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4 5- shards till epoch 102
     * \ / \ / | |
     * 6 7 4 5- shards from epoch 103 - 205
     * \ / | /\
     * 8 4 9 10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 7)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeHorizon2() {
        List<Shard> shards = constructShardListForGraphA();
        List<KinesisClientLease> currentLeases = new ArrayList<>();

        currentLeases.add(newLease("shardId-4"));
        currentLeases.add(newLease("shardId-5"));
        currentLeases.add(newLease("shardId-7"));

        List<KinesisClientLease> newLeases =
                ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_TRIM_HORIZON);
        Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap =
                new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);

        assertEquals(expectedShardIdCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease : newLeases) {
            assertTrue("Unexpected lease: " + lease,
                    expectedShardIdCheckpointMap.containsKey(lease.getLeaseKey()));
            assertEquals(expectedShardIdCheckpointMap.get(lease.getLeaseKey()), lease.getCheckpoint());
        }
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position TrimHorizon)
     * For shard graph B (see the construct method doc for structure).
     *
     * Current leases: empty set
     */
    @Test
    public final void testDetermineNewLeasesToCreateGraphBNoInitialLeasesTrim() {
        List<Shard> shards = constructShardListForGraphB();
        List<KinesisClientLease> currentLeases = new ArrayList<>();
        List<KinesisClientLease> newLeases =
                ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_TRIM_HORIZON);
        Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap =
                new HashMap<>();
        for (int i = 0; i < 11; i++) {
            String expectedShardId = "shardId-" + i;
            expectedShardIdCheckpointMap.put(expectedShardId, ExtendedSequenceNumber.TRIM_HORIZON);
        }

        assertEquals(expectedShardIdCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease : newLeases) {
            assertTrue("Unexpected lease: " + lease,
                    expectedShardIdCheckpointMap.containsKey(lease.getLeaseKey()));
            assertEquals(expectedShardIdCheckpointMap.get(lease.getLeaseKey()), lease.getCheckpoint());
        }
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position AT_TIMESTAMP)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeAtTimestamp1() {
        List<Shard> shards = constructShardListForGraphA();
        List<KinesisClientLease> currentLeases = new ArrayList<>();


        currentLeases.add(newLease("shardId-3"));
        currentLeases.add(newLease("shardId-4"));
        currentLeases.add(newLease("shardId-5"));

        List<KinesisClientLease> newLeases =
                ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_AT_TIMESTAMP);
        Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);

        assertEquals(expectedShardIdCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease : newLeases) {
            assertTrue("Unexpected lease: " + lease,
                    expectedShardIdCheckpointMap.containsKey(lease.getLeaseKey()));
            assertEquals(expectedShardIdCheckpointMap.get(lease.getLeaseKey()), lease.getCheckpoint());
        }
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position AT_TIMESTAMP)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 7)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeAtTimestamp2() {
        List<Shard> shards = constructShardListForGraphA();
        List<KinesisClientLease> currentLeases = new ArrayList<>();

        currentLeases.add(newLease("shardId-4"));
        currentLeases.add(newLease("shardId-5"));
        currentLeases.add(newLease("shardId-7"));

        List<KinesisClientLease> newLeases =
                ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_AT_TIMESTAMP);
        Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);

        assertEquals(expectedShardIdCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease : newLeases) {
            assertTrue("Unexpected lease: " + lease,
                    expectedShardIdCheckpointMap.containsKey(lease.getLeaseKey()));
            assertEquals(expectedShardIdCheckpointMap.get(lease.getLeaseKey()), lease.getCheckpoint());
        }
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position AT_TIMESTAMP)
     * For shard graph B (see the construct method doc for structure).
     * Current leases: empty set
     */
    @Test
    public final void testDetermineNewLeasesToCreateGraphBNoInitialLeasesAtTimestamp() {
        List<Shard> shards = constructShardListForGraphB();
        List<KinesisClientLease> currentLeases = new ArrayList<>();
        List<KinesisClientLease> newLeases =
                ShardSyncer.determineNewLeasesToCreate(shards, currentLeases, INITIAL_POSITION_AT_TIMESTAMP);
        Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap =
                new HashMap<>();
        for (int i = 0; i < shards.size(); i++) {
            String expectedShardId = "shardId-" + i;
            expectedShardIdCheckpointMap.put(expectedShardId, ExtendedSequenceNumber.AT_TIMESTAMP);
        }

        assertEquals(expectedShardIdCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease : newLeases) {
            assertTrue("Unexpected lease: " + lease,
                    expectedShardIdCheckpointMap.containsKey(lease.getLeaseKey()));
            assertEquals(expectedShardIdCheckpointMap.get(lease.getLeaseKey()), lease.getCheckpoint());
        }
    }


    /*
     * Helper method to construct a shard list for graph A. Graph A is defined below.
     * Shard structure (y-axis is epochs):
     * 0 1 2 3 4 5- shards till epoch 102
     * \ / \ / | |
     * 6 7 4 5- shards from epoch 103 - 205
     * \ / | /\
     * 8 4 9 10 - shards from epoch 206 (open - no ending sequenceNumber)
     */
    List<Shard> constructShardListForGraphA() {
        List<Shard> shards = new ArrayList<Shard>();

        SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("11", "102");
        SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("11", null);
        SequenceNumberRange range2 = ShardObjectHelper.newSequenceNumberRange("11", "205");
        SequenceNumberRange range3 = ShardObjectHelper.newSequenceNumberRange("103", "205");
        SequenceNumberRange range4 = ShardObjectHelper.newSequenceNumberRange("206", null);

        HashKeyRange hashRange0 = ShardObjectHelper.newHashKeyRange("0", "99");
        HashKeyRange hashRange1 = ShardObjectHelper.newHashKeyRange("100", "199");
        HashKeyRange hashRange2 = ShardObjectHelper.newHashKeyRange("200", "299");
        HashKeyRange hashRange3 = ShardObjectHelper.newHashKeyRange("300", "399");
        HashKeyRange hashRange4 = ShardObjectHelper.newHashKeyRange("400", "499");
        HashKeyRange hashRange5 = ShardObjectHelper.newHashKeyRange("500", ShardObjectHelper.MAX_HASH_KEY);
        HashKeyRange hashRange6 = ShardObjectHelper.newHashKeyRange("0", "199");
        HashKeyRange hashRange7 = ShardObjectHelper.newHashKeyRange("200", "399");
        HashKeyRange hashRange8 = ShardObjectHelper.newHashKeyRange("0", "399");
        HashKeyRange hashRange9 = ShardObjectHelper.newHashKeyRange("500", "799");
        HashKeyRange hashRange10 = ShardObjectHelper.newHashKeyRange("800", ShardObjectHelper.MAX_HASH_KEY);

        shards.add(ShardObjectHelper.newShard("shardId-0", null, null, range0, hashRange0));
        shards.add(ShardObjectHelper.newShard("shardId-1", null, null, range0, hashRange1));
        shards.add(ShardObjectHelper.newShard("shardId-2", null, null, range0, hashRange2));
        shards.add(ShardObjectHelper.newShard("shardId-3", null, null, range0, hashRange3));
        shards.add(ShardObjectHelper.newShard("shardId-4", null, null, range1, hashRange4));
        shards.add(ShardObjectHelper.newShard("shardId-5", null, null, range2, hashRange5));

        shards.add(ShardObjectHelper.newShard("shardId-6", "shardId-0", "shardId-1", range3, hashRange6));
        shards.add(ShardObjectHelper.newShard("shardId-7", "shardId-2", "shardId-3", range3, hashRange7));

        shards.add(ShardObjectHelper.newShard("shardId-8", "shardId-6", "shardId-7", range4, hashRange8));
        shards.add(ShardObjectHelper.newShard("shardId-9", "shardId-5", null, range4, hashRange9));
        shards.add(ShardObjectHelper.newShard("shardId-10", null, "shardId-5", range4, hashRange10));

        return shards;
    }

    /*
     * Helper method to construct a shard list for graph B. Graph B is defined below.
     * Shard structure (x-axis is epochs):
     * 0 3 6 9
     * \ / \ / \ /
     * 2 5 8
     * / \ / \ / \
     * 1 4 7 10
     */
    List<Shard> constructShardListForGraphB() {
        List<Shard> shards = new ArrayList<Shard>();

        SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("1000", "1049");
        SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("1050", "1099");
        SequenceNumberRange range2 = ShardObjectHelper.newSequenceNumberRange("1100", "1149");
        SequenceNumberRange range3 = ShardObjectHelper.newSequenceNumberRange("1150", "1199");
        SequenceNumberRange range4 = ShardObjectHelper.newSequenceNumberRange("1200", "1249");
        SequenceNumberRange range5 = ShardObjectHelper.newSequenceNumberRange("1250", "1299");
        SequenceNumberRange range6 = ShardObjectHelper.newSequenceNumberRange("1300", null);

        HashKeyRange hashRange0 = ShardObjectHelper.newHashKeyRange("0", "499");
        HashKeyRange hashRange1 = ShardObjectHelper.newHashKeyRange("500", ShardObjectHelper.MAX_HASH_KEY);
        HashKeyRange hashRange2 = ShardObjectHelper.newHashKeyRange("0", ShardObjectHelper.MAX_HASH_KEY);

        shards.add(ShardObjectHelper.newShard("shardId-0", null, null, range0, hashRange0));
        shards.add(ShardObjectHelper.newShard("shardId-1", null, null, range0, hashRange1));
        shards.add(ShardObjectHelper.newShard("shardId-2", "shardId-0", "shardId-1", range1, hashRange2));
        shards.add(ShardObjectHelper.newShard("shardId-3", "shardId-2", null, range2, hashRange0));
        shards.add(ShardObjectHelper.newShard("shardId-4", "shardId-2", null, range2, hashRange1));
        shards.add(ShardObjectHelper.newShard("shardId-5", "shardId-3", "shardId-4", range3, hashRange2));
        shards.add(ShardObjectHelper.newShard("shardId-6", "shardId-5", null, range4, hashRange0));
        shards.add(ShardObjectHelper.newShard("shardId-7", "shardId-5", null, range4, hashRange1));
        shards.add(ShardObjectHelper.newShard("shardId-8", "shardId-6", "shardId-7", range5, hashRange2));
        shards.add(ShardObjectHelper.newShard("shardId-9", "shardId-8", null, range6, hashRange0));
        shards.add(ShardObjectHelper.newShard("shardId-10", null, "shardId-8", range6, hashRange1));

        return shards;
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors when shardId is null
     */
    @Test
    public final void testCheckIfDescendantAndAddNewLeasesForAncestorsNullShardId() {
        Map<String, Boolean> memoizationContext = new HashMap<>();
        assertFalse(ShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(null, INITIAL_POSITION_LATEST,
                null,
                null,
                null,
                memoizationContext));
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors when shard has been trimmed
     */
    @Test
    public final void testCheckIfDescendantAndAddNewLeasesForAncestorsTrimmedShard() {
        String shardId = "shardId-trimmed";
        Map<String, Shard> kinesisShards = new HashMap<String, Shard>();
        Map<String, Boolean> memoizationContext = new HashMap<>();
        assertFalse(ShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId, INITIAL_POSITION_LATEST,
                null,
                kinesisShards,
                null,
                memoizationContext));
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors when there is a current lease for the shard
     */
    @Test
    public final void testCheckIfDescendantAndAddNewLeasesForAncestorsForShardWithCurrentLease() {
        String shardId = "shardId-current";
        Map<String, Shard> kinesisShards = new HashMap<String, Shard>();
        kinesisShards.put(shardId, ShardObjectHelper.newShard(shardId, null, null, null));
        Set<String> shardIdsOfCurrentLeases = new HashSet<String>();
        shardIdsOfCurrentLeases.add(shardId);
        Map<String, KinesisClientLease> newLeaseMap = new HashMap<String, KinesisClientLease>();
        Map<String, Boolean> memoizationContext = new HashMap<>();
        assertTrue(ShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId, INITIAL_POSITION_LATEST,
                shardIdsOfCurrentLeases,
                kinesisShards,
                newLeaseMap,
                memoizationContext));
        assertTrue(newLeaseMap.isEmpty());
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors - two parents, two ancestors, not descendant
     */
    @Test
    public final void testCheckIfDescendantAndAddNewLeasesForAncestors2P2ANotDescendant() {
        Set<String> shardIdsOfCurrentLeases = new HashSet<String>();
        Map<String, KinesisClientLease> newLeaseMap = new HashMap<String, KinesisClientLease>();
        Map<String, Shard> kinesisShards = new HashMap<String, Shard>();

        String parentShardId = "shardId-parent";
        kinesisShards.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null));

        String adjacentParentShardId = "shardId-adjacentParent";
        kinesisShards.put(adjacentParentShardId, ShardObjectHelper.newShard(adjacentParentShardId, null, null, null));

        String shardId = "shardId-9-1";
        kinesisShards.put(shardId, ShardObjectHelper.newShard(shardId, parentShardId, adjacentParentShardId, null));

        Map<String, Boolean> memoizationContext = new HashMap<>();
        assertFalse(ShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId, INITIAL_POSITION_LATEST,
                shardIdsOfCurrentLeases,
                kinesisShards,
                newLeaseMap,
                memoizationContext));
        assertTrue(newLeaseMap.isEmpty());
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors - two parents, there is a lease for one parent.
     */
    @Test
    public final void testCheckIfDescendantAndAddNewLeasesForAncestors2P2A1PDescendant() {
        Set<String> shardIdsOfCurrentLeases = new HashSet<String>();
        Map<String, KinesisClientLease> newLeaseMap = new HashMap<String, KinesisClientLease>();
        Map<String, Shard> kinesisShards = new HashMap<String, Shard>();

        String parentShardId = "shardId-parent";
        kinesisShards.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null));
        shardIdsOfCurrentLeases.add(parentShardId);

        String adjacentParentShardId = "shardId-adjacentParent";
        kinesisShards.put(adjacentParentShardId, ShardObjectHelper.newShard(adjacentParentShardId, null, null, null));

        String shardId = "shardId-9-1";
        Shard shard = ShardObjectHelper.newShard(shardId, parentShardId, adjacentParentShardId, null);
        kinesisShards.put(shardId, shard);

        Map<String, Boolean> memoizationContext = new HashMap<>();
        assertTrue(ShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId, INITIAL_POSITION_LATEST,
                shardIdsOfCurrentLeases,
                kinesisShards,
                newLeaseMap,
                memoizationContext));
        assertEquals(1, newLeaseMap.size());
        assertTrue(newLeaseMap.containsKey(adjacentParentShardId));
        KinesisClientLease adjacentParentLease = newLeaseMap.get(adjacentParentShardId);
        assertEquals(ExtendedSequenceNumber.LATEST, adjacentParentLease.getCheckpoint());
    }

    /**
     * Test parentShardIds() when the shard has no parents.
     */
    @Test
    public final void testGetParentShardIdsNoParents() {
        Shard shard = new Shard();
        assertTrue(ShardSyncer.getParentShardIds(shard, null).isEmpty());
    }

    /**
     * Test parentShardIds() when the shard has no parents.
     */
    @Test
    public final void testGetParentShardIdsTrimmedParents() {
        Map<String, Shard> shardMap = new HashMap<String, Shard>();
        Shard shard = ShardObjectHelper.newShard("shardId-test", "foo", "bar", null);
        assertTrue(ShardSyncer.getParentShardIds(shard, shardMap).isEmpty());
    }

    /**
     * Test parentShardIds() when the shard has a single parent.
     */
    @Test
    public final void testGetParentShardIdsSingleParent() {
        Map<String, Shard> shardMap = new HashMap<String, Shard>();

        String parentShardId = "shardId-parent";
        shardMap.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null));

        Shard shard = ShardObjectHelper.newShard("shardId-test", parentShardId, null, null);
        Set<String> parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
        assertEquals(1, parentShardIds.size());
        assertTrue(parentShardIds.contains(parentShardId));

        shard.setParentShardId(null);
        parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
        assertTrue(parentShardIds.isEmpty());

        shard.setAdjacentParentShardId(parentShardId);
        parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
        assertEquals(1, parentShardIds.size());
        assertTrue(parentShardIds.contains(parentShardId));
    }

    /**
     * Test parentShardIds() when the shard has two parents, one is trimmed.
     */
    @Test
    public final void testGetParentShardIdsOneTrimmedParent() {
        Map<String, Shard> shardMap = new HashMap<String, Shard>();

        String parentShardId = "shardId-parent";
        Shard parent = ShardObjectHelper.newShard(parentShardId, null, null, null);

        String adjacentParentShardId = "shardId-adjacentParent";
        Shard adjacentParent = ShardObjectHelper.newShard(adjacentParentShardId, null, null, null);

        Shard shard = ShardObjectHelper.newShard("shardId-test", parentShardId, adjacentParentShardId, null);

        shardMap.put(parentShardId, parent);
        Set<String> parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
        assertEquals(1, parentShardIds.size());
        assertTrue(parentShardIds.contains(parentShardId));

        shardMap.remove(parentShardId);
        parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
        assertTrue(parentShardIds.isEmpty());

        shardMap.put(adjacentParentShardId, adjacentParent);
        parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
        assertEquals(1, parentShardIds.size());
        assertTrue(parentShardIds.contains(adjacentParentShardId));
    }

    /**
     * Test parentShardIds() when the shard has two parents.
     */
    @Test
    public final void testGetParentShardIdsTwoParents() {
        Map<String, Shard> shardMap = new HashMap<String, Shard>();

        String parentShardId = "shardId-parent";
        shardMap.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null));

        String adjacentParentShardId = "shardId-adjacentParent";
        shardMap.put(adjacentParentShardId, ShardObjectHelper.newShard(adjacentParentShardId, null, null, null));

        Shard shard = ShardObjectHelper.newShard("shardId-test", parentShardId, adjacentParentShardId, null);

        Set<String> parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
        assertEquals(2, parentShardIds.size());
        assertTrue(parentShardIds.contains(parentShardId));
        assertTrue(parentShardIds.contains(adjacentParentShardId));
    }

    /**
     */
    @Test
    public final void testNewLease() {
        Shard shard = new Shard();
        String shardId = "shardId-95";
        shard.setShardId(shardId);
        String parentShardId = "shardId-parent";
        String adjacentParentShardId = "shardId-adjacentParent";
        shard.setParentShardId(parentShardId);
        shard.setAdjacentParentShardId(adjacentParentShardId);

        KinesisClientLease lease = ShardSyncer.newKCLLease(shard);
        assertEquals(shardId, lease.getLeaseKey());
        assertNull(lease.getCheckpoint());
        Set<String> parentIds = lease.getParentShardIds();
        assertEquals(2, parentIds.size());
        assertTrue(parentIds.contains(parentShardId));
        assertTrue(parentIds.contains(adjacentParentShardId));
    }

    /**
     * Test method for constructShardIdToShardMap.
     *
     * .
     */
    @Test
    public final void testConstructShardIdToShardMap() {
        List<Shard> shards = new ArrayList<Shard>(2);
        shards.add(ShardObjectHelper.newShard("shardId-0", null, null, null));
        shards.add(ShardObjectHelper.newShard("shardId-1", null, null, null));

        Map<String, Shard> shardIdToShardMap = ShardSyncer.constructShardIdToShardMap(shards);
        assertEquals(shards.size(), shardIdToShardMap.size());
        for (Shard shard : shards) {
            assertSame(shard, shardIdToShardMap.get(shard.getShardId()));
        }
    }

    /**
     * Test getOpenShards() - no shards are open.
     */
    @Test
    public final void testGetOpenShardsNoneOpen() {
        List<Shard> shards = new ArrayList<Shard>();
        shards.add(ShardObjectHelper.newShard("shardId-9384",
                null,
                null,
                ShardObjectHelper.newSequenceNumberRange("123", "345")));
        assertTrue(ShardSyncer.getOpenShards(shards).isEmpty());
    }

    /**
     * Test getOpenShards() - test null and max end sequence number.
     */
    @Test
    public final void testGetOpenShardsNullAndMaxEndSeqNum() {
        List<Shard> shards = new ArrayList<Shard>();
        String shardId = "shardId-2738";
        SequenceNumberRange sequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("123", null);
        shards.add(ShardObjectHelper.newShard(shardId, null, null, sequenceNumberRange));

        // Verify shard is considered open when it has a null end sequence number
        List<Shard> openShards = ShardSyncer.getOpenShards(shards);
        assertEquals(1, openShards.size());
        assertEquals(shardId, openShards.get(0).getShardId());

        // Close shard before testing for max sequence number
        sequenceNumberRange.setEndingSequenceNumber("1000");
        openShards = ShardSyncer.getOpenShards(shards);
        assertTrue(openShards.isEmpty());

        // Verify shard is considered closed when the end sequence number is set to max allowed sequence number
        sequenceNumberRange.setEndingSequenceNumber(MAX_SEQUENCE_NUMBER.toString());
        openShards = ShardSyncer.getOpenShards(shards);
        assertEquals(0, openShards.size());
    }

    /**
     * Test isCandidateForCleanup
     * 
     * @throws KinesisClientLibIOException
     */
    @Test
    public final void testIsCandidateForCleanup() throws KinesisClientLibIOException {
        String parentShardId = "shardId-0000";
        String adjacentParentShardId = "shardId-0001";
        String shardId = "shardId-0002";
        KinesisClientLease lease = newLease(shardId);
        List<String> parentShardIds = new ArrayList<>();
        parentShardIds.add(parentShardId);
        parentShardIds.add(adjacentParentShardId);
        lease.setParentShardIds(parentShardIds);
        Set<String> currentKinesisShardIds = new HashSet<>();

        currentKinesisShardIds.add(shardId);
        assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));

        currentKinesisShardIds.clear();
        assertTrue(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));

        currentKinesisShardIds.add(parentShardId);
        // assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));

        currentKinesisShardIds.clear();
        assertTrue(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));

        currentKinesisShardIds.add(adjacentParentShardId);
        // assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
        currentKinesisShardIds.add(parentShardId);
        // assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
        currentKinesisShardIds.add(shardId);
        assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    }

    /**
     * Test isCandidateForCleanup
     * 
     * @throws KinesisClientLibIOException
     */
    @Test(expected = KinesisClientLibIOException.class)
    public final void testIsCandidateForCleanupParentExists() throws KinesisClientLibIOException {
        String parentShardId = "shardId-0000";
        String adjacentParentShardId = "shardId-0001";
        String shardId = "shardId-0002";
        KinesisClientLease lease = newLease(shardId);
        List<String> parentShardIds = new ArrayList<>();
        parentShardIds.add(parentShardId);
        parentShardIds.add(adjacentParentShardId);
        lease.setParentShardIds(parentShardIds);
        Set<String> currentKinesisShardIds = new HashSet<>();

        currentKinesisShardIds.add(parentShardId);
        assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    }

    /**
     * Test isCandidateForCleanup
     * 
     * @throws KinesisClientLibIOException
     */
    @Test(expected = KinesisClientLibIOException.class)
    public final void testIsCandidateForCleanupAdjacentParentExists() throws KinesisClientLibIOException {
        String parentShardId = "shardId-0000";
        String adjacentParentShardId = "shardId-0001";
        String shardId = "shardId-0002";
        KinesisClientLease lease = newLease(shardId);
        List<String> parentShardIds = new ArrayList<>();
        parentShardIds.add(parentShardId);
        parentShardIds.add(adjacentParentShardId);
        lease.setParentShardIds(parentShardIds);
        Set<String> currentKinesisShardIds = new HashSet<>();

        currentKinesisShardIds.add(adjacentParentShardId);
        assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    }

    /**
     * Test cleanup of lease for a shard that has been fully processed (and processing of child shards has begun).
     * 
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     */
    @Test
    public final void testCleanupLeaseForClosedShard()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        String closedShardId = "shardId-2";
        KinesisClientLease leaseForClosedShard = newLease(closedShardId);
        leaseForClosedShard.setCheckpoint(new ExtendedSequenceNumber("1234"));
        dynamoDBLeaseManager.createLeaseIfNotExists(leaseForClosedShard);

        Set<String> childShardIds = new HashSet<>();
        List<KinesisClientLease> trackedLeases = new ArrayList<>();
        Set<String> parentShardIds = new HashSet<>();
        parentShardIds.add(closedShardId);
        String childShardId1 = "shardId-5";
        KinesisClientLease childLease1 = newLease(childShardId1);
        childLease1.setParentShardIds(parentShardIds);
        childLease1.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        String childShardId2 = "shardId-7";
        KinesisClientLease childLease2 = newLease(childShardId2);
        childLease2.setParentShardIds(parentShardIds);
        childLease2.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        Map<String, KinesisClientLease> trackedLeaseMap = ShardSyncer.constructShardIdToKCLLeaseMap(trackedLeases);

        // empty list of leases
        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, dynamoDBLeaseManager);
        assertNotNull(dynamoDBLeaseManager.getLease(closedShardId));

        // closed shard has not been fully processed yet (checkpoint != SHARD_END)
        trackedLeases.add(leaseForClosedShard);
        trackedLeaseMap = ShardSyncer.constructShardIdToKCLLeaseMap(trackedLeases);
        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, dynamoDBLeaseManager);
        assertNotNull(dynamoDBLeaseManager.getLease(closedShardId));

        // closed shard has been fully processed yet (checkpoint == SHARD_END)
        leaseForClosedShard.setCheckpoint(ExtendedSequenceNumber.SHARD_END);
        dynamoDBLeaseManager.updateLease(leaseForClosedShard);
        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, dynamoDBLeaseManager);
        assertNull(dynamoDBLeaseManager.getLease(closedShardId));

        // lease for only one child exists
        childShardIds.add(childShardId1);
        childShardIds.add(childShardId2);
        dynamoDBLeaseManager.createLeaseIfNotExists(leaseForClosedShard);
        dynamoDBLeaseManager.createLeaseIfNotExists(childLease1);
        trackedLeases.add(childLease1);
        trackedLeaseMap = ShardSyncer.constructShardIdToKCLLeaseMap(trackedLeases);
        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, dynamoDBLeaseManager);
        assertNotNull(dynamoDBLeaseManager.getLease(closedShardId));

        // leases for both children exists, but they are both at TRIM_HORIZON
        dynamoDBLeaseManager.createLeaseIfNotExists(childLease2);
        trackedLeases.add(childLease2);
        trackedLeaseMap = ShardSyncer.constructShardIdToKCLLeaseMap(trackedLeases);
        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, dynamoDBLeaseManager);
        assertNotNull(dynamoDBLeaseManager.getLease(closedShardId));

        // leases for both children exists, one is at TRIM_HORIZON
        childLease1.setCheckpoint(new ExtendedSequenceNumber("34890"));
        dynamoDBLeaseManager.updateLease(childLease1);
        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, dynamoDBLeaseManager);
        assertNotNull(dynamoDBLeaseManager.getLease(closedShardId));

        // leases for both children exists, NONE of them are at TRIM_HORIZON
        childLease2.setCheckpoint(new ExtendedSequenceNumber("43789"));
        dynamoDBLeaseManager.updateLease(childLease2);
        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, dynamoDBLeaseManager);
        assertNull(dynamoDBLeaseManager.getLease(closedShardId));
    }

    /**
     * Test we can handle trimmed Kinesis shards (absent from the shard list), and valid closed shards.
     * 
     * @throws KinesisClientLibIOException
     */
    @Test
    public final void testAssertShardCoveredOrAbsentTestAbsentAndValid() throws KinesisClientLibIOException {
        List<Shard> shards = new ArrayList<>();
        String expectedClosedShardId = "shardId-34098";
        SequenceNumberRange sequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("103", "205");
        HashKeyRange hashKeyRange = ShardObjectHelper.newHashKeyRange("10", "25");
        Shard closedShard =
                ShardObjectHelper.newShard(expectedClosedShardId, null, null, sequenceNumberRange, hashKeyRange);
        SequenceNumberRange childSequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("206", "300");
        Shard child1 =
                ShardObjectHelper.newShard("shardId-54879", expectedClosedShardId, null, childSequenceNumberRange);
        Map<String, Shard> shardIdToShardMap = ShardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> closedShardIds = new HashSet<>();
        closedShardIds.add(expectedClosedShardId);
        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);

        // test for case where shard has been trimmed (absent from list)
        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);

        // Populate shards.
        shards.add(closedShard);
        shards.add(child1);
        shardIdToShardMap.put(expectedClosedShardId, closedShard);
        shardIdToShardMap.put(child1.getShardId(), child1);
        shardIdToChildShardIdsMap = ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);

        // test degenerate split/merge
        child1.setHashKeyRange(hashKeyRange);
        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);

        // test merge
        child1.setHashKeyRange(ShardObjectHelper.newHashKeyRange("10", "2985"));
        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);
        child1.setHashKeyRange(ShardObjectHelper.newHashKeyRange("3", "25"));
        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);

        // test split
        HashKeyRange childHashKeyRange1 = ShardObjectHelper.newHashKeyRange("10", "15");
        HashKeyRange childHashKeyRange2 = ShardObjectHelper.newHashKeyRange("16", "25");
        child1.setHashKeyRange(childHashKeyRange1);
        Shard child2 = ShardObjectHelper.newShard("shardId-43789",
                null,
                expectedClosedShardId,
                childSequenceNumberRange,
                childHashKeyRange2);
        shards.add(child2);
        shardIdToShardMap.put(child2.getShardId(), child2);
        shardIdToChildShardIdsMap = ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);
    }

    /**
     * Test we throw an exception if the shard is open
     * 
     * @throws KinesisClientLibIOException
     */
    @Test(expected = KinesisClientLibIOException.class)
    public final void testAssertShardCoveredOrAbsentTestOpen() throws KinesisClientLibIOException {
        List<Shard> shards = new ArrayList<>();
        String expectedClosedShardId = "shardId-34098";
        SequenceNumberRange sequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("103", null);
        HashKeyRange hashKeyRange = ShardObjectHelper.newHashKeyRange("10", "25");
        Shard openShard =
                ShardObjectHelper.newShard(expectedClosedShardId, null, null, sequenceNumberRange, hashKeyRange);
        shards.add(openShard);
        Map<String, Shard> shardIdToShardMap = ShardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> closedShardIds = new HashSet<>();
        closedShardIds.add(expectedClosedShardId);
        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);
    }

    /**
     * Test we throw an exception if there are no children
     * 
     * @throws KinesisClientLibIOException
     */
    @Test(expected = KinesisClientLibIOException.class)
    public final void testAssertShardCoveredOrAbsentTestNoChildren() throws KinesisClientLibIOException {
        List<Shard> shards = new ArrayList<>();
        String expectedClosedShardId = "shardId-34098";
        SequenceNumberRange sequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("103", "205");
        HashKeyRange hashKeyRange = ShardObjectHelper.newHashKeyRange("10", "25");
        Shard closedShard =
                ShardObjectHelper.newShard(expectedClosedShardId, null, null, sequenceNumberRange, hashKeyRange);
        shards.add(closedShard);
        Map<String, Shard> shardIdToShardMap = ShardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> closedShardIds = new HashSet<>();
        closedShardIds.add(expectedClosedShardId);
        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);
    }

    /**
     * Test we throw an exception if children don't cover hash key range (min of children > min of parent)
     * 
     * @throws KinesisClientLibIOException
     */
    @Test(expected = KinesisClientLibIOException.class)
    public final void testAssertShardCoveredOrAbsentTestIncompleteSplitMin() throws KinesisClientLibIOException {
        HashKeyRange hashKeyRange = ShardObjectHelper.newHashKeyRange("10", "25");
        HashKeyRange childHashKeyRange1 = ShardObjectHelper.newHashKeyRange("12", "15");
        HashKeyRange childHashKeyRange2 = ShardObjectHelper.newHashKeyRange("16", "25");
        testAssertShardCoveredOrAbsentTestIncompleteSplit(hashKeyRange, childHashKeyRange1, childHashKeyRange2);
    }

    /**
     * Test we throw an exception if children don't cover hash key range (max of children < max of parent)
     * 
     * @throws KinesisClientLibIOException
     */
    @Test(expected = KinesisClientLibIOException.class)
    public final void testAssertShardCoveredOrAbsentTestIncompleteSplitMax() throws KinesisClientLibIOException {
        HashKeyRange hashKeyRange = ShardObjectHelper.newHashKeyRange("10", "25");
        HashKeyRange childHashKeyRange1 = ShardObjectHelper.newHashKeyRange("10", "15");
        HashKeyRange childHashKeyRange2 = ShardObjectHelper.newHashKeyRange("16", "23");
        testAssertShardCoveredOrAbsentTestIncompleteSplit(hashKeyRange, childHashKeyRange1, childHashKeyRange2);
    }

    private void testAssertShardCoveredOrAbsentTestIncompleteSplit(HashKeyRange parentHashKeyRange,
            HashKeyRange child1HashKeyRange,
            HashKeyRange child2HashKeyRange)
        throws KinesisClientLibIOException {
        List<Shard> shards = new ArrayList<>();
        String expectedClosedShardId = "shardId-34098";
        SequenceNumberRange sequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("103", "205");
        Shard closedShard =
                ShardObjectHelper.newShard(expectedClosedShardId, null, null, sequenceNumberRange, parentHashKeyRange);
        shards.add(closedShard);

        SequenceNumberRange childSequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("206", "300");
        Shard child1 = ShardObjectHelper.newShard("shardId-43789",
                null,
                expectedClosedShardId,
                childSequenceNumberRange,
                child1HashKeyRange);
        shards.add(child1);
        Shard child2 = ShardObjectHelper.newShard("shardId-43789",
                null,
                expectedClosedShardId,
                childSequenceNumberRange,
                child2HashKeyRange);
        shards.add(child2);

        Map<String, Shard> shardIdToShardMap = ShardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> closedShardIds = new HashSet<>();
        closedShardIds.add(expectedClosedShardId);
        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);
    }

    /**
     * Helper method.
     * 
     * @param shardId
     * @return
     */
    private KinesisClientLease newLease(String shardId) {
        KinesisClientLease lease = new KinesisClientLease();
        lease.setLeaseKey(shardId);

        return lease;
    }

}
