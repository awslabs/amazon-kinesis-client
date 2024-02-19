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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.kinesis.leases.impl.Lease;
import com.amazonaws.services.kinesis.model.ShardFilter;
import com.amazonaws.services.kinesis.model.ShardFilterType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.KinesisClientLibIOException;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ExceptionThrowingLeaseManager.ExceptionThrowingLeaseManagerMethods;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisShardSyncer.MemoizationContext;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisLocalFileProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.util.KinesisLocalFileDataCreator;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.leases.impl.LeaseManager;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;

import junit.framework.Assert;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
// CHECKSTYLE:IGNORE JavaNCSS FOR NEXT 800 LINES
public class ShardSyncerTest {
    private static final Log LOG = LogFactory.getLog(KinesisShardSyncer.class);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_AT_TIMESTAMP =
            InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(1000L));
    private static String LEASE_OWNER = "leaseOwner";
    private final boolean cleanupLeasesOfCompletedShards = true;
    private static final int EXPONENT = 128;
    AmazonDynamoDB ddbClient = DynamoDBEmbedded.create().amazonDynamoDB();
    private LeaseManager<KinesisClientLease> leaseManager = new KinesisClientLeaseManager("tempTestTable", ddbClient, KinesisClientLibConfiguration.DEFAULT_DDB_BILLING_MODE);
    protected static final KinesisLeaseCleanupValidator leaseCleanupValidator = new KinesisLeaseCleanupValidator();
    private static final KinesisShardSyncer shardSyncer = new KinesisShardSyncer(leaseCleanupValidator);
    private static final HashKeyRange hashKeyRange = new HashKeyRange().withStartingHashKey("0").withEndingHashKey("10");

    /**
     * Old/Obsolete max value of a sequence number (2^128 -1).
     */
    public static final BigInteger MAX_SEQUENCE_NUMBER = new BigInteger("2").pow(EXPONENT).subtract(BigInteger.ONE);

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
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
        boolean created = leaseManager.createLeaseTableIfNotExists(1L, 1L);
        if (created) {
            LOG.info("New table created.");
        }
        leaseManager.deleteAll();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        leaseManager.deleteAll();
    }

    /**
     * Test determineNewLeasesToCreate() where there are no shards
     */
    @Test
    public final void testDetermineNewLeasesToCreateNoShards() {
        List<Shard> shards = new ArrayList<Shard>();
        List<KinesisClientLease> leases = new ArrayList<KinesisClientLease>();
        final LeaseSynchronizer leaseSynchronizer = getLeaseSynchronizer(shards, leases);

        Assert.assertTrue(shardSyncer.determineNewLeasesToCreate(leaseSynchronizer, shards, leases, INITIAL_POSITION_LATEST).isEmpty());
    }

    /**
     * Test determineNewLeasesToCreate() where there are no leases and no resharding operations have been performed
     */
    @Test
    public final void testDetermineNewLeasesToCreate0Leases0Reshards() {
        List<Shard> shards = new ArrayList<Shard>();
        List<KinesisClientLease> currentLeases = new ArrayList<KinesisClientLease>();
        SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        String shardId0 = "shardId-0";
        shards.add(ShardObjectHelper.newShard(shardId0, null, null, sequenceRange, hashKeyRange));

        String shardId1 = "shardId-1";
        shards.add(ShardObjectHelper.newShard(shardId1, null, null, sequenceRange, hashKeyRange));

        final LeaseSynchronizer leaseSynchronizer = getLeaseSynchronizer(shards, currentLeases);

        List<KinesisClientLease> newLeases =
                shardSyncer.determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, INITIAL_POSITION_LATEST);
        Assert.assertEquals(2, newLeases.size());
        Set<String> expectedLeaseShardIds = new HashSet<String>();
        expectedLeaseShardIds.add(shardId0);
        expectedLeaseShardIds.add(shardId1);
        for (KinesisClientLease lease : newLeases) {
            Assert.assertTrue(expectedLeaseShardIds.contains(lease.getLeaseKey()));
        }
    }

    /**
     * Test determineNewLeasesToCreate() where there is one lease and no resharding operations have been performed, but one of
     * the shards was marked as inconsistent.
     */
    @Test
    public final void testDetermineNewLeasesToCreate0Leases0Reshards1Inconsistent() {
        List<Shard> shards = new ArrayList<Shard>();
        List<KinesisClientLease> currentLeases = new ArrayList<KinesisClientLease>();
        SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        String shardId0 = "shardId-0";
        shards.add(ShardObjectHelper.newShard(shardId0, null, null, sequenceRange, hashKeyRange));

        String shardId1 = "shardId-1";
        shards.add(ShardObjectHelper.newShard(shardId1, null, null, sequenceRange, hashKeyRange));

        String shardId2 = "shardId-2";
        shards.add(ShardObjectHelper.newShard(shardId2, shardId1, null, sequenceRange, hashKeyRange));

        String shardIdWithLease = "shardId-3";
        shards.add(ShardObjectHelper.newShard(shardIdWithLease, shardIdWithLease, null, sequenceRange, hashKeyRange));

        currentLeases.add(newLease(shardIdWithLease));

        Set<String> inconsistentShardIds = new HashSet<String>();
        inconsistentShardIds.add(shardId2);

        final LeaseSynchronizer leaseSynchronizer = getLeaseSynchronizer(shards, currentLeases);

        List<KinesisClientLease> newLeases =
                shardSyncer.determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, INITIAL_POSITION_LATEST, inconsistentShardIds);
        Assert.assertEquals(2, newLeases.size());
        Set<String> expectedLeaseShardIds = new HashSet<String>();
        expectedLeaseShardIds.add(shardId0);
        expectedLeaseShardIds.add(shardId1);
        for (KinesisClientLease lease : newLeases) {
            Assert.assertTrue(expectedLeaseShardIds.contains(lease.getLeaseKey()));
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
     * All open and closed shards within stream's retention period should be sync'ed when lease table is empty.
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtLatestWithEmptyLeaseTable1()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {
        List<Shard> shards = constructShardListForGraphA();
        File dataFile = KinesisLocalFileDataCreator.generateTempDataFile(shards, 2, "testBootstrap1");
        dataFile.deleteOnExit();
        IKinesisProxy kinesisProxy = new KinesisLocalFileProxy(dataFile.getAbsolutePath());

        shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, INITIAL_POSITION_LATEST,
                cleanupLeasesOfCompletedShards, false, shards);
        List<KinesisClientLease> newLeases = leaseManager.listLeases();
        Set<String> expectedLeaseShardIds = new HashSet<String>();
        for (int i = 0; i < 11; i++) {
            expectedLeaseShardIds.add("shardId-" + i);
        }
        Assert.assertEquals(expectedLeaseShardIds.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            Assert.assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            Assert.assertEquals(ExtendedSequenceNumber.LATEST, lease1.getCheckpoint());
        }
        dataFile.delete();
    }

    /**
     * We should only create leases for shards at LATEST when lease table is not empty.
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtLatestWithPartialLeaseTable1()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {
        List<Shard> shards = constructShardListForGraphA();
        File dataFile = KinesisLocalFileDataCreator.generateTempDataFile(shards, 2, "testBootstrap1");
        dataFile.deleteOnExit();
        IKinesisProxy kinesisProxy = new KinesisLocalFileProxy(dataFile.getAbsolutePath());

        // shardId-10 exists at LATEST - create a lease for it
        KinesisClientLease lease = newLease("shardId-10");
        lease.setCheckpoint(ExtendedSequenceNumber.LATEST);
        leaseManager.createLeaseIfNotExists(lease);

        shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, INITIAL_POSITION_LATEST,
                cleanupLeasesOfCompletedShards, false, shards);
        List<KinesisClientLease> newLeases = leaseManager.listLeases();
        Set<String> expectedLeaseShardIds = new HashSet<String>();
        expectedLeaseShardIds.add("shardId-4");
        expectedLeaseShardIds.add("shardId-8");
        expectedLeaseShardIds.add("shardId-9");
        expectedLeaseShardIds.add("shardId-10");
        Assert.assertEquals(expectedLeaseShardIds.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            Assert.assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            Assert.assertEquals(ExtendedSequenceNumber.LATEST, lease1.getCheckpoint());
        }
        dataFile.delete();
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtTrimHorizon()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {
        List<Shard> shards = constructShardListForGraphA();
        File dataFile = KinesisLocalFileDataCreator.generateTempDataFile(shards, 2, "testBootstrap1");
        dataFile.deleteOnExit();
        IKinesisProxy kinesisProxy = new KinesisLocalFileProxy(dataFile.getAbsolutePath());

        shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards, false, shards);
        List<KinesisClientLease> newLeases = leaseManager.listLeases();
        Set<String> expectedLeaseShardIds = new HashSet<String>();
        for (int i = 0; i < 11; i++) {
            expectedLeaseShardIds.add("shardId-" + i);
        }
        Assert.assertEquals(expectedLeaseShardIds.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            Assert.assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            Assert.assertEquals(ExtendedSequenceNumber.TRIM_HORIZON, lease1.getCheckpoint());
        }
        dataFile.delete();
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
        File dataFile = KinesisLocalFileDataCreator.generateTempDataFile(shards, 1, "testBootstrap1");
        dataFile.deleteOnExit();
        IKinesisProxy kinesisProxy = new KinesisLocalFileProxy(dataFile.getAbsolutePath());

        shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, INITIAL_POSITION_AT_TIMESTAMP,
                cleanupLeasesOfCompletedShards, false, shards);
        List<KinesisClientLease> newLeases = leaseManager.listLeases();
        Set<String> expectedLeaseShardIds = new HashSet<String>();
        for (int i = 0; i < 11; i++) {
            expectedLeaseShardIds.add("shardId-" + i);
        }
        Assert.assertEquals(expectedLeaseShardIds.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            Assert.assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            Assert.assertEquals(ExtendedSequenceNumber.AT_TIMESTAMP, lease1.getCheckpoint());
        }
        dataFile.delete();
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
        File dataFile = KinesisLocalFileDataCreator.generateTempDataFile(shards, 2, "testBootstrap1");
        dataFile.deleteOnExit();
        IKinesisProxy kinesisProxy = new KinesisLocalFileProxy(dataFile.getAbsolutePath());

        shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards, false, shards);
        dataFile.delete();
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
        Assert.assertEquals("shardId-5", shard.getShardId());
        SequenceNumberRange range = shard.getSequenceNumberRange();
        // shardId-5 in graph A has two children (shardId-9 and shardId-10).  if shardId-5
        // is not closed, those children should be ignored when syncing shards, no leases
        // should be obtained for them, and we should obtain a lease on the still-open
        // parent.
        range.setEndingSequenceNumber(null);
        shard.setSequenceNumberRange(range);
        File dataFile = KinesisLocalFileDataCreator.generateTempDataFile(shards, 2, "testBootstrap1");
        dataFile.deleteOnExit();
        IKinesisProxy kinesisProxy = new KinesisLocalFileProxy(dataFile.getAbsolutePath());

        // Create a dummy lease in the lease table - otherwise leaseManager will create leases for all shards if
        // lease table is empty.
        KinesisClientLease lease = newLease("shardId-1000");
        lease.setCheckpoint(ExtendedSequenceNumber.LATEST);
        leaseManager.createLeaseIfNotExists(lease);

        shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, INITIAL_POSITION_LATEST,
                cleanupLeasesOfCompletedShards, true, shards);
        List<KinesisClientLease> newLeases = leaseManager.listLeases();
        Set<String> expectedLeaseShardIds = new HashSet<String>();
        expectedLeaseShardIds.add("shardId-1000"); // dummy lease will still be in the table.
        expectedLeaseShardIds.add("shardId-4");
        expectedLeaseShardIds.add("shardId-5");
        expectedLeaseShardIds.add("shardId-8");
        Assert.assertEquals(expectedLeaseShardIds.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            Assert.assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            Assert.assertEquals(ExtendedSequenceNumber.LATEST, lease1.getCheckpoint());
        }
        dataFile.delete();
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
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList(
                "shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5"
        ));
        testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(null,
                Integer.MAX_VALUE, INITIAL_POSITION_TRIM_HORIZON, expectedLeaseKeysToCreate);
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtTrimHorizonAndClosedShardWithDeleteLeaseExceptions()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {

        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList(
                "shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5"
        ));
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 10;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.DELETELEASE, c, INITIAL_POSITION_TRIM_HORIZON, expectedLeaseKeysToCreate);
            // Need to clean up lease manager every time after calling KinesisShardSyncer
            leaseManager.deleteAll();
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
    public final void testCheckAndCreateLeasesForNewShardsAtTrimHorizonAndClosedShardWithListLeasesExceptions()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList(
                "shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5"
        ));
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 10;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.LISTLEASES, c, INITIAL_POSITION_TRIM_HORIZON, expectedLeaseKeysToCreate);
            // Need to clean up lease manager every time after calling KinesisShardSyncer
            leaseManager.deleteAll();
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
    public final void testCheckAndCreateLeasesForNewShardsAtTrimHorizonAndClosedShardWithCreateLeaseExceptions()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList(
                "shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5"
        ));
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 1;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.CREATELEASEIFNOTEXISTS, c, INITIAL_POSITION_TRIM_HORIZON, expectedLeaseKeysToCreate);
        }
    }

    // Try catch leaseException for different lease manager methods and eventually let it succeed.
    // This would not throw any exceptions if:
    // 1). exceptionMethod equals to null or NONE.
    // 2). exceptionTime is a very big or negative value.
    private void retryCheckAndCreateLeaseForNewShards(IKinesisProxy kinesisProxy,
            ExceptionThrowingLeaseManagerMethods exceptionMethod,
            int exceptionTime, InitialPositionInStreamExtended position)
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (exceptionMethod != null) {
            ExceptionThrowingLeaseManager exceptionThrowingLeaseManager =
                    new ExceptionThrowingLeaseManager(leaseManager);
            // Set exception and throwing time for exceptionThrowingManager.
            exceptionThrowingLeaseManager.setLeaseLeaseManagerThrowingExceptionScenario(exceptionMethod, exceptionTime);
            // Only need to try two times.
            for (int i = 1; i <= 2; i++) {
                try {
                    leaseManager.deleteAll();
                    shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy,
                            exceptionThrowingLeaseManager,
                            position,
                            cleanupLeasesOfCompletedShards,
                            false, null);
                    return;
                } catch (LeasingException e) {
                    LOG.debug("Catch leasing exception", e);
                }
                // Clear throwing exception scenario every time after calling KinesisShardSyncer
                exceptionThrowingLeaseManager.clearLeaseManagerThrowingExceptionScenario();
            }
        } else {
            shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy,
                    leaseManager,
                    position,
                    cleanupLeasesOfCompletedShards,
                    false, null);
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
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList(
                "shardId-8", "shardId-4", "shardId-9", "shardId-10"
        ));
        testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(null,
                Integer.MAX_VALUE, INITIAL_POSITION_AT_TIMESTAMP, expectedLeaseKeysToCreate);
    }

    /**
     * @throws KinesisClientLibIOException
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws IOException
     */
    @Test
    public final void testCheckAndCreateLeasesForNewShardsAtTimestampAndClosedShardWithDeleteLeaseExceptions()
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList(
                "shardId-8", "shardId-4", "shardId-9", "shardId-10"
        ));
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 10;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.DELETELEASE,
                    c, INITIAL_POSITION_AT_TIMESTAMP, expectedLeaseKeysToCreate);
            // Need to clean up lease manager every time after calling KinesisShardSyncer
            leaseManager.deleteAll();
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
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList(
                "shardId-8", "shardId-4", "shardId-9", "shardId-10"
        ));
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 10;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.LISTLEASES,
                    c, INITIAL_POSITION_AT_TIMESTAMP, expectedLeaseKeysToCreate);
            // Need to clean up lease manager every time after calling KinesisShardSyncer
            leaseManager.deleteAll();
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
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList(
                "shardId-8", "shardId-4", "shardId-9", "shardId-10"
        ));
        // Define the max calling count for lease manager methods.
        // From the Shard Graph, the max count of calling could be 10
        int maxCallingCount = 5;
        for (int c = 1; c <= maxCallingCount; c = c + 2) {
            testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
                    ExceptionThrowingLeaseManagerMethods.CREATELEASEIFNOTEXISTS,
                    c, INITIAL_POSITION_AT_TIMESTAMP, expectedLeaseKeysToCreate);
            // Need to clean up lease manager every time after calling KinesisShardSyncer
            leaseManager.deleteAll();
        }
    }

    // Real implementation of testing CheckAndCreateLeasesForNewShards with different leaseManager types.
    private void testCheckAndCreateLeasesForNewShardsAtSpecifiedPositionAndClosedShardImpl(
            ExceptionThrowingLeaseManagerMethods exceptionMethod,
            int exceptionTime,
            InitialPositionInStreamExtended position, Set<String> expectedLeaseKeysToCreate)
            throws KinesisClientLibIOException, DependencyException, InvalidStateException, ProvisionedThroughputException,
            IOException {
        ExtendedSequenceNumber extendedSequenceNumber =
                new ExtendedSequenceNumber(position.getInitialPositionInStream().toString());
        List<Shard> shards = constructShardListForGraphA();
        File dataFile = KinesisLocalFileDataCreator.generateTempDataFile(shards, 2, "testBootstrap1");
        dataFile.deleteOnExit();
        final IKinesisProxy kinesisProxy = spy(new KinesisLocalFileProxy(dataFile.getAbsolutePath()));
        when(kinesisProxy.getShardList()).thenReturn(shards);
        when(kinesisProxy.getShardListWithFilter(any())).thenReturn(getFilteredShards(shards, position));

        retryCheckAndCreateLeaseForNewShards(kinesisProxy, exceptionMethod, exceptionTime, position);

        List<KinesisClientLease> newLeases = leaseManager.listLeases();
        Map<String, ExtendedSequenceNumber> expectedShardIdToCheckpointMap =
                new HashMap<String, ExtendedSequenceNumber>();
        expectedLeaseKeysToCreate.forEach(l -> expectedShardIdToCheckpointMap.put(l, extendedSequenceNumber));

        Assert.assertEquals(expectedShardIdToCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease1 : newLeases) {
            ExtendedSequenceNumber expectedCheckpoint = expectedShardIdToCheckpointMap.get(lease1.getLeaseKey());
            Assert.assertNotNull(expectedCheckpoint);
            Assert.assertEquals(expectedCheckpoint, lease1.getCheckpoint());
        }

        dataFile.delete();
    }

    private void testBootstrapShardLeasesAtStartingPosition(InitialPositionInStreamExtended initialPosition)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException, IOException,
            KinesisClientLibIOException {
        List<Shard> shards = new ArrayList<Shard>();
        SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        String shardId0 = "shardId-0";
        shards.add(ShardObjectHelper.newShard(shardId0, null, null, sequenceRange, hashKeyRange));
        String shardId1 = "shardId-1";
        shards.add(ShardObjectHelper.newShard(shardId1, null, null, sequenceRange, hashKeyRange));
        File dataFile = KinesisLocalFileDataCreator.generateTempDataFile(shards, 2, "testBootstrap1");
        dataFile.deleteOnExit();
        IKinesisProxy kinesisProxy = new KinesisLocalFileProxy(dataFile.getAbsolutePath());

        shardSyncer.bootstrapShardLeases(kinesisProxy, leaseManager, initialPosition,
                false);
        List<KinesisClientLease> newLeases = leaseManager.listLeases();
        Assert.assertEquals(2, newLeases.size());
        Set<String> expectedLeaseShardIds = new HashSet<String>();
        expectedLeaseShardIds.add(shardId0);
        expectedLeaseShardIds.add(shardId1);
        for (KinesisClientLease lease1 : newLeases) {
            Assert.assertTrue(expectedLeaseShardIds.contains(lease1.getLeaseKey()));
            Assert.assertEquals(new ExtendedSequenceNumber(initialPosition.getInitialPositionInStream().toString()),
                    lease1.getCheckpoint());
        }
        dataFile.delete();
    }

    /**
     * Test determineNewLeasesToCreate() starting at latest and at trim horizon ("beginning" of shard)
     */
    @Test
    public final void testDetermineNewLeasesToCreateStartingPosition() {
        List<Shard> shards = new ArrayList<Shard>();
        List<KinesisClientLease> currentLeases = new ArrayList<KinesisClientLease>();
        SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        String shardId0 = "shardId-0";
        shards.add(ShardObjectHelper.newShard(shardId0, null, null, sequenceRange, hashKeyRange));

        String shardId1 = "shardId-1";
        shards.add(ShardObjectHelper.newShard(shardId1, null, null, sequenceRange, hashKeyRange));

        Set<InitialPositionInStreamExtended> initialPositions = new HashSet<InitialPositionInStreamExtended>();
        initialPositions.add(INITIAL_POSITION_LATEST);
        initialPositions.add(INITIAL_POSITION_TRIM_HORIZON);

        for (InitialPositionInStreamExtended initialPosition : initialPositions) {
            final LeaseSynchronizer leaseSynchronizer = getLeaseSynchronizer(shards, currentLeases);
            List<KinesisClientLease> newLeases =
                    shardSyncer.determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, initialPosition);
            Assert.assertEquals(2, newLeases.size());
            Set<String> expectedLeaseShardIds = new HashSet<String>();
            expectedLeaseShardIds.add(shardId0);
            expectedLeaseShardIds.add(shardId1);
            for (KinesisClientLease lease : newLeases) {
                Assert.assertTrue(expectedLeaseShardIds.contains(lease.getLeaseKey()));
                Assert.assertEquals(new ExtendedSequenceNumber(initialPosition.getInitialPositionInStream().toString()),
                        lease.getCheckpoint());
            }
        }
    }

    /**
     * Test determineNewLeasesToCreate() - 1 closed and 1 open shard (ignore closed shard), 1 shard with a lease
     * already in lease table. If lease table is non-empty, closed shards should be ignored.
     */
    @Test
    public final void testDetermineNewLeasesToCreateIgnoresClosedShardWithPartialLeaseTable() {
        final List<Shard> shardsWithoutLeases = new ArrayList<Shard>();
        final List<Shard> shardsWithLeases = new ArrayList<Shard>();
        final List<KinesisClientLease> currentLeases = new ArrayList<KinesisClientLease>();

        shardsWithoutLeases.add(ShardObjectHelper.newShard("shardId-0",
                null,
                null,
                ShardObjectHelper.newSequenceNumberRange("303", "404"),
                hashKeyRange));
        final String lastShardId = "shardId-1";
        shardsWithoutLeases.add(ShardObjectHelper.newShard(lastShardId,
                null,
                null,
                ShardObjectHelper.newSequenceNumberRange("405", null),
                hashKeyRange));

        shardsWithLeases.add(ShardObjectHelper.newShard("shardId-2",
                null,
                null,
                ShardObjectHelper.newSequenceNumberRange("202", "302"),
                hashKeyRange));
        currentLeases.add(newLease("shardId-2"));

        final List<Shard> allShards =
                Stream.concat(shardsWithLeases.stream(), shardsWithoutLeases.stream()).collect(Collectors.toList());
        final LeaseSynchronizer leaseSynchronizer = getLeaseSynchronizer(allShards, currentLeases);

        final List<KinesisClientLease> newLeases =
                shardSyncer.determineNewLeasesToCreate(leaseSynchronizer, shardsWithoutLeases, currentLeases, INITIAL_POSITION_LATEST);
        Assert.assertEquals(1, newLeases.size());
        Assert.assertEquals(lastShardId, newLeases.get(0).getLeaseKey());
    }

    /**
     * Test determineNewLeasesToCreate() - 1 closed and 1 open shard. Since lease table is empty, we should create
     * leases for all shards, regardless if they are open or closed.
     */
    @Test
    public final void testDetermineNewLeasesToCreateDoesntIgnoreClosedShardWithEmptyLeaseTable() {
        final List<Shard> shards = new ArrayList<>();
        final List<KinesisClientLease> currentLeases = new ArrayList<>();

        final String firstShardId = "shardId-0";
        shards.add(ShardObjectHelper.newShard(firstShardId,
                null,
                null,
                ShardObjectHelper.newSequenceNumberRange("303", "404"),
                hashKeyRange));
        final String lastShardId = "shardId-1";
        shards.add(ShardObjectHelper.newShard(lastShardId,
                null,
                null,
                ShardObjectHelper.newSequenceNumberRange("405", null),
                hashKeyRange));

        final LeaseSynchronizer leaseSynchronizer = getLeaseSynchronizer(shards, currentLeases);

        final List<KinesisClientLease> newLeases =
                shardSyncer.determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, INITIAL_POSITION_LATEST);
        Assert.assertEquals(2, newLeases.size());

        final Set<String> expectedLeaseShardIds = new HashSet<>();
        expectedLeaseShardIds.add(firstShardId);
        expectedLeaseShardIds.add(lastShardId);
        for (KinesisClientLease lease : newLeases) {
            Assert.assertTrue(expectedLeaseShardIds.contains(lease.getLeaseKey()));
        }
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position Latest)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     * Initial position: LATEST
     * Leases to create: (2, 6)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeLatestA_PartialHashRange1() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-3", "shardId-4", "shardId-5");

        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap =
                new HashMap<String, ExtendedSequenceNumber>();
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.LATEST);

        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position Latest)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 7)
     * Initial position: LATEST
     * Leases to create: (6)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeLatestA_PartialHashRange2() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-7");

        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap =
                new HashMap<String, ExtendedSequenceNumber>();
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.LATEST);

        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position TrimHorizon)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (2, 6)
     * Initial position: LATEST
     * Leases to create: (3, 4, 9, 10)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeLatestA_PartialHashRange3() {
        List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-2", "shardId-6");

        Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap =
                new HashMap<String, ExtendedSequenceNumber>();
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.LATEST);

        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position TrimHorizon)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 9, 10)
     * Initial position: LATEST
     * Leases to create: (8)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeLatestA_PartialHashRange4() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-9", "shardId-10");

        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap =
                new HashMap<String, ExtendedSequenceNumber>();
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.LATEST);

        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Helper method to construct a shard list for graph C. Graph C is defined below. Shard structure (y-axis is
     * epochs):     0      1  2  3  - shards till
     *            /   \    |  \ /
     *           4     5   1   6  - shards from epoch 103 - 205
     *          / \   / \  |   |
     *         7   8 9  10 1   6
     * shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (9, 10)
     * Initial position: LATEST
     * Expected leases: (1, 6, 7, 8)
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeLatestC_PartialHashRange5() {
        final List<Shard> shards = constructShardListForGraphC();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-9", "shardId-10");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.LATEST);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors (initial position AT_TIMESTAMP)
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 6, 7)
     * Initial position: LATEST
     * Leases to create: empty set
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeLatestA_CompleteHashRange() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-6", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 2, 3, 4, 5, 6, 7)
     * Initial position: LATEST
     * Expected leases: empty set
     */
    @Test
    public final void testDetermineNewLeasesToCreateSplitMergeLatestA_CompleteHashRangeWithoutGC() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-0", "shardId-1", "shardId-2",
                "shardId-3", "shardId-4", "shardId-5", "shardId-6", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = Collections.emptyMap();

        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: empty set
     * Initial position: LATEST
     * Expected leases: (4, 8, 9, 10)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestA_EmptyLeaseTable() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Collections.emptyList();
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.LATEST);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 4, 7, 9, 10)
     * Initial position: LATEST
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestA_CompleteHashRangeAcrossDifferentEpochs() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-0", "shardId-1", "shardId-4", "shardId-7",
                "shardId-9", "shardId-10");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedNoNewLeases);
    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: (6)
     * Initial position: LATEST
     * Expected leases: (7)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestB_PartialHashRange() {
        final List<Shard> shards = constructShardListForGraphB();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-6");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.LATEST);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: (5)
     * Initial position: LATEST
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestB_CompleteHashRange() {
        final List<Shard> shards = constructShardListForGraphB();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedNoNewLeases);
    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: (0, 1, 2, 3, 4, 5)
     * Initial position: LATEST
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestB_CompleteHashRangeWithoutGC() {
        final List<Shard> shards = constructShardListForGraphB();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-0", "shardId-1", "shardId-2", "shardId-3",
                "shardId-4", "shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedNoNewLeases);
    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: empty set
     * Initial position: LATEST
     * Expected leases: (9, 10)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestB_EmptyLeaseTable() {
        final List<Shard> shards = constructShardListForGraphB();
        final List<String> shardIdsOfCurrentLeases = Collections.emptyList();
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.LATEST);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     * Initial position: TRIM_HORIZON
     * Expected leases: (0, 1, 2)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_PartialHashRange1() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-3", "shardId-4", "shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.TRIM_HORIZON);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 7)
     * Initial position: TRIM_HORIZON
     * Expected leases: (0, 1)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_PartialHashRange2() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (2, 6)
     * Initial position: TRIM_HORIZON
     * Expected leases: (3, 4, 5)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_PartialHashRange3() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-2", "shardId-6");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-5", ExtendedSequenceNumber.TRIM_HORIZON);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 9, 10)
     * Initial position: TRIM_HORIZON
     * Expected leases: (0, 1, 2, 3)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_PartialHashRange4() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-9", "shardId-10");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.TRIM_HORIZON);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 6, 7)
     * Initial position: TRIM_HORIZON
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_CompleteHashRange() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-6", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedNoNewLeases);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 2, 3, 4, 5, 6, 7)
     * Initial position: TRIM_HORIZON
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_CompleteHashRangeWithoutGC() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-0", "shardId-1", "shardId-2", "shardId-3",
                "shardId-4", "shardId-5", "shardId-6", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedNoNewLeases);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: empty set
     * Initial position: TRIM_HORIZON
     * Expected leases: (0, 1, 2, 3, 4, 5)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_EmptyLeaseTable() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Collections.emptyList();
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-5", ExtendedSequenceNumber.TRIM_HORIZON);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 4, 7, 9, 10)
     * Initial position: TRIM_HORIZON
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_CompleteHashRangeAcrossDifferentEpochs() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-0", "shardId-1", "shardId-4", "shardId-7",
                "shardId-9", "shardId-10");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedNoNewLeases);
    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: (6)
     * Initial position: TRIM_HORIZON
     * Expected leases: (7)
     */
//    TODO: Account for out-of-order lease creation in TRIM_HORIZON and AT_TIMESTAMP cases
//    @Test
//    public void testDetermineNewLeasesToCreateSplitMergeHorizonB_PartialHashRange() {
//        final List<Shard> shards = constructShardListForGraphB();
//        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-6");
//        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
//        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.TRIM_HORIZON);
//        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
//    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: (5)
     * Initial position: TRIM_HORIZON
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonB_CompleteHashRange() {
        final List<Shard> shards = constructShardListForGraphB();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedNoNewLeases);
    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: (0, 1, 2, 3, 4, 5)
     * Initial position: TRIM_HORIZON
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonB_CompleteHashRangeWithoutGC() {
        final List<Shard> shards = constructShardListForGraphB();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-0", "shardId-1", "shardId-2", "shardId-3",
                "shardId-4", "shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedNoNewLeases);
    }

    /**
     * CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)p
     *
     * Current leases: empty set
     * Initial position: TRIM_HORIZON
     * Expected leases: (0, 1)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonB_EmptyLeaseTable() {
        final List<Shard> shards = constructShardListForGraphB();
        final List<String> shardIdsOfCurrentLeases = Collections.emptyList();
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (0, 1, 2)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_PartialHashRange1() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-3", "shardId-4", "shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.AT_TIMESTAMP);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 7)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (0, 1)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_PartialHashRange2() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (2, 6)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (3, 4, 5)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_PartialHashRange3() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-2", "shardId-6");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-5", ExtendedSequenceNumber.AT_TIMESTAMP);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 9, 10)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (0, 1, 2, 3)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_PartialHashRange4() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-9", "shardId-10");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.AT_TIMESTAMP);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 6, 7)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_CompleteHashRange() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-6", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedNoNewLeases);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 2, 3, 4, 5, 6, 7)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_CompleteHashRangeWithoutGC() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-0", "shardId-1", "shardId-2", "shardId-3",
                "shardId-4", "shardId-5", "shardId-6", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedNoNewLeases);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: empty set
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (0, 1, 2, 3, 4, 5)     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_EmptyLeaseTable() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Collections.emptyList();
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-5", ExtendedSequenceNumber.AT_TIMESTAMP);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 4, 7, 9, 10)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_CompleteHashRangeAcrossDifferentEpochs() {
        final List<Shard> shards = constructShardListForGraphA();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-0", "shardId-1", "shardId-4", "shardId-7",
                "shardId-9", "shardId-10");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedNoNewLeases);
    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: (6)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (7)
     */
//    TODO: Account for out-of-order lease creation in TRIM_HORIZON and AT_TIMESTAMP cases
//    @Test
//    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampB_PartialHashRange() {
//        final List<Shard> shards = constructShardListForGraphB();
//        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-6");
//        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
//        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.AT_TIMESTAMP);
//        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
//    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: (5)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampB_CompleteHashRange() {
        final List<Shard> shards = constructShardListForGraphB();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedNoNewLeases);
    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: (0, 1, 2, 3, 4, 5)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: empty set
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampB_CompleteHashRangeWithoutGC() {
        final List<Shard> shards = constructShardListForGraphB();
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-0", "shardId-1", "shardId-2", "shardId-3",
                "shardId-4", "shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedNoNewLeases = Collections.emptyMap();
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedNoNewLeases);
    }

    /**
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
     *
     * Current leases: empty set
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (0, 1)
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampB_EmptyLeaseTable() {
        final List<Shard> shards = constructShardListForGraphB();
        final List<String> shardIdsOfCurrentLeases = Collections.emptyList();
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);
        testCheckIfDescendantAndAddNewLeasesForAncestors(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Helper method to construct a shard list for graph A. Graph A is defined below.
     * Shard structure (y-axis is epochs):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 210
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 211 (open - no ending sequenceNumber)
     */
    List<Shard> constructShardListForGraphA() {
        List<Shard> shards = new ArrayList<Shard>();

        SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("11", "102");
        SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("11", null);
        SequenceNumberRange range2 = ShardObjectHelper.newSequenceNumberRange("11", "210");
        SequenceNumberRange range3 = ShardObjectHelper.newSequenceNumberRange("103", "210");
        SequenceNumberRange range4 = ShardObjectHelper.newSequenceNumberRange("211", null);

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

    /**
     * Helper method to construct a shard list for graph B. Graph B is defined below.
     * Shard structure (x-axis is epochs):
     * 0   1    shards till epoch 1049
     *  \ /
     *   2      shards from epoch 1050 - 1099
     *  / \
     * 3   4    shards from epoch 1100 - 1149
     *  \ /
     *   5      shards from epoch 1150 - 1199
     *  / \
     * 6   7    shards from epoch 1200 - 1249
     *  \ /
     *   8      shards from epoch 1250 - 1299
     *  / \
     * 9   10   shards from epoch 1300 (open - no ending sequence number)
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
     * Helper method to construct a shard list for graph C. Graph C is defined below. Shard structure (y-axis is
     * epochs):     0      1  2  3  - shards till
     *            /   \    |  \ /
     *           4     5   1   6  - shards from epoch 103 - 205
     *          / \   / \  |   |
     *         7   8 9  10 1   6
     * shards from epoch 206 (open - no ending sequenceNumber)
     */
    private List<Shard> constructShardListForGraphC() {
        final SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("11", "102");
        final SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("11", null);
        final SequenceNumberRange range2 = ShardObjectHelper.newSequenceNumberRange("103", null);
        final SequenceNumberRange range3 = ShardObjectHelper.newSequenceNumberRange("103", "205");
        final SequenceNumberRange range4 = ShardObjectHelper.newSequenceNumberRange("206", null);

        return Arrays.asList(
                ShardObjectHelper.newShard("shardId-0", null, null, range0,
                        ShardObjectHelper.newHashKeyRange("0", "399")),
                ShardObjectHelper.newShard("shardId-1", null, null, range1,
                        ShardObjectHelper.newHashKeyRange("400", "499")),
                ShardObjectHelper.newShard("shardId-2", null, null, range0,
                        ShardObjectHelper.newHashKeyRange("500", "599")),
                ShardObjectHelper.newShard("shardId-3", null, null, range0,
                        ShardObjectHelper.newHashKeyRange("600", ShardObjectHelper.MAX_HASH_KEY)),
                ShardObjectHelper.newShard("shardId-4", "shardId-0", null, range3,
                        ShardObjectHelper.newHashKeyRange("0", "199")),
                ShardObjectHelper.newShard("shardId-5", "shardId-0", null, range3,
                        ShardObjectHelper.newHashKeyRange("200", "399")),
                ShardObjectHelper.newShard("shardId-6", "shardId-2", "shardId-3", range2,
                        ShardObjectHelper.newHashKeyRange("500", ShardObjectHelper.MAX_HASH_KEY)),
                ShardObjectHelper.newShard("shardId-7", "shardId-4", null, range4,
                        ShardObjectHelper.newHashKeyRange("0", "99")),
                ShardObjectHelper.newShard("shardId-8", "shardId-4", null, range4,
                        ShardObjectHelper.newHashKeyRange("100", "199")),
                ShardObjectHelper.newShard("shardId-9", "shardId-5", null, range4,
                        ShardObjectHelper.newHashKeyRange("200", "299")),
                ShardObjectHelper.newShard("shardId-10", "shardId-5", null, range4,
                        ShardObjectHelper.newHashKeyRange("300", "399")));
    }


    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors when shardId is null
     */
    @Test
    public final void testCheckIfDescendantAndAddNewLeasesForAncestorsNullShardId() {
        final MemoizationContext memoizationContext = new MemoizationContext();
        Assert.assertFalse(shardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(null, INITIAL_POSITION_LATEST,
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
        final MemoizationContext memoizationContext = new MemoizationContext();
        Assert.assertFalse(shardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId, INITIAL_POSITION_LATEST,
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
        final MemoizationContext memoizationContext = new MemoizationContext();
        Assert.assertTrue(shardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId, INITIAL_POSITION_LATEST,
                shardIdsOfCurrentLeases,
                kinesisShards,
                newLeaseMap,
                memoizationContext));
        Assert.assertTrue(newLeaseMap.isEmpty());
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

        final MemoizationContext memoizationContext = new MemoizationContext();
        Assert.assertFalse(shardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId, INITIAL_POSITION_LATEST,
                shardIdsOfCurrentLeases,
                kinesisShards,
                newLeaseMap,
                memoizationContext));
        Assert.assertTrue(newLeaseMap.isEmpty());
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
        kinesisShards.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null, hashKeyRange));
        shardIdsOfCurrentLeases.add(parentShardId);

        String adjacentParentShardId = "shardId-adjacentParent";
        kinesisShards.put(adjacentParentShardId, ShardObjectHelper.newShard(adjacentParentShardId, null, null, null, hashKeyRange));

        String shardId = "shardId-9-1";
        Shard shard = ShardObjectHelper.newShard(shardId, parentShardId, adjacentParentShardId, null, hashKeyRange);
        kinesisShards.put(shardId, shard);

        final MemoizationContext memoizationContext = new MemoizationContext();
        Assert.assertTrue(shardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId, INITIAL_POSITION_LATEST,
                shardIdsOfCurrentLeases,
                kinesisShards,
                newLeaseMap,
                memoizationContext));
        Assert.assertEquals(1, newLeaseMap.size());
        Assert.assertTrue(newLeaseMap.containsKey(adjacentParentShardId));
        KinesisClientLease adjacentParentLease = newLeaseMap.get(adjacentParentShardId);
        Assert.assertEquals(ExtendedSequenceNumber.LATEST, adjacentParentLease.getCheckpoint());
    }

    /**
     * Test getParentShardIds() when the shard has no parents.
     */
    @Test
    public final void testGetParentShardIdsNoParents() {
        Shard shard = new Shard();
        Assert.assertTrue(shardSyncer.getParentShardIds(shard, null).isEmpty());
    }

    /**
     * Test getParentShardIds() when the shard has no parents.
     */
    @Test
    public final void testGetParentShardIdsTrimmedParents() {
        Map<String, Shard> shardMap = new HashMap<String, Shard>();
        Shard shard = ShardObjectHelper.newShard("shardId-test", "foo", "bar", null);
        Assert.assertTrue(shardSyncer.getParentShardIds(shard, shardMap).isEmpty());
    }

    /**
     * Test getParentShardIds() when the shard has a single parent.
     */
    @Test
    public final void testGetParentShardIdsSingleParent() {
        Map<String, Shard> shardMap = new HashMap<String, Shard>();

        String parentShardId = "shardId-parent";
        shardMap.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null));

        Shard shard = ShardObjectHelper.newShard("shardId-test", parentShardId, null, null);
        Set<String> parentShardIds = shardSyncer.getParentShardIds(shard, shardMap);
        Assert.assertEquals(1, parentShardIds.size());
        Assert.assertTrue(parentShardIds.contains(parentShardId));

        shard.setParentShardId(null);
        parentShardIds = shardSyncer.getParentShardIds(shard, shardMap);
        Assert.assertTrue(parentShardIds.isEmpty());

        shard.setAdjacentParentShardId(parentShardId);
        parentShardIds = shardSyncer.getParentShardIds(shard, shardMap);
        Assert.assertEquals(1, parentShardIds.size());
        Assert.assertTrue(parentShardIds.contains(parentShardId));
    }

    /**
     * Test getParentShardIds() when the shard has two parents, one is trimmed.
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
        Set<String> parentShardIds = shardSyncer.getParentShardIds(shard, shardMap);
        Assert.assertEquals(1, parentShardIds.size());
        Assert.assertTrue(parentShardIds.contains(parentShardId));

        shardMap.remove(parentShardId);
        parentShardIds = shardSyncer.getParentShardIds(shard, shardMap);
        Assert.assertTrue(parentShardIds.isEmpty());

        shardMap.put(adjacentParentShardId, adjacentParent);
        parentShardIds = shardSyncer.getParentShardIds(shard, shardMap);
        Assert.assertEquals(1, parentShardIds.size());
        Assert.assertTrue(parentShardIds.contains(adjacentParentShardId));
    }

    /**
     * Test getParentShardIds() when the shard has two parents.
     */
    @Test
    public final void testGetParentShardIdsTwoParents() {
        Map<String, Shard> shardMap = new HashMap<String, Shard>();

        String parentShardId = "shardId-parent";
        shardMap.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null));

        String adjacentParentShardId = "shardId-adjacentParent";
        shardMap.put(adjacentParentShardId, ShardObjectHelper.newShard(adjacentParentShardId, null, null, null));

        Shard shard = ShardObjectHelper.newShard("shardId-test", parentShardId, adjacentParentShardId, null);

        Set<String> parentShardIds = shardSyncer.getParentShardIds(shard, shardMap);
        Assert.assertEquals(2, parentShardIds.size());
        Assert.assertTrue(parentShardIds.contains(parentShardId));
        Assert.assertTrue(parentShardIds.contains(adjacentParentShardId));
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
        shard.setHashKeyRange(hashKeyRange);

        KinesisClientLease lease = shardSyncer.newKCLLease(shard);
        Assert.assertEquals(shardId, lease.getLeaseKey());
        Assert.assertNull(lease.getCheckpoint());
        Set<String> parentIds = lease.getParentShardIds();
        Assert.assertEquals(2, parentIds.size());
        Assert.assertTrue(parentIds.contains(parentShardId));
        Assert.assertTrue(parentIds.contains(adjacentParentShardId));
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

        Map<String, Shard> shardIdToShardMap = shardSyncer.constructShardIdToShardMap(shards);
        Assert.assertEquals(shards.size(), shardIdToShardMap.size());
        for (Shard shard : shards) {
            Assert.assertSame(shard, shardIdToShardMap.get(shard.getShardId()));
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
        Assert.assertTrue(shardSyncer.getOpenShards(shards).isEmpty());
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
        List<Shard> openShards = shardSyncer.getOpenShards(shards);
        Assert.assertEquals(1, openShards.size());
        Assert.assertEquals(shardId, openShards.get(0).getShardId());

        // Close shard before testing for max sequence number
        sequenceNumberRange.setEndingSequenceNumber("1000");
        openShards = shardSyncer.getOpenShards(shards);
        Assert.assertTrue(openShards.isEmpty());

        // Verify shard is considered closed when the end sequence number is set to max allowed sequence number
        sequenceNumberRange.setEndingSequenceNumber(MAX_SEQUENCE_NUMBER.toString());
        openShards = shardSyncer.getOpenShards(shards);
        Assert.assertEquals(0, openShards.size());
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
        Assert.assertFalse(leaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds));

        currentKinesisShardIds.clear();
        Assert.assertTrue(leaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds));

        currentKinesisShardIds.add(parentShardId);
        // Assert.assertFalse(leaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds));

        currentKinesisShardIds.clear();
        Assert.assertTrue(leaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds));

        currentKinesisShardIds.add(adjacentParentShardId);
        // Assert.assertFalse(leaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds));
        currentKinesisShardIds.add(parentShardId);
        // Assert.assertFalse(leaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds));
        currentKinesisShardIds.add(shardId);
        Assert.assertFalse(leaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds));
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
        Assert.assertFalse(leaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds));
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
        Assert.assertFalse(leaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds));
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
        Map<String, Shard> shardIdToShardMap = shardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                shardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> closedShardIds = new HashSet<>();
        closedShardIds.add(expectedClosedShardId);
        shardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);

        // test for case where shard has been trimmed (absent from list)
        shardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);

        // Populate shards.
        shards.add(closedShard);
        shards.add(child1);
        shardIdToShardMap.put(expectedClosedShardId, closedShard);
        shardIdToShardMap.put(child1.getShardId(), child1);
        shardIdToChildShardIdsMap = shardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);

        // test degenerate split/merge
        child1.setHashKeyRange(hashKeyRange);
        shardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);

        // test merge
        child1.setHashKeyRange(ShardObjectHelper.newHashKeyRange("10", "2985"));
        shardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);
        child1.setHashKeyRange(ShardObjectHelper.newHashKeyRange("3", "25"));
        shardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);

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
        shardIdToChildShardIdsMap = shardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        shardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);
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
        Map<String, Shard> shardIdToShardMap = shardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                shardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> closedShardIds = new HashSet<>();
        closedShardIds.add(expectedClosedShardId);
        shardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);
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
        Map<String, Shard> shardIdToShardMap = shardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                shardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> closedShardIds = new HashSet<>();
        closedShardIds.add(expectedClosedShardId);
        shardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);
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

    /**
     * Tests that when reading from TIP, we use the AT_LATEST shard filter
     * @throws Exception
     */
    @Test
    public final void testEmptyLeaseTableBootstrapUsesShardFilterWithAtLatest() throws Exception {
        ShardFilter shardFilter = new ShardFilter().withType(ShardFilterType.AT_LATEST);
        testEmptyLeaseTableUsesListShardsWithFilter(INITIAL_POSITION_LATEST, shardFilter);
    }

    /**
     * Tests that when reading from TRIM, we use the TRIM_HORIZON shard filter
     * @throws Exception
     */
    @Test
    public final void testEmptyLeaseTableBootstrapUsesShardFilterWithAtTrimHorizon() throws Exception {
        ShardFilter shardFilter = new ShardFilter().withType(ShardFilterType.AT_TRIM_HORIZON);
        testEmptyLeaseTableUsesListShardsWithFilter(INITIAL_POSITION_TRIM_HORIZON, shardFilter);
    }

    /**
     * Tests that when reading from AT_TIMESTAMP, we use the AT_TIMESTAMP shard filter
     * @throws Exception
     */
    @Test
    public final void testEmptyLeaseTableBootstrapUsesShardFilterWithAtTimestamp() throws Exception {
        ShardFilter shardFilter = new ShardFilter().withType(ShardFilterType.AT_TIMESTAMP).withTimestamp(new Date(1000L));
        testEmptyLeaseTableUsesListShardsWithFilter(INITIAL_POSITION_AT_TIMESTAMP, shardFilter);
    }

    private void testEmptyLeaseTableUsesListShardsWithFilter(InitialPositionInStreamExtended initialPosition,
                                                             ShardFilter shardFilter) throws Exception {

        final List<Shard> shards = constructShardListForGraphA();
        final File dataFile = KinesisLocalFileDataCreator.generateTempDataFile(shards, 2, "testBootstrap1");
        dataFile.deleteOnExit();
        final IKinesisProxy kinesisProxy = spy(new KinesisLocalFileProxy(dataFile.getAbsolutePath()));

        // Make sure ListShardsWithFilter is called in all public shard sync methods
        shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, initialPosition,
                cleanupLeasesOfCompletedShards, false);

        leaseManager.deleteAll();

        shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, initialPosition,
                cleanupLeasesOfCompletedShards, false, null);

        verify(kinesisProxy, times(2)).getShardListWithFilter(shardFilter);
        verify(kinesisProxy, never()).getShardList();
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

        Map<String, Shard> shardIdToShardMap = shardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                shardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> closedShardIds = new HashSet<>();
        closedShardIds.add(expectedClosedShardId);
        shardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, closedShardIds);
    }

    /*
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Initial position: TRIM_HORIZON
     * Leases to create: (0, 1, 2, 3, 4, 5)
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTrimHorizonWithEmptyLeaseTable() throws Exception {
        final List<Shard> shards = constructShardListForGraphA();
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList("shardId-0", "shardId-1", "shardId-2",
                "shardId-3", "shardId-4", "shardId-5"));
        testCheckAndCreateLeaseForShardsIfMissing(shards, INITIAL_POSITION_TRIM_HORIZON, expectedLeaseKeysToCreate);
    }

    /*
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Initial position: AT_TIMESTAMP(1000)
     * Leases to create: (8, 4, 9, 10)
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTimestampWithEmptyLeaseTable1() throws Exception {
        final List<Shard> shards = constructShardListForGraphA();
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList("shardId-8", "shardId-4", "shardId-9",
                "shardId-10"));
        testCheckAndCreateLeaseForShardsIfMissing(shards, INITIAL_POSITION_AT_TIMESTAMP, expectedLeaseKeysToCreate);
    }

    /*
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Initial position: AT_TIMESTAMP(200)
     * Leases to create: (6, 7, 4, 5)
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTimestampWithEmptyLeaseTable2() throws Exception {
        final List<Shard> shards = constructShardListForGraphA();
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList("shardId-6", "shardId-7", "shardId-4",
                "shardId-5"));
        final InitialPositionInStreamExtended initialPosition = InitialPositionInStreamExtended
                .newInitialPositionAtTimestamp(new Date(200L));
        testCheckAndCreateLeaseForShardsIfMissing(shards, initialPosition, expectedLeaseKeysToCreate);
    }

    /*
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Initial position: LATEST
     * Leases to create: (8, 4, 9, 10)
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtLatestWithEmptyLeaseTable2() throws Exception {
        final List<Shard> shards = constructShardListForGraphA();
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList("shardId-8", "shardId-4", "shardId-9",
                "shardId-10"));
        testCheckAndCreateLeaseForShardsIfMissing(shards, INITIAL_POSITION_LATEST, expectedLeaseKeysToCreate);
    }

    private void testCheckAndCreateLeaseForShardsIfMissing(List<Shard> shards,InitialPositionInStreamExtended initialPositionInStreamExtended,
                                                           Set<String> expectedLeaseKeys) throws Exception {
        testCheckAndCreateLeaseForShardsIfMissing(shards, initialPositionInStreamExtended, expectedLeaseKeys, Collections.emptyList());
    }

    private void testCheckAndCreateLeaseForShardsIfMissing(List<Shard> shards,InitialPositionInStreamExtended initialPositionInStreamExtended,
                                                           Set<String> expectedLeaseKeys, List<KinesisClientLease> existingLeases) throws Exception {
        final File dataFile = KinesisLocalFileDataCreator.generateTempDataFile(shards, 0, "fileName");
        dataFile.deleteOnExit();
        final IKinesisProxy kinesisProxy = spy(new KinesisLocalFileProxy(dataFile.getAbsolutePath()));
        when(kinesisProxy.getShardList()).thenReturn(shards);
        when(kinesisProxy.getShardListWithFilter(any())).thenReturn(getFilteredShards(shards, initialPositionInStreamExtended));

        // Populate existing leases
        for (KinesisClientLease lease : existingLeases) {
            leaseManager.createLeaseIfNotExists(lease);
        }

        List<KinesisClientLease> oldLeases = leaseManager.listLeases();
        shardSyncer.checkAndCreateLeasesForNewShards(kinesisProxy, leaseManager, initialPositionInStreamExtended,
                false, false);
        List<KinesisClientLease> newLeases = leaseManager.listLeases();
        newLeases.removeAll(oldLeases);

        final Set<String> newLeaseKeys = newLeases.stream().map(Lease::getLeaseKey).collect(Collectors.toSet());
        final Set<ExtendedSequenceNumber> newSequenceNumbers = newLeases.stream().map(KinesisClientLease::getCheckpoint).collect(Collectors.toSet());
        final Set<ExtendedSequenceNumber> expectedSequenceNumbers = new HashSet<>(Collections
                .singletonList(new ExtendedSequenceNumber(initialPositionInStreamExtended.getInitialPositionInStream().name())));

        assertThat(newLeases.size(), equalTo(expectedLeaseKeys.size()));
        assertThat(newLeaseKeys, equalTo(expectedLeaseKeys));
        assertThat(newSequenceNumbers, equalTo(expectedSequenceNumbers));

        dataFile.delete();
    }

    /*
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Missing leases: (0, 6, 8)
     * Initial position: TRIM_HORIZON
     * Leases to create: (0)
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTrimHorizonWithPartialLeaseTable() throws Exception {
        final List<Shard> shards = constructShardListForGraphA();
        // Leases for shard-0 and its descendants (shard-6, and shard-8) are missing. Expect lease sync to recover the
        // lease for shard-0 when reading from TRIM_HORIZON.
        final Set<String> missingLeaseKeys = new HashSet<>(Arrays.asList("shardId-0", "shardId-6", "shardId-8"));
        final List<Shard> shardsWithLeases = shards.stream()
                .filter(s -> !missingLeaseKeys.contains(s.getShardId())).collect(Collectors.toList());
        final List<KinesisClientLease> existingLeases = createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.TRIM_HORIZON, LEASE_OWNER);

        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList("shardId-0"));
        testCheckAndCreateLeaseForShardsIfMissing(shards, INITIAL_POSITION_TRIM_HORIZON, expectedLeaseKeysToCreate, existingLeases);
    }


    /*
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Missing leases: (0, 6, 8)
     * Initial position: AT_TIMESTAMP(1000)
     * Leases to create: (0)
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTimestampWithPartialLeaseTable1() throws Exception {
        final List<Shard> shards = constructShardListForGraphA();
        // Leases for shard-0 and its descendants (shard-6, and shard-8) are missing. Expect lease sync to recover the
        // lease for shard-0 when reading from AT_TIMESTAMP.
        final Set<String> missingLeaseKeys = new HashSet<>(Arrays.asList("shardId-0", "shardId-6", "shardId-8"));
        final List<Shard> shardsWithLeases = shards.stream()
                .filter(s -> !missingLeaseKeys.contains(s.getShardId())).collect(Collectors.toList());
        final List<KinesisClientLease> existingLeases = createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.AT_TIMESTAMP, LEASE_OWNER);

        final Set<String> expectedLeaseKeys= new HashSet<>(Arrays.asList("shardId-0"));
        testCheckAndCreateLeaseForShardsIfMissing(shards, INITIAL_POSITION_AT_TIMESTAMP, expectedLeaseKeys, existingLeases);
    }

    /*
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Missing leases: (0, 6, 8)
     * Initial position: AT_TIMESTAMP(200)
     * Leases to create: (0)
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTimestampWithPartialLeaseTable2() throws Exception {
        final List<Shard> shards = constructShardListForGraphA();
        final InitialPositionInStreamExtended initialPosition = InitialPositionInStreamExtended
                .newInitialPositionAtTimestamp(new Date(200L));
        // Leases for shard-0 and its descendants (shard-6, and shard-8) are missing. Expect lease sync to recover the
        // lease for shard-0 when reading from AT_TIMESTAMP.
        final Set<String> missingLeaseKeys = new HashSet<>(Arrays.asList("shardId-0", "shardId-6", "shardId-8"));
        final List<Shard> shardsWithLeases = shards.stream()
                .filter(s -> !missingLeaseKeys.contains(s.getShardId())).collect(Collectors.toList());
        final List<KinesisClientLease> existingLeases = createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.AT_TIMESTAMP, LEASE_OWNER);

        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList("shardId-0"));
        testCheckAndCreateLeaseForShardsIfMissing(shards, initialPosition, expectedLeaseKeysToCreate, existingLeases);
    }

    /*
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Missing leases: (0, 6, 8)
     * Initial position: LATEST
     * Leases to create: (0)
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtLatestWithPartialLeaseTable2() throws Exception {
        final List<Shard> shards = constructShardListForGraphA();
        // Leases for shard-0 and its descendants (shard-6, and shard-8) are missing. Expect lease sync to recover the
        // lease for shard-0 when reading from LATEST.
        final Set<String> missingLeaseKeys = new HashSet<>(Arrays.asList("shardId-0", "shardId-6", "shardId-8"));
        final List<Shard> shardsWithLeases = shards.stream()
                .filter(s -> !missingLeaseKeys.contains(s.getShardId())).collect(Collectors.toList());
        final List<KinesisClientLease> existingLeases = createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.LATEST, LEASE_OWNER);

        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(Arrays.asList("shardId-0"));
        testCheckAndCreateLeaseForShardsIfMissing(shards, INITIAL_POSITION_LATEST, expectedLeaseKeysToCreate, existingLeases);
    }



    private List<KinesisClientLease> createLeasesFromShards(final List<Shard> shards, final ExtendedSequenceNumber checkpoint,
                                               final String leaseOwner) {
        return shards.stream().map(shard -> {
            final Set<String> parentShardIds = new HashSet<>();
            if (StringUtils.isNotEmpty(shard.getParentShardId())) {
                parentShardIds.add(shard.getParentShardId());
            }
            if (StringUtils.isNotEmpty(shard.getAdjacentParentShardId())) {
                parentShardIds.add(shard.getAdjacentParentShardId());
            }

            final KinesisClientLease lease = new KinesisClientLease();
            lease.setLeaseKey(shard.getShardId());
            lease.setLeaseOwner(leaseOwner);
            lease.setLeaseCounter(0L);
            lease.setLastCounterIncrementNanos(0L);
            lease.setCheckpoint(checkpoint);
            lease.setOwnerSwitchesSinceCheckpoint(0L);
            lease.setParentShardIds(parentShardIds);

            return lease;
        }).collect(Collectors.toList());
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

    /**
     * Helper method to test CheckIfDescendantAndAddNewLeasesForAncestors and verify new leases created with an expected result.
     * @param shards
     * @param shardIdsOfCurrentLeases
     * @param checkpoint
     * @param expectedShardIdCheckpointMap
     */
    private void testCheckIfDescendantAndAddNewLeasesForAncestors(List<Shard> shards, List<String> shardIdsOfCurrentLeases,
                                                                  InitialPositionInStreamExtended checkpoint, Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap) {
        final List<KinesisClientLease> currentLeases = shardIdsOfCurrentLeases.stream()
                .map(shardId -> newLease(shardId)).collect(Collectors.toList());
        final Map<String, Shard> shardIdToShardMap = KinesisShardSyncer.constructShardIdToShardMap(shards);
        final Map<String, Set<String>> shardIdToChildShardIdsMap =
                KinesisShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);

        final LeaseSynchronizer leaseSynchronizer = new NonEmptyLeaseTableSynchronizer(shardIdToShardMap, shardIdToChildShardIdsMap);
        final List<KinesisClientLease> newLeases =
                shardSyncer.determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, checkpoint);

        Assert.assertEquals(expectedShardIdCheckpointMap.size(), newLeases.size());
        for (KinesisClientLease lease : newLeases) {
            Assert.assertTrue("Unexpected lease: " + lease,
                    expectedShardIdCheckpointMap.containsKey(lease.getLeaseKey()));
            Assert.assertEquals(expectedShardIdCheckpointMap.get(lease.getLeaseKey()), lease.getCheckpoint());
        }
    }

    /**
     * Helper method to get appropriate LeaseSynchronizer based on available shards and current leases. If there are
     * no current leases (empty lease table case), return EmptyLeaseTableSynchronizer. Else, return
     * NonEmptyLeaseTableSynchronizer with appropriate lease mappings.
     *
     * @param shards
     * @param currentLeases
     * @return
     */
    private LeaseSynchronizer getLeaseSynchronizer(List<Shard> shards, List<KinesisClientLease> currentLeases) {
        if (currentLeases.isEmpty()) {
            return new EmptyLeaseTableSynchronizer();
        }

        final Map<String, Shard> shardIdToShardMap = KinesisShardSyncer.constructShardIdToShardMap(shards);
        final Map<String, Set<String>> shardIdToChildShardIdsMap =
                KinesisShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);

        return new NonEmptyLeaseTableSynchronizer(shardIdToShardMap, shardIdToChildShardIdsMap);
    }


    /**
     * Helper method to mimic behavior of Kinesis ListShardsWithFilter calls.
     */
    private static List<Shard> getFilteredShards(List<Shard> shards, InitialPositionInStreamExtended initialPosition) {
        switch (initialPosition.getInitialPositionInStream()) {
            case LATEST:
                return shards.stream()
                        .filter(s -> s.getSequenceNumberRange().getEndingSequenceNumber() == null)
                        .collect(Collectors.toList());
            case TRIM_HORIZON:
                String minSeqNum = shards.stream()
                        .min(Comparator.comparingLong(s -> Long.parseLong(s.getSequenceNumberRange().getStartingSequenceNumber())))
                        .map(s -> s.getSequenceNumberRange().getStartingSequenceNumber())
                        .orElseThrow(RuntimeException::new);
                return shards.stream()
                        .filter(s -> s.getSequenceNumberRange().getStartingSequenceNumber().equals(minSeqNum))
                        .collect(Collectors.toList());
            case AT_TIMESTAMP:
                return shards.stream()
                        .filter(s -> new Date(Long.parseLong(s.getSequenceNumberRange().getStartingSequenceNumber()))
                                .compareTo(initialPosition.getTimestamp()) <= 0)
                        .filter(s -> s.getSequenceNumberRange().getEndingSequenceNumber() == null ||
                                new Date(Long.parseLong(s.getSequenceNumberRange().getEndingSequenceNumber()))
                                        .compareTo(initialPosition.getTimestamp()) > 0)
                        .collect(Collectors.toList());
        }
        throw new RuntimeException("Unsupported initial position " + initialPosition);
    }
}
