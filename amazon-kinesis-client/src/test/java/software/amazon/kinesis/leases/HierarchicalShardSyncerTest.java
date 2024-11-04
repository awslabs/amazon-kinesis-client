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

//
// TODO: Fix the lack of DynamoDB Loca
//

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
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;
import software.amazon.kinesis.common.HashKeyRangeForLease;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.DeletedStreamListProvider;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.NullMetricsScope;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.leases.HierarchicalShardSyncer.MemoizationContext;
import static software.amazon.kinesis.leases.HierarchicalShardSyncer.determineNewLeasesToCreate;

@RunWith(MockitoJUnitRunner.class)
public class HierarchicalShardSyncerTest {
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_AT_TIMESTAMP =
            InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(1000L));
    private static final int EXPONENT = 128;
    private static final String LEASE_OWNER = "TestOwner";
    private static final MetricsScope SCOPE = new NullMetricsScope();
    private static final boolean MULTISTREAM_MODE_ON = true;
    private static final String STREAM_IDENTIFIER = "123456789012:stream:1";
    private static final HierarchicalShardSyncer.MultiStreamArgs MULTI_STREAM_ARGS =
            new HierarchicalShardSyncer.MultiStreamArgs(
                    MULTISTREAM_MODE_ON, StreamIdentifier.multiStreamInstance(STREAM_IDENTIFIER));

    /**
     * <pre>
     * Shard structure (y-axis is
     * epochs): 0 1 2 3 4   5- shards till
     *          \ / \ / |   |
     *           6   7  4   5- shards from epoch 103 - 205
     *            \ /   |  /\
     *             8    4 9 10 -
     * shards from epoch 206 (open - no ending sequenceNumber)
     * </pre>
     */
    private static final List<Shard> SHARD_GRAPH_A = Collections.unmodifiableList(constructShardListForGraphA());

    /**
     * Shard structure (x-axis is epochs):
     * <pre>
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     * </pre>
     */
    private static final List<Shard> SHARD_GRAPH_B = Collections.unmodifiableList(constructShardListForGraphB());

    /**
     * <pre>
     * Shard structure (y-axis is
     * epochs):     0      1  2  3  - shards till
     *            /   \    |  \ /
     *           4     5   1   6  - shards from epoch 103 - 205
     *          / \   / \  |   |
     *         7   8 9  10 1   6
     * shards from epoch 206 (open - no ending sequenceNumber)
     * </pre>
     */
    private static final List<Shard> SHARD_GRAPH_C = Collections.unmodifiableList(constructShardListForGraphC());

    private final boolean ignoreUnexpectedChildShards = false;

    private HierarchicalShardSyncer hierarchicalShardSyncer;

    /**
     * Old/Obsolete max value of a sequence number (2^128 -1).
     */
    public static final BigInteger MAX_SEQUENCE_NUMBER =
            new BigInteger("2").pow(EXPONENT).subtract(BigInteger.ONE);

    @Mock
    private ShardDetector shardDetector;

    @Mock
    private DynamoDBLeaseRefresher dynamoDBLeaseRefresher;

    @Before
    public void setup() {
        hierarchicalShardSyncer = new HierarchicalShardSyncer();
        when(shardDetector.streamIdentifier()).thenReturn(StreamIdentifier.singleStreamInstance("stream"));
    }

    private void setupMultiStream() {
        hierarchicalShardSyncer = new HierarchicalShardSyncer(true, STREAM_IDENTIFIER);
        when(shardDetector.streamIdentifier()).thenReturn(StreamIdentifier.multiStreamInstance(STREAM_IDENTIFIER));
    }

    /**
     * Test determineNewLeasesToCreate() where there are no shards
     */
    @Test
    public void testDetermineNewLeasesToCreateNoShards() {
        final List<Shard> shards = Collections.emptyList();
        final List<Lease> leases = Collections.emptyList();
        final HierarchicalShardSyncer.LeaseSynchronizer emptyLeaseTableSynchronizer =
                new HierarchicalShardSyncer.EmptyLeaseTableSynchronizer();
        assertTrue(determineNewLeasesToCreate(emptyLeaseTableSynchronizer, shards, leases, INITIAL_POSITION_LATEST)
                .isEmpty());
    }

    /**
     * Test determineNewLeasesToCreate() where there are no shards for MultiStream
     */
    @Test
    public void testDetermineNewLeasesToCreateNoShardsForMultiStream() {
        final List<Shard> shards = Collections.emptyList();
        final List<Lease> leases = Collections.emptyList();
        final HierarchicalShardSyncer.LeaseSynchronizer emptyLeaseTableSynchronizer =
                new HierarchicalShardSyncer.EmptyLeaseTableSynchronizer();

        assertTrue(determineNewLeasesToCreate(
                        emptyLeaseTableSynchronizer,
                        shards,
                        leases,
                        INITIAL_POSITION_LATEST,
                        Collections.emptySet(),
                        MULTI_STREAM_ARGS)
                .isEmpty());
    }

    /**
     * Test determineNewLeasesToCreate() where there are no leases and no resharding operations have been performed
     */
    @Test
    public void testDetermineNewLeasesToCreate0Leases0Reshards() {
        final String shardId0 = "shardId-0";
        final String shardId1 = "shardId-1";
        final SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        final List<Shard> shards = Arrays.asList(
                ShardObjectHelper.newShard(shardId0, null, null, sequenceRange),
                ShardObjectHelper.newShard(shardId1, null, null, sequenceRange));
        final List<Lease> currentLeases = Collections.emptyList();
        final HierarchicalShardSyncer.LeaseSynchronizer emptyLeaseTableSynchronizer =
                new HierarchicalShardSyncer.EmptyLeaseTableSynchronizer();

        final List<Lease> newLeases =
                determineNewLeasesToCreate(emptyLeaseTableSynchronizer, shards, currentLeases, INITIAL_POSITION_LATEST);
        validateLeases(newLeases, shardId0, shardId1);
    }

    /**
     * Test determineNewLeasesToCreate() where there are no leases and no resharding operations have been performed
     */
    @Test
    public void testDetermineNewLeasesToCreate0Leases0ReshardsForMultiStream() {
        final String shardId0 = "shardId-0";
        final String shardId1 = "shardId-1";
        final SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        final List<Shard> shards = Arrays.asList(
                ShardObjectHelper.newShard(shardId0, null, null, sequenceRange),
                ShardObjectHelper.newShard(shardId1, null, null, sequenceRange));
        final List<Lease> currentLeases = Collections.emptyList();
        final HierarchicalShardSyncer.LeaseSynchronizer emptyLeaseTableSynchronizer =
                new HierarchicalShardSyncer.EmptyLeaseTableSynchronizer();

        final List<Lease> newLeases = determineNewLeasesToCreate(
                emptyLeaseTableSynchronizer,
                shards,
                currentLeases,
                INITIAL_POSITION_LATEST,
                new HashSet<>(),
                MULTI_STREAM_ARGS);
        validateLeases(newLeases, toMultiStreamLeases(shardId0, shardId1));
    }

    /**
     * Test determineNewLeasesToCreate() where there is one lease and no resharding operations have been performed, but
     * one of the shards was marked as inconsistent.
     */
    @Test
    public void testDetermineNewLeasesToCreate0Leases0Reshards1Inconsistent() {
        final String shardId0 = "shardId-0";
        final String shardId1 = "shardId-1";
        final String shardId2 = "shardId-2";
        final String shardId3 = "shardId-3";
        final SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        final List<Shard> shardsWithLeases =
                Arrays.asList(ShardObjectHelper.newShard(shardId3, null, null, sequenceRange));
        final List<Shard> shardsWithoutLeases = Arrays.asList(
                ShardObjectHelper.newShard(shardId0, null, null, sequenceRange),
                ShardObjectHelper.newShard(shardId1, null, null, sequenceRange),
                ShardObjectHelper.newShard(shardId2, shardId1, null, sequenceRange));

        final List<Shard> shards = Stream.of(shardsWithLeases, shardsWithoutLeases)
                .flatMap(x -> x.stream())
                .collect(Collectors.toList());
        final List<Lease> currentLeases =
                createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.LATEST, "foo");
        final Set<String> inconsistentShardIds = new HashSet<>(Collections.singletonList(shardId2));

        Map<String, Shard> shardIdToShardMap = HierarchicalShardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                HierarchicalShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        final HierarchicalShardSyncer.LeaseSynchronizer leaseSynchronizer =
                new HierarchicalShardSyncer.NonEmptyLeaseTableSynchronizer(
                        shardDetector, shardIdToShardMap, shardIdToChildShardIdsMap);

        final List<Lease> newLeases = determineNewLeasesToCreate(
                leaseSynchronizer, shards, currentLeases, INITIAL_POSITION_LATEST, inconsistentShardIds);
        validateLeases(newLeases, shardId0, shardId1);
    }

    /**
     * Test determineNewLeasesToCreate() where there are no leases and no resharding operations have been performed, but
     * one of the shards was marked as inconsistent.
     */
    @Test
    public void testDetermineNewLeasesToCreate0Leases0Reshards1InconsistentMultiStream() {
        final String shardId0 = "shardId-0";
        final String shardId1 = "shardId-1";
        final String shardId2 = "shardId-2";
        final String shardId3 = "shardId-3";
        final SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        final List<Shard> shardsWithLeases =
                Arrays.asList(ShardObjectHelper.newShard(shardId3, null, null, sequenceRange));
        final List<Shard> shardsWithoutLeases = Arrays.asList(
                ShardObjectHelper.newShard(shardId0, null, null, sequenceRange),
                ShardObjectHelper.newShard(shardId1, null, null, sequenceRange),
                ShardObjectHelper.newShard(shardId2, shardId1, null, sequenceRange));

        final List<Shard> shards = Stream.of(shardsWithLeases, shardsWithoutLeases)
                .flatMap(x -> x.stream())
                .collect(Collectors.toList());
        final List<Lease> currentLeases = new ArrayList<>(
                createMultiStreamLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.LATEST, "foo"));
        final Set<String> inconsistentShardIds = new HashSet<>(Collections.singletonList(shardId2));

        Map<String, Shard> shardIdToShardMap = HierarchicalShardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                HierarchicalShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
        final HierarchicalShardSyncer.LeaseSynchronizer leaseSynchronizer =
                new HierarchicalShardSyncer.NonEmptyLeaseTableSynchronizer(
                        shardDetector, shardIdToShardMap, shardIdToChildShardIdsMap);

        final List<Lease> newLeases = determineNewLeasesToCreate(
                leaseSynchronizer,
                shards,
                currentLeases,
                INITIAL_POSITION_LATEST,
                inconsistentShardIds,
                MULTI_STREAM_ARGS);
        validateLeases(newLeases, toMultiStreamLeases(shardId0, shardId1));
    }

    private static void validateHashRangeInLease(List<Lease> leases) {
        final Consumer<Lease> leaseValidation = lease -> {
            Validate.notNull(lease.hashKeyRangeForLease());
            Validate.isTrue(lease.hashKeyRangeForLease()
                            .startingHashKey()
                            .compareTo(lease.hashKeyRangeForLease().endingHashKey())
                    < 0);
        };
        leases.forEach(leaseValidation);
    }

    /**
     * Validates that a {@link Lease} exists for each expected lease key.
     */
    private static void validateLeases(final List<Lease> leases, final String... expectedLeaseKeys) {
        validateHashRangeInLease(leases);
        assertEquals(expectedLeaseKeys.length, leases.size());

        final Set<String> leaseKeys = leases.stream().map(Lease::leaseKey).collect(Collectors.toSet());
        assertThat(leaseKeys, Matchers.containsInAnyOrder(expectedLeaseKeys));
    }

    /**
     * Test bootstrapShardLeases() starting at TRIM_HORIZON ("beginning" of stream)
     */
    @Test
    public void testBootstrapShardLeasesAtTrimHorizon() throws Exception {
        testCheckAndCreateLeasesForShardsIfMissing(INITIAL_POSITION_TRIM_HORIZON);
    }

    /**
     * Test bootstrapShardLeases() starting at LATEST (tip of stream)
     */
    @Test
    public void testBootstrapShardLeasesAtLatest() throws Exception {
        testCheckAndCreateLeasesForShardsIfMissing(INITIAL_POSITION_LATEST);
    }

    private void testLeaseCreation(
            final List<Shard> shards, final boolean ignoreUnexpectedChildShards, final String... expectedLeaseKeys)
            throws Exception {
        final ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);

        when(shardDetector.listShardsWithoutConsumingResourceNotFoundException())
                .thenReturn(shards);
        when(dynamoDBLeaseRefresher.listLeases()).thenReturn(Collections.emptyList());
        when(dynamoDBLeaseRefresher.createLeaseIfNotExists(leaseCaptor.capture()))
                .thenReturn(true);

        hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                INITIAL_POSITION_LATEST,
                SCOPE,
                ignoreUnexpectedChildShards,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());

        final List<Lease> requestLeases = leaseCaptor.getAllValues();
        final Set<ExtendedSequenceNumber> extendedSequenceNumbers =
                requestLeases.stream().map(Lease::checkpoint).collect(Collectors.toSet());

        validateLeases(requestLeases, expectedLeaseKeys);
        assertEquals(1, extendedSequenceNumbers.size());

        extendedSequenceNumbers.forEach(seq -> assertEquals(ExtendedSequenceNumber.LATEST, seq));

        verify(shardDetector, never()).listShards();
        verify(shardDetector).listShardsWithoutConsumingResourceNotFoundException();
        verify(dynamoDBLeaseRefresher, times(requestLeases.size())).createLeaseIfNotExists(any(Lease.class));
        verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));
    }

    /**
     * Test checkAndCreateLeaseForNewShards while not providing a pre-fetched list of shards
     */
    @Test
    public void testCheckAndCreateLeasesForShardsIfMissingAtLatest() throws Exception {
        testLeaseCreation(SHARD_GRAPH_A, false, "shardId-4", "shardId-8", "shardId-9", "shardId-10");
    }

    @Test
    public void testCheckAndCreateLeasesForShardsIfMissingAtLatestMultiStream() throws Exception {
        setupMultiStream();
        testLeaseCreation(
                SHARD_GRAPH_A, false, toMultiStreamLeases("shardId-4", "shardId-8", "shardId-9", "shardId-10"));
    }

    /**
     * Converts one-or-more shard ids to their multi-stream equivalent.
     *
     * @param shardIds vararg of shard ids (i.e., {@code shardId-<number>})
     * @return a same-sized array where the Nth element is the multi-stream
     *      equivalent of the Nth {@code shardIds} input
     */
    private static String[] toMultiStreamLeases(final String... shardIds) {
        final String[] multiStreamLeaseKey = new String[shardIds.length];
        for (int i = 0; i < shardIds.length; i++) {
            multiStreamLeaseKey[i] = STREAM_IDENTIFIER + ":" + shardIds[i];
        }
        return multiStreamLeaseKey;
    }

    private void testCheckAndCreateLeasesForShardsWithShardList(final String... expectedLeaseKeys) throws Exception {
        final ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);
        when(dynamoDBLeaseRefresher.createLeaseIfNotExists(leaseCaptor.capture()))
                .thenReturn(true);

        hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                INITIAL_POSITION_LATEST,
                SHARD_GRAPH_A,
                false,
                SCOPE,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());

        final List<Lease> requestLeases = leaseCaptor.getAllValues();
        final Set<ExtendedSequenceNumber> extendedSequenceNumbers =
                requestLeases.stream().map(Lease::checkpoint).collect(Collectors.toSet());

        validateLeases(requestLeases, expectedLeaseKeys);
        assertEquals(1, extendedSequenceNumbers.size());

        extendedSequenceNumbers.forEach(seq -> assertEquals(ExtendedSequenceNumber.LATEST, seq));

        verify(shardDetector, never()).listShards();
        verify(shardDetector, never()).listShardsWithoutConsumingResourceNotFoundException();
        verify(dynamoDBLeaseRefresher, times(requestLeases.size())).createLeaseIfNotExists(any(Lease.class));
        verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));
    }

    /**
     * Test checkAndCreateLeaseForNewShards with a pre-fetched list of shards. In this scenario, shardDetector.listShards()
     * or shardDetector.listShardsWithoutConsumingResourceNotFoundException() should never be called.
     */
    @Test
    public void testCheckAndCreateLeasesForShardsWithShardList() throws Exception {
        testCheckAndCreateLeasesForShardsWithShardList("shardId-4", "shardId-8", "shardId-9", "shardId-10");
    }

    /**
     * Test checkAndCreateLeaseForNewShards with a pre-fetched list of shards. In this scenario, shardDetector.listShards()
     * or shardDetector.listShardsWithoutConsumingResourceNotFoundException() should never be called.
     */
    @Test
    public void testCheckAndCreateLeasesForShardsWithShardListMultiStream() throws Exception {
        setupMultiStream();
        testCheckAndCreateLeasesForShardsWithShardList(
                toMultiStreamLeases("shardId-4", "shardId-8", "shardId-9", "shardId-10"));
    }

    /**
     * Test checkAndCreateLeaseForNewShards with an empty list of shards. In this scenario, shardDetector.listShards()
     * or shardDetector.listShardsWithoutConsumingResourceNotFoundException() should never be called.
     */
    @Test
    public void testCheckAndCreateLeasesForShardsWithEmptyShardList() throws Exception {
        final ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);

        hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                INITIAL_POSITION_LATEST,
                new ArrayList<>(),
                false,
                SCOPE,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());

        final List<Lease> requestLeases = leaseCaptor.getAllValues();
        final Set<ExtendedSequenceNumber> extendedSequenceNumbers =
                requestLeases.stream().map(Lease::checkpoint).collect(Collectors.toSet());
        validateLeases(requestLeases);
        assertEquals(0, extendedSequenceNumbers.size());

        verify(shardDetector, never()).listShards();
        verify(shardDetector, never()).listShardsWithoutConsumingResourceNotFoundException();
        verify(dynamoDBLeaseRefresher, never()).createLeaseIfNotExists(any(Lease.class));
        verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));
    }

    /**
     * <pre>
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Initial position: TRIM_HORIZON
     * Leases to create: (0, 1, 2, 3, 4, 5)
     * </pre>
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTrimHorizonWithEmptyLeaseTable() throws Exception {
        final Set<String> expectedLeaseKeysToCreate = new HashSet<>(
                Arrays.asList("shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5"));
        testCheckAndCreateLeaseForShardsIfMissing(
                SHARD_GRAPH_A, INITIAL_POSITION_TRIM_HORIZON, expectedLeaseKeysToCreate);
    }

    /**
     * <pre>
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Initial position: AT_TIMESTAMP(1000)
     * Leases to create: (8, 4, 9, 10)
     * </pre>
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTimestampWithEmptyLeaseTable1() throws Exception {
        final Set<String> expectedLeaseKeysToCreate =
                new HashSet<>(Arrays.asList("shardId-8", "shardId-4", "shardId-9", "shardId-10"));
        testCheckAndCreateLeaseForShardsIfMissing(
                SHARD_GRAPH_A, INITIAL_POSITION_AT_TIMESTAMP, expectedLeaseKeysToCreate);
    }

    /**
     * <pre>
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Initial position: AT_TIMESTAMP(200)
     * Leases to create: (6, 7, 4, 5)
     * </pre>
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTimestampWithEmptyLeaseTable2() throws Exception {
        final Set<String> expectedLeaseKeysToCreate =
                new HashSet<>(Arrays.asList("shardId-6", "shardId-7", "shardId-4", "shardId-5"));
        final InitialPositionInStreamExtended initialPosition =
                InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(200L));
        testCheckAndCreateLeaseForShardsIfMissing(SHARD_GRAPH_A, initialPosition, expectedLeaseKeysToCreate);
    }

    /**
     * <pre>
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Initial position: LATEST
     * Leases to create: (8, 4, 9, 10)
     * </pre>
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtLatestWithEmptyLeaseTable() throws Exception {
        final Set<String> expectedLeaseKeysToCreate =
                new HashSet<>(Arrays.asList("shardId-8", "shardId-4", "shardId-9", "shardId-10"));
        testCheckAndCreateLeaseForShardsIfMissing(SHARD_GRAPH_A, INITIAL_POSITION_LATEST, expectedLeaseKeysToCreate);
    }

    /**
     * <pre>
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Missing leases: (0, 6, 8)
     * Initial position: TRIM_HORIZON
     * Leases to create: (0)
     * </pre>
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTrimHorizonWithPartialLeaseTable() throws Exception {
        final List<Shard> shards = SHARD_GRAPH_A;
        // Leases for shard-0 and its descendants (shard-6, and shard-8) are missing. Expect lease sync to recover the
        // lease for shard-0 when reading from TRIM_HORIZON.
        final Set<String> missingLeaseKeys = new HashSet<>(Arrays.asList("shardId-0", "shardId-6", "shardId-8"));
        final List<Shard> shardsWithLeases = shards.stream()
                .filter(s -> !missingLeaseKeys.contains(s.shardId()))
                .collect(Collectors.toList());
        final List<Lease> existingLeases =
                createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.TRIM_HORIZON, LEASE_OWNER);

        final Set<String> expectedLeaseKeysToCreate = Collections.singleton("shardId-0");
        testCheckAndCreateLeaseForShardsIfMissing(
                shards, INITIAL_POSITION_TRIM_HORIZON, expectedLeaseKeysToCreate, existingLeases);
    }

    /**
     * <pre>
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Missing leases: (0, 6, 8)
     * Initial position: AT_TIMESTAMP(1000)
     * Leases to create: (0)
     * </pre>
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTimestampWithPartialLeaseTable1() throws Exception {
        final List<Shard> shards = SHARD_GRAPH_A;
        // Leases for shard-0 and its descendants (shard-6, and shard-8) are missing. Expect lease sync to recover the
        // lease for shard-0 when reading from AT_TIMESTAMP.
        final Set<String> missingLeaseKeys = new HashSet<>(Arrays.asList("shardId-0", "shardId-6", "shardId-8"));
        final List<Shard> shardsWithLeases = shards.stream()
                .filter(s -> !missingLeaseKeys.contains(s.shardId()))
                .collect(Collectors.toList());
        final List<Lease> existingLeases =
                createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.AT_TIMESTAMP, LEASE_OWNER);

        final Set<String> expectedLeaseKeysToCreate = Collections.singleton("shardId-0");
        testCheckAndCreateLeaseForShardsIfMissing(
                shards, INITIAL_POSITION_AT_TIMESTAMP, expectedLeaseKeysToCreate, existingLeases);
    }

    /**
     * <pre>
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Missing leases: (0, 6, 8)
     * Initial position: AT_TIMESTAMP(200)
     * Leases to create: (0)
     * </pre>
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTimestampWithPartialLeaseTable2() throws Exception {
        final List<Shard> shards = SHARD_GRAPH_A;
        final InitialPositionInStreamExtended initialPosition =
                InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(200L));
        // Leases for shard-0 and its descendants (shard-6, and shard-8) are missing. Expect lease sync to recover the
        // lease for shard-0 when reading from AT_TIMESTAMP.
        final Set<String> missingLeaseKeys = new HashSet<>(Arrays.asList("shardId-0", "shardId-6", "shardId-8"));
        final List<Shard> shardsWithLeases = shards.stream()
                .filter(s -> !missingLeaseKeys.contains(s.shardId()))
                .collect(Collectors.toList());
        final List<Lease> existingLeases =
                createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.AT_TIMESTAMP, LEASE_OWNER);

        final Set<String> expectedLeaseKeysToCreate = Collections.singleton("shardId-0");
        testCheckAndCreateLeaseForShardsIfMissing(shards, initialPosition, expectedLeaseKeysToCreate, existingLeases);
    }

    /**
     * <pre>
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Missing leases: (0, 6, 8)
     * Initial position: LATEST
     * Leases to create: (0)
     * </pre>
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsAtLatestWithPartialLeaseTable() throws Exception {
        final List<Shard> shards = SHARD_GRAPH_A;
        // Leases for shard-0 and its descendants (shard-6, and shard-8) are missing. Expect lease sync to recover the
        // lease for shard-0 when reading from LATEST.
        final Set<String> missingLeaseKeys = new HashSet<>(Arrays.asList("shardId-0", "shardId-6", "shardId-8"));
        final List<Shard> shardsWithLeases = shards.stream()
                .filter(s -> !missingLeaseKeys.contains(s.shardId()))
                .collect(Collectors.toList());
        final List<Lease> existingLeases =
                createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.LATEST, LEASE_OWNER);

        final Set<String> expectedLeaseKeysToCreate = Collections.singleton("shardId-0");
        testCheckAndCreateLeaseForShardsIfMissing(
                shards, INITIAL_POSITION_LATEST, expectedLeaseKeysToCreate, existingLeases);
    }

    @Test(expected = KinesisClientLibIOException.class)
    public void testCheckAndCreateLeasesForNewShardsWhenParentIsOpen() throws Exception {
        final List<Shard> shards = new ArrayList<>(SHARD_GRAPH_A);
        final SequenceNumberRange range = shards.get(0).sequenceNumberRange().toBuilder()
                .endingSequenceNumber(null)
                .build();
        final Shard shard = shards.get(3).toBuilder().sequenceNumberRange(range).build();
        shards.remove(3);
        shards.add(3, shard);

        when(shardDetector.listShardsWithoutConsumingResourceNotFoundException())
                .thenReturn(shards);

        try {
            hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                    shardDetector,
                    dynamoDBLeaseRefresher,
                    INITIAL_POSITION_TRIM_HORIZON,
                    SCOPE,
                    false,
                    dynamoDBLeaseRefresher.isLeaseTableEmpty());
        } finally {
            verify(shardDetector).listShardsWithoutConsumingResourceNotFoundException();
            verify(dynamoDBLeaseRefresher, never()).listLeases();
        }
    }

    @Test(expected = KinesisClientLibIOException.class)
    public void testCheckAndCreateLeasesForNewShardsWhenParentIsOpenForMultiStream() throws Exception {
        final List<Shard> shards = new ArrayList<>(SHARD_GRAPH_A);
        final SequenceNumberRange range = shards.get(0).sequenceNumberRange().toBuilder()
                .endingSequenceNumber(null)
                .build();
        final Shard shard = shards.get(3).toBuilder().sequenceNumberRange(range).build();
        shards.remove(3);
        shards.add(3, shard);

        when(shardDetector.listShardsWithoutConsumingResourceNotFoundException())
                .thenReturn(shards);
        setupMultiStream();
        try {
            hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                    shardDetector,
                    dynamoDBLeaseRefresher,
                    INITIAL_POSITION_TRIM_HORIZON,
                    SCOPE,
                    false,
                    dynamoDBLeaseRefresher.isLeaseTableEmpty());
        } finally {
            verify(shardDetector).listShardsWithoutConsumingResourceNotFoundException();
            verify(dynamoDBLeaseRefresher, never()).listLeases();
        }
    }

    private void testCheckAndCreateLeasesForNewShardsWhenParentIsOpenAndIgnoringInconsistentChildren(
            final String... expectedLeaseKeys) throws Exception {
        final List<Shard> shards = new ArrayList<>(SHARD_GRAPH_A);
        final Shard shard = shards.get(5);
        assertEquals("shardId-5", shard.shardId());

        shards.remove(5);

        // shardId-5 in graph A has two children (shardId-9 and shardId-10). if shardId-5
        // is not closed, those children should be ignored when syncing shards, no leases
        // should be obtained for them, and we should obtain a lease on the still-open
        // parent.
        shards.add(
                5,
                shard.toBuilder()
                        .sequenceNumberRange(shard.sequenceNumberRange().toBuilder()
                                .endingSequenceNumber(null)
                                .build())
                        .build());

        testLeaseCreation(shards, true, expectedLeaseKeys);
    }

    /**
     * Test checkAndCreateLeasesForNewShards() when a parent is open and children of open parents are being ignored.
     */
    @Test
    public void testCheckAndCreateLeasesForNewShardsWhenParentIsOpenAndIgnoringInconsistentChildren() throws Exception {
        testCheckAndCreateLeasesForNewShardsWhenParentIsOpenAndIgnoringInconsistentChildren(
                "shardId-4", "shardId-5", "shardId-8");
    }

    @Test
    public void testCheckAndCreateLeasesForNewShardsWhenParentIsOpenAndIgnoringInconsistentChildrenMultiStream()
            throws Exception {
        setupMultiStream();
        testCheckAndCreateLeasesForNewShardsWhenParentIsOpenAndIgnoringInconsistentChildren(
                toMultiStreamLeases("shardId-4", "shardId-5", "shardId-8"));
    }

    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTrimHorizonAndClosedShard() throws Exception {
        testCheckAndCreateLeasesForNewShardsAndClosedShard(
                ExtendedSequenceNumber.TRIM_HORIZON, INITIAL_POSITION_TRIM_HORIZON);
    }

    @Test
    public void testCheckAndCreateLeasesForNewShardsAtTimestampAndClosedShard() throws Exception {
        testCheckAndCreateLeasesForNewShardsAndClosedShard(
                ExtendedSequenceNumber.AT_TIMESTAMP, INITIAL_POSITION_AT_TIMESTAMP);
    }

    private void testCheckAndCreateLeasesForNewShardsAndClosedShard(
            final ExtendedSequenceNumber sequenceNumber, final InitialPositionInStreamExtended position)
            throws Exception {
        final String shardIdPrefix = "shardId-%d";
        final List<Shard> shards = SHARD_GRAPH_A;
        final List<Lease> leases = createLeasesFromShards(shards, sequenceNumber, LEASE_OWNER);

        // Marking shardId-0 as ShardEnd.
        leases.stream()
                .filter(lease -> String.format(shardIdPrefix, 0).equals(lease.leaseKey()))
                .findFirst()
                .ifPresent(lease -> lease.checkpoint(ExtendedSequenceNumber.SHARD_END));

        // Marking child of shardId-0 to be processed and not at TRIM_HORIZON.
        leases.stream()
                .filter(lease -> String.format(shardIdPrefix, 6).equals(lease.leaseKey()))
                .findFirst()
                .ifPresent(lease -> lease.checkpoint(new ExtendedSequenceNumber("1")));

        final ArgumentCaptor<Lease> leaseCreateCaptor = ArgumentCaptor.forClass(Lease.class);
        final ArgumentCaptor<Lease> leaseDeleteCaptor = ArgumentCaptor.forClass(Lease.class);

        when(shardDetector.listShardsWithoutConsumingResourceNotFoundException())
                .thenReturn(shards);
        when(dynamoDBLeaseRefresher.listLeases())
                .thenReturn(Collections.emptyList())
                .thenReturn(leases);
        when(dynamoDBLeaseRefresher.createLeaseIfNotExists(leaseCreateCaptor.capture()))
                .thenReturn(true);

        // Initial call: No leases present, create leases.
        hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                position,
                SCOPE,
                ignoreUnexpectedChildShards,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());

        final Set<Lease> createLeases = new HashSet<>(leaseCreateCaptor.getAllValues());

        final Set<Lease> expectedCreateLeases = getExpectedLeasesForGraphA(shards, sequenceNumber, position);

        assertEquals(expectedCreateLeases, createLeases);

        verify(shardDetector, times(1)).listShardsWithoutConsumingResourceNotFoundException();
        verify(dynamoDBLeaseRefresher, times(1)).listLeases();
        verify(dynamoDBLeaseRefresher, times(expectedCreateLeases.size())).createLeaseIfNotExists(any(Lease.class));
        verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));

        // Second call: Leases present, no leases should be deleted.
        hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                position,
                SCOPE,
                ignoreUnexpectedChildShards,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());
        final List<Lease> deleteLeases = leaseDeleteCaptor.getAllValues();

        assertTrue(deleteLeases.isEmpty());

        verify(shardDetector, times(2)).listShardsWithoutConsumingResourceNotFoundException();
        verify(dynamoDBLeaseRefresher, times(expectedCreateLeases.size())).createLeaseIfNotExists(any(Lease.class));
        verify(dynamoDBLeaseRefresher, times(2)).listLeases();
    }

    @Test(expected = DependencyException.class)
    public void testCheckAndCreateLeasesForNewShardsAtTrimHorizonAndClosedShardWithListLeasesExceptions()
            throws Exception {
        testCheckAndCreateLeasesForNewShardsAndClosedShardWithListLeasesExceptions(
                ExtendedSequenceNumber.TRIM_HORIZON, INITIAL_POSITION_TRIM_HORIZON);
    }

    @Test(expected = DependencyException.class)
    public void testCheckAndCreateLeasesForNewShardsAtTimestampAndClosedShardWithListLeasesExceptions()
            throws Exception {
        testCheckAndCreateLeasesForNewShardsAndClosedShardWithListLeasesExceptions(
                ExtendedSequenceNumber.AT_TIMESTAMP, INITIAL_POSITION_AT_TIMESTAMP);
    }

    private void testCheckAndCreateLeasesForNewShardsAndClosedShardWithListLeasesExceptions(
            final ExtendedSequenceNumber sequenceNumber, final InitialPositionInStreamExtended position)
            throws Exception {
        final String shardIdPrefix = "shardId-%d";
        final List<Shard> shards = SHARD_GRAPH_A;
        final List<Lease> leases = createLeasesFromShards(shards, sequenceNumber, LEASE_OWNER);

        // Marking shardId-0 as ShardEnd.
        leases.stream()
                .filter(lease -> String.format(shardIdPrefix, 0).equals(lease.leaseKey()))
                .findFirst()
                .ifPresent(lease -> lease.checkpoint(ExtendedSequenceNumber.SHARD_END));

        // Marking child of shardId-0 to be processed and not at TRIM_HORIZON.
        leases.stream()
                .filter(lease -> String.format(shardIdPrefix, 6).equals(lease.leaseKey()))
                .findFirst()
                .ifPresent(lease -> lease.checkpoint(new ExtendedSequenceNumber("1")));

        final ArgumentCaptor<Lease> leaseCreateCaptor = ArgumentCaptor.forClass(Lease.class);

        when(shardDetector.listShardsWithoutConsumingResourceNotFoundException())
                .thenReturn(shards);
        when(dynamoDBLeaseRefresher.listLeases())
                .thenThrow(new DependencyException(new Throwable("Throw for ListLeases")))
                .thenReturn(Collections.emptyList())
                .thenReturn(leases);
        when(dynamoDBLeaseRefresher.createLeaseIfNotExists(leaseCreateCaptor.capture()))
                .thenReturn(true);

        try {
            // Initial call: Call to create leases. Fails on ListLeases
            hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                    shardDetector,
                    dynamoDBLeaseRefresher,
                    position,
                    SCOPE,
                    ignoreUnexpectedChildShards,
                    dynamoDBLeaseRefresher.isLeaseTableEmpty());
        } finally {
            verify(shardDetector, times(1)).listShardsWithoutConsumingResourceNotFoundException();
            verify(dynamoDBLeaseRefresher, times(1)).listLeases();
            verify(dynamoDBLeaseRefresher, never()).createLeaseIfNotExists(any(Lease.class));
            verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));

            // Second call: Leases not present, leases will be created.
            hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                    shardDetector,
                    dynamoDBLeaseRefresher,
                    position,
                    SCOPE,
                    ignoreUnexpectedChildShards,
                    dynamoDBLeaseRefresher.isLeaseTableEmpty());

            final Set<Lease> createLeases = new HashSet<>(leaseCreateCaptor.getAllValues());
            final Set<Lease> expectedCreateLeases = getExpectedLeasesForGraphA(shards, sequenceNumber, position);

            assertThat(createLeases, equalTo(expectedCreateLeases));

            verify(shardDetector, times(2)).listShardsWithoutConsumingResourceNotFoundException();
            verify(dynamoDBLeaseRefresher, times(2)).listLeases();
            verify(dynamoDBLeaseRefresher, times(expectedCreateLeases.size())).createLeaseIfNotExists(any(Lease.class));
            verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));

            // Final call: Leases present, belongs to TestOwner, shardId-0 is at ShardEnd should be cleaned up.
            hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                    shardDetector,
                    dynamoDBLeaseRefresher,
                    position,
                    SCOPE,
                    ignoreUnexpectedChildShards,
                    dynamoDBLeaseRefresher.isLeaseTableEmpty());

            verify(shardDetector, times(3)).listShardsWithoutConsumingResourceNotFoundException();
            verify(dynamoDBLeaseRefresher, times(expectedCreateLeases.size())).createLeaseIfNotExists(any(Lease.class));
            verify(dynamoDBLeaseRefresher, times(3)).listLeases();
            verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));
        }
    }

    @Test
    public void testDeletedStreamListProviderUpdateOnResourceNotFound()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException, InterruptedException {
        DeletedStreamListProvider dummyDeletedStreamListProvider = new DeletedStreamListProvider();
        hierarchicalShardSyncer =
                new HierarchicalShardSyncer(MULTISTREAM_MODE_ON, STREAM_IDENTIFIER, dummyDeletedStreamListProvider);
        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenReturn(false);
        when(shardDetector.listShardsWithoutConsumingResourceNotFoundException())
                .thenThrow(ResourceNotFoundException.builder().build());
        boolean response = hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                INITIAL_POSITION_TRIM_HORIZON,
                SCOPE,
                ignoreUnexpectedChildShards,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());
        Set<StreamIdentifier> deletedStreamSet = dummyDeletedStreamListProvider.purgeAllDeletedStream();

        assertFalse(response);
        assertThat(deletedStreamSet.size(), equalTo(1));
        assertThat(deletedStreamSet.iterator().next().toString(), equalTo(STREAM_IDENTIFIER));

        verify(shardDetector).listShardsWithoutConsumingResourceNotFoundException();
        verify(shardDetector, never()).listShards();
    }

    @Test(expected = DependencyException.class)
    public void testCheckAndCreateLeasesForNewShardsAtTrimHorizonAndClosedShardWithCreateLeaseExceptions()
            throws Exception {
        testCheckAndCreateLeasesForNewShardsAndClosedShardWithCreateLeaseExceptions(
                ExtendedSequenceNumber.TRIM_HORIZON, INITIAL_POSITION_TRIM_HORIZON);
    }

    @Test(expected = DependencyException.class)
    public void testCheckAndCreateLeasesForNewShardsAtTimestampAndClosedShardWithCreateLeaseExceptions()
            throws Exception {
        testCheckAndCreateLeasesForNewShardsAndClosedShardWithCreateLeaseExceptions(
                ExtendedSequenceNumber.AT_TIMESTAMP, INITIAL_POSITION_AT_TIMESTAMP);
    }

    private void testCheckAndCreateLeasesForNewShardsAndClosedShardWithCreateLeaseExceptions(
            final ExtendedSequenceNumber sequenceNumber, final InitialPositionInStreamExtended position)
            throws Exception {
        final String shardIdPrefix = "shardId-%d";
        final List<Shard> shards = SHARD_GRAPH_A;
        final List<Lease> leases = createLeasesFromShards(shards, sequenceNumber, LEASE_OWNER);

        // Marking shardId-0 as ShardEnd.
        leases.stream()
                .filter(lease -> String.format(shardIdPrefix, 0).equals(lease.leaseKey()))
                .findFirst()
                .ifPresent(lease -> lease.checkpoint(ExtendedSequenceNumber.SHARD_END));

        // Marking child of shardId-0 to be processed and not at TRIM_HORIZON.
        leases.stream()
                .filter(lease -> String.format(shardIdPrefix, 6).equals(lease.leaseKey()))
                .findFirst()
                .ifPresent(lease -> lease.checkpoint(new ExtendedSequenceNumber("1")));

        final ArgumentCaptor<Lease> leaseCreateCaptor = ArgumentCaptor.forClass(Lease.class);

        when(shardDetector.listShardsWithoutConsumingResourceNotFoundException())
                .thenReturn(shards);
        when(dynamoDBLeaseRefresher.listLeases())
                .thenReturn(Collections.emptyList())
                .thenReturn(Collections.emptyList())
                .thenReturn(leases);
        when(dynamoDBLeaseRefresher.createLeaseIfNotExists(leaseCreateCaptor.capture()))
                .thenThrow(new DependencyException(new Throwable("Throw for CreateLease")))
                .thenReturn(true);

        try {
            // Initial call: No leases present, create leases. Create lease Fails
            hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                    shardDetector,
                    dynamoDBLeaseRefresher,
                    position,
                    SCOPE,
                    ignoreUnexpectedChildShards,
                    dynamoDBLeaseRefresher.isLeaseTableEmpty());
        } finally {
            verify(shardDetector, times(1)).listShardsWithoutConsumingResourceNotFoundException();
            verify(dynamoDBLeaseRefresher, times(1)).listLeases();
            verify(dynamoDBLeaseRefresher, times(1)).createLeaseIfNotExists(any(Lease.class));
            verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));

            hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                    shardDetector,
                    dynamoDBLeaseRefresher,
                    position,
                    SCOPE,
                    ignoreUnexpectedChildShards,
                    dynamoDBLeaseRefresher.isLeaseTableEmpty());

            final Set<Lease> createLeases = new HashSet<>(leaseCreateCaptor.getAllValues());

            final Set<Lease> expectedCreateLeases = getExpectedLeasesForGraphA(shards, sequenceNumber, position);

            assertThat(createLeases, equalTo(expectedCreateLeases));
            verify(shardDetector, times(2)).listShardsWithoutConsumingResourceNotFoundException();
            verify(dynamoDBLeaseRefresher, times(2)).listLeases();
            verify(dynamoDBLeaseRefresher, times(1 + expectedCreateLeases.size()))
                    .createLeaseIfNotExists(any(Lease.class));
            verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));

            // Final call: Leases are present, shardId-0 is at ShardEnd needs to be cleaned up.
            hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                    shardDetector,
                    dynamoDBLeaseRefresher,
                    position,
                    SCOPE,
                    ignoreUnexpectedChildShards,
                    dynamoDBLeaseRefresher.isLeaseTableEmpty());

            verify(shardDetector, times(3)).listShardsWithoutConsumingResourceNotFoundException();
            verify(dynamoDBLeaseRefresher, times(1 + expectedCreateLeases.size()))
                    .createLeaseIfNotExists(any(Lease.class));
            verify(dynamoDBLeaseRefresher, times(3)).listLeases();
            verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));
        }
    }

    private List<Lease> createLeasesFromShards(
            final List<Shard> shards, final ExtendedSequenceNumber checkpoint, final String leaseOwner) {
        return shards.stream()
                .map(shard -> {
                    final Set<String> parentShardIds = new HashSet<>();
                    if (StringUtils.isNotEmpty(shard.parentShardId())) {
                        parentShardIds.add(shard.parentShardId());
                    }
                    if (StringUtils.isNotEmpty(shard.adjacentParentShardId())) {
                        parentShardIds.add(shard.adjacentParentShardId());
                    }
                    return new Lease(
                            shard.shardId(),
                            leaseOwner,
                            0L,
                            UUID.randomUUID(),
                            0L,
                            checkpoint,
                            null,
                            0L,
                            parentShardIds,
                            new HashSet<>(),
                            null,
                            HashKeyRangeForLease.fromHashKeyRange(shard.hashKeyRange()));
                })
                .collect(Collectors.toList());
    }

    private List<MultiStreamLease> createMultiStreamLeasesFromShards(
            final List<Shard> shards, final ExtendedSequenceNumber checkpoint, final String leaseOwner) {
        return shards.stream()
                .map(shard -> {
                    final Set<String> parentShardIds = new HashSet<>();
                    if (StringUtils.isNotEmpty(shard.parentShardId())) {
                        parentShardIds.add(shard.parentShardId());
                    }
                    if (StringUtils.isNotEmpty(shard.adjacentParentShardId())) {
                        parentShardIds.add(shard.adjacentParentShardId());
                    }
                    final MultiStreamLease msLease = new MultiStreamLease();
                    msLease.shardId(shard.shardId());
                    msLease.leaseOwner(leaseOwner);
                    msLease.leaseCounter(0L);
                    msLease.concurrencyToken(UUID.randomUUID());
                    msLease.lastCounterIncrementNanos(0L);
                    msLease.checkpoint(checkpoint);
                    msLease.parentShardIds(parentShardIds);
                    msLease.streamIdentifier(STREAM_IDENTIFIER);
                    return msLease;
                })
                .collect(Collectors.toList());
    }

    private void testCheckAndCreateLeasesForShardsIfMissing(InitialPositionInStreamExtended initialPosition)
            throws Exception {
        final String shardId0 = "shardId-0";
        final String shardId1 = "shardId-1";
        final HashKeyRange range1 =
                ShardObjectHelper.newHashKeyRange(ShardObjectHelper.MIN_HASH_KEY, BigInteger.ONE.toString());
        final HashKeyRange range2 =
                ShardObjectHelper.newHashKeyRange(new BigInteger("2").toString(), ShardObjectHelper.MAX_HASH_KEY);
        final SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("11", null);
        final List<Shard> shards = Arrays.asList(
                ShardObjectHelper.newShard(shardId0, null, null, sequenceRange, range1),
                ShardObjectHelper.newShard(shardId1, null, null, sequenceRange, range2));
        final Set<String> expectedLeaseKeys = new HashSet<>(Arrays.asList(shardId0, shardId1));

        testCheckAndCreateLeaseForShardsIfMissing(shards, initialPosition, expectedLeaseKeys);
    }

    private void testCheckAndCreateLeaseForShardsIfMissing(
            final List<Shard> shards,
            final InitialPositionInStreamExtended initialPosition,
            final Set<String> expectedLeaseKeys)
            throws Exception {
        testCheckAndCreateLeaseForShardsIfMissing(shards, initialPosition, expectedLeaseKeys, Collections.emptyList());
    }

    private void testCheckAndCreateLeaseForShardsIfMissing(
            final List<Shard> shards,
            final InitialPositionInStreamExtended initialPosition,
            final Set<String> expectedLeaseKeys,
            final List<Lease> existingLeases)
            throws Exception {
        final ArgumentCaptor<Lease> leaseCaptor = ArgumentCaptor.forClass(Lease.class);

        when(shardDetector.listShardsWithoutConsumingResourceNotFoundException())
                .thenReturn(shards);
        when(shardDetector.listShardsWithFilter(any())).thenReturn(getFilteredShards(shards, initialPosition));
        when(dynamoDBLeaseRefresher.listLeases()).thenReturn(existingLeases);
        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenReturn(existingLeases.isEmpty());
        when(dynamoDBLeaseRefresher.createLeaseIfNotExists(leaseCaptor.capture()))
                .thenReturn(true);

        hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                initialPosition,
                SCOPE,
                false,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());

        final List<Lease> leases = leaseCaptor.getAllValues();
        final Set<String> leaseKeys = leases.stream().map(Lease::leaseKey).collect(Collectors.toSet());
        final Set<ExtendedSequenceNumber> leaseSequenceNumbers =
                leases.stream().map(Lease::checkpoint).collect(Collectors.toSet());

        final Set<ExtendedSequenceNumber> expectedSequenceNumbers =
                new HashSet<>(Collections.singletonList(new ExtendedSequenceNumber(
                        initialPosition.getInitialPositionInStream().name())));

        assertEquals(expectedLeaseKeys.size(), leases.size());
        assertEquals(expectedLeaseKeys, leaseKeys);
        assertEquals(expectedSequenceNumbers, leaseSequenceNumbers);

        verify(dynamoDBLeaseRefresher, times(expectedLeaseKeys.size())).createLeaseIfNotExists(any(Lease.class));
        verify(dynamoDBLeaseRefresher, never()).deleteLease(any(Lease.class));
    }

    @Test
    public void testDetermineNewLeasesToCreateStartingPosition() {
        final String shardId0 = "shardId-0";
        final String shardId1 = "shardId-1";
        final List<Lease> currentLeases = new ArrayList<>();
        final HierarchicalShardSyncer.LeaseSynchronizer emptyLeaseTableSynchronizer =
                new HierarchicalShardSyncer.EmptyLeaseTableSynchronizer();
        final SequenceNumberRange sequenceRange = ShardObjectHelper.newSequenceNumberRange("342980", null);

        final List<Shard> shards = Arrays.asList(
                ShardObjectHelper.newShard(shardId0, null, null, sequenceRange),
                ShardObjectHelper.newShard(shardId1, null, null, sequenceRange));

        final Set<InitialPositionInStreamExtended> initialPositions =
                new HashSet<>(Arrays.asList(INITIAL_POSITION_LATEST, INITIAL_POSITION_TRIM_HORIZON));

        final Set<String> expectedLeaseShardIds = new HashSet<>(Arrays.asList(shardId0, shardId1));

        for (InitialPositionInStreamExtended initialPosition : initialPositions) {
            final List<Lease> newLeases =
                    determineNewLeasesToCreate(emptyLeaseTableSynchronizer, shards, currentLeases, initialPosition);
            assertEquals(2, newLeases.size());

            for (Lease lease : newLeases) {
                assertTrue(expectedLeaseShardIds.contains(lease.leaseKey()));
                assertThat(
                        lease.checkpoint(),
                        equalTo(new ExtendedSequenceNumber(
                                initialPosition.getInitialPositionInStream().toString())));
            }
        }
    }

    /**
     * Tests that leases are not created for closed shards.
     */
    @Test
    public void testDetermineNewLeasesToCreateIgnoreClosedShard() {
        final String lastShardId = "shardId-1";

        final List<Shard> shardsWithoutLeases = Arrays.asList(
                ShardObjectHelper.newShard(
                        "shardId-0", null, null, ShardObjectHelper.newSequenceNumberRange("303", "404")),
                ShardObjectHelper.newShard(
                        lastShardId, null, null, ShardObjectHelper.newSequenceNumberRange("405", null)));

        final List<Shard> shardsWithLeases = Arrays.asList(ShardObjectHelper.newShard(
                "shardId-2", null, null, ShardObjectHelper.newSequenceNumberRange("202", "302")));

        final List<Shard> shards = Stream.of(shardsWithLeases, shardsWithoutLeases)
                .flatMap(x -> x.stream())
                .collect(Collectors.toList());
        final List<Lease> currentLeases =
                createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.LATEST, "foo");

        Map<String, Shard> shardIdToShardMap = HierarchicalShardSyncer.constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap =
                HierarchicalShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);

        final HierarchicalShardSyncer.LeaseSynchronizer leaseSynchronizer =
                new HierarchicalShardSyncer.NonEmptyLeaseTableSynchronizer(
                        shardDetector, shardIdToShardMap, shardIdToChildShardIdsMap);

        final List<Lease> newLeases =
                determineNewLeasesToCreate(leaseSynchronizer, shards, currentLeases, INITIAL_POSITION_LATEST);

        assertThat(newLeases.size(), equalTo(1));
        assertThat(newLeases.get(0).leaseKey(), equalTo(lastShardId));
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     * Initial position: LATEST
     * Expected leases: (2, 6)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestA_PartialHashRange1() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-3", "shardId-4", "shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.LATEST);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 7)
     * Initial position: LATEST
     * Expected leases: (6)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestA_PartialHashRange2() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.LATEST);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (2, 6)
     * Initial position: LATEST
     * Expected leases: (3, 4, 9, 10)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestA_PartialHashRange3() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-2", "shardId-6");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.LATEST);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 9, 10)
     * Initial position: LATEST
     * Expected leases: (8)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestA_PartialHashRange4() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-9", "shardId-10");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.LATEST);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * <pre>
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
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestC_PartialHashRange5() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-9", "shardId-10");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-6", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.LATEST);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_C, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 6, 7)
     * Initial position: LATEST
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestA_CompleteHashRange() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-6", "shardId-7");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 2, 3, 4, 5, 6, 7)
     * Initial position: LATEST
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestA_CompleteHashRangeWithoutGC() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList(
                "shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5", "shardId-6", "shardId-7");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: empty set
     * Initial position: LATEST
     * Expected leases: (4, 8, 9, 10)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestA_EmptyLeaseTable() {
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-8", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.LATEST);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, Collections.emptyList(), INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 4, 7, 9, 10)
     * Initial position: LATEST
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestA_CompleteHashRangeAcrossDifferentEpochs() {
        final List<String> shardIdsOfCurrentLeases =
                Arrays.asList("shardId-0", "shardId-1", "shardId-4", "shardId-7", "shardId-9", "shardId-10");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST);
    }

    /**
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: (6)
     * Initial position: LATEST
     * Expected leases: (7)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestB_PartialHashRange() {
        final List<String> shardIdsOfCurrentLeases = Collections.singletonList("shardId-6");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.LATEST);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_B, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: (5)
     * Initial position: LATEST
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestB_CompleteHashRange() {
        final List<String> shardIdsOfCurrentLeases = Collections.singletonList("shardId-5");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_B, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST);
    }

    /**
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: (0, 1, 2, 3, 4, 5)
     * Initial position: LATEST
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestB_CompleteHashRangeWithoutGC() {
        final List<String> shardIdsOfCurrentLeases =
                Arrays.asList("shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_B, shardIdsOfCurrentLeases, INITIAL_POSITION_LATEST);
    }

    /**
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: empty set
     * Initial position: LATEST
     * Expected leases: (9, 10)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeLatestB_EmptyLeaseTable() {
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-9", ExtendedSequenceNumber.LATEST);
        expectedShardIdCheckpointMap.put("shardId-10", ExtendedSequenceNumber.LATEST);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_B, Collections.emptyList(), INITIAL_POSITION_LATEST, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     * Initial position: TRIM_HORIZON
     * Expected leases: (0, 1, 2)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_PartialHashRange1() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-3", "shardId-4", "shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.TRIM_HORIZON);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 7)
     * Initial position: TRIM_HORIZON
     * Expected leases: (0, 1)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_PartialHashRange2() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (2, 6)
     * Initial position: TRIM_HORIZON
     * Expected leases: (3, 4, 5)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_PartialHashRange3() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-2", "shardId-6");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-5", ExtendedSequenceNumber.TRIM_HORIZON);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 9, 10)
     * Initial position: TRIM_HORIZON
     * Expected leases: (0, 1, 2, 3)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_PartialHashRange4() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-9", "shardId-10");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.TRIM_HORIZON);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 6, 7)
     * Initial position: TRIM_HORIZON
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_CompleteHashRange() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-6", "shardId-7");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 2, 3, 4, 5, 6, 7)
     * Initial position: TRIM_HORIZON
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_CompleteHashRangeWithoutGC() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList(
                "shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5", "shardId-6", "shardId-7");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: empty set
     * Initial position: TRIM_HORIZON
     * Expected leases: (0, 1, 2, 3, 4, 5)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_EmptyLeaseTable() {
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-5", ExtendedSequenceNumber.TRIM_HORIZON);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, Collections.emptyList(), INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 4, 7, 9, 10)
     * Initial position: TRIM_HORIZON
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonA_CompleteHashRangeAcrossDifferentEpochs() {
        final List<String> shardIdsOfCurrentLeases =
                Arrays.asList("shardId-0", "shardId-1", "shardId-4", "shardId-7", "shardId-9", "shardId-10");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON);
    }

    /*
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: (6)
     * Initial position: TRIM_HORIZON
     * Expected leases: (7)
     * </pre>
     */
    //    TODO: Account for out-of-order lease creation in TRIM_HORIZON and AT_TIMESTAMP cases
    //    @Test
    //    public void testDetermineNewLeasesToCreateSplitMergeHorizonB_PartialHashRange() {
    //        final List<Shard> shards = constructShardListForGraphB();
    //        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-6");
    //        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
    //        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.TRIM_HORIZON);
    //        assertExpectedLeasesAreCreated(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON,
    // expectedShardIdCheckpointMap);
    //    }

    /**
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: (5)
     * Initial position: TRIM_HORIZON
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonB_CompleteHashRange() {
        final List<String> shardIdsOfCurrentLeases = Collections.singletonList("shardId-5");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_B, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON);
    }

    /**
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: (0, 1, 2, 3, 4, 5)
     * Initial position: TRIM_HORIZON
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonB_CompleteHashRangeWithoutGC() {
        final List<String> shardIdsOfCurrentLeases =
                Arrays.asList("shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_B, shardIdsOfCurrentLeases, INITIAL_POSITION_TRIM_HORIZON);
    }

    /**
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: empty set
     * Initial position: TRIM_HORIZON
     * Expected leases: (0, 1)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeHorizonB_EmptyLeaseTable() {
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.TRIM_HORIZON);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.TRIM_HORIZON);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_B, Collections.emptyList(), INITIAL_POSITION_TRIM_HORIZON, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (0, 1, 2)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_PartialHashRange1() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-3", "shardId-4", "shardId-5");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.AT_TIMESTAMP);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 7)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (0, 1)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_PartialHashRange2() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-7");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (2, 6)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (3, 4, 5)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_PartialHashRange3() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-2", "shardId-6");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-5", ExtendedSequenceNumber.AT_TIMESTAMP);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 9, 10)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (0, 1, 2, 3)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_PartialHashRange4() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-9", "shardId-10");
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.AT_TIMESTAMP);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (4, 5, 6, 7)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_CompleteHashRange() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-4", "shardId-5", "shardId-6", "shardId-7");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 2, 3, 4, 5, 6, 7)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_CompleteHashRangeWithoutGC() {
        final List<String> shardIdsOfCurrentLeases = Arrays.asList(
                "shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5", "shardId-6", "shardId-7");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: empty set
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (0, 1, 2, 3, 4, 5)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_EmptyLeaseTable() {
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-2", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-3", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-4", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-5", ExtendedSequenceNumber.AT_TIMESTAMP);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_A, Collections.emptyList(), INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors
     * Shard structure (each level depicts a stream segment):
     * <pre>
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (0, 1, 4, 7, 9, 10)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampA_CompleteHashRangeAcrossDifferentEpochs() {
        final List<String> shardIdsOfCurrentLeases =
                Arrays.asList("shardId-0", "shardId-1", "shardId-4", "shardId-7", "shardId-9", "shardId-10");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_A, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP);
    }

    /*
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: (6)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (7)
     * </pre>
     */
    //    TODO: Account for out-of-order lease creation in TRIM_HORIZON and AT_TIMESTAMP cases
    //    @Test
    //    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampB_PartialHashRange() {
    //        final List<Shard> shards = constructShardListForGraphB();
    //        final List<String> shardIdsOfCurrentLeases = Arrays.asList("shardId-6");
    //        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
    //        expectedShardIdCheckpointMap.put("shardId-7", ExtendedSequenceNumber.AT_TIMESTAMP);
    //        assertExpectedLeasesAreCreated(shards, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP,
    // expectedShardIdCheckpointMap);
    //    }

    /**
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: (5)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampB_CompleteHashRange() {
        final List<String> shardIdsOfCurrentLeases = Collections.singletonList("shardId-5");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_B, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP);
    }

    /**
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: (0, 1, 2, 3, 4, 5)
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: empty set
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampB_CompleteHashRangeWithoutGC() {
        final List<String> shardIdsOfCurrentLeases =
                Arrays.asList("shardId-0", "shardId-1", "shardId-2", "shardId-3", "shardId-4", "shardId-5");
        assertExpectedLeasesAreCreated(SHARD_GRAPH_B, shardIdsOfCurrentLeases, INITIAL_POSITION_AT_TIMESTAMP);
    }

    /**
     * <pre>
     * Shard structure (x-axis is epochs):
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     *
     * Current leases: empty set
     * Initial position: AT_TIMESTAMP(1000)
     * Expected leases: (0, 1)
     * </pre>
     */
    @Test
    public void testDetermineNewLeasesToCreateSplitMergeAtTimestampB_EmptyLeaseTable() {
        final Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap = new HashMap<>();
        expectedShardIdCheckpointMap.put("shardId-0", ExtendedSequenceNumber.AT_TIMESTAMP);
        expectedShardIdCheckpointMap.put("shardId-1", ExtendedSequenceNumber.AT_TIMESTAMP);
        assertExpectedLeasesAreCreated(
                SHARD_GRAPH_B, Collections.emptyList(), INITIAL_POSITION_AT_TIMESTAMP, expectedShardIdCheckpointMap);
    }

    private void assertExpectedLeasesAreCreated(
            final List<Shard> shards,
            final List<String> shardIdsOfCurrentLeases,
            final InitialPositionInStreamExtended initialPosition) {
        assertExpectedLeasesAreCreated(shards, shardIdsOfCurrentLeases, initialPosition, Collections.emptyMap());
    }

    private void assertExpectedLeasesAreCreated(
            List<Shard> shards,
            List<String> shardIdsOfCurrentLeases,
            InitialPositionInStreamExtended initialPosition,
            Map<String, ExtendedSequenceNumber> expectedShardIdCheckpointMap) {

        final List<Lease> currentLeases = shardIdsOfCurrentLeases.stream()
                .map(shardId -> newLease(shardId))
                .collect(Collectors.toList());

        final Map<String, Shard> shardIdToShardMap = HierarchicalShardSyncer.constructShardIdToShardMap(shards);
        final Map<String, Set<String>> shardIdToChildShardIdsMap =
                HierarchicalShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);

        final HierarchicalShardSyncer.LeaseSynchronizer nonEmptyLeaseTableSynchronizer =
                new HierarchicalShardSyncer.NonEmptyLeaseTableSynchronizer(
                        shardDetector, shardIdToShardMap, shardIdToChildShardIdsMap);

        final List<Lease> newLeases =
                determineNewLeasesToCreate(nonEmptyLeaseTableSynchronizer, shards, currentLeases, initialPosition);

        assertThat(newLeases.size(), equalTo(expectedShardIdCheckpointMap.size()));
        for (Lease lease : newLeases) {
            assertThat(
                    "Unexpected lease: " + lease,
                    expectedShardIdCheckpointMap.containsKey(lease.leaseKey()),
                    equalTo(true));
            assertThat(lease.checkpoint(), equalTo(expectedShardIdCheckpointMap.get(lease.leaseKey())));
        }
    }

    /**
     * <pre>
     * Helper method to construct a shard list for graph A. Graph A is defined below. Shard structure (y-axis is
     * epochs): 0 1 2 3 4   5- shards till
     *          \ / \ / |   |
     *           6   7  4   5- shards from epoch 103 - 205
     *            \ /   |  /\
     *             8    4 9 10 -
     * shards from epoch 206 (open - no ending sequenceNumber)
     * </pre>
     */
    private static List<Shard> constructShardListForGraphA() {
        final SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("11", "102");
        final SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("11", null);
        final SequenceNumberRange range2 = ShardObjectHelper.newSequenceNumberRange("11", "205");
        final SequenceNumberRange range3 = ShardObjectHelper.newSequenceNumberRange("103", "205");
        final SequenceNumberRange range4 = ShardObjectHelper.newSequenceNumberRange("206", null);

        return Arrays.asList(
                ShardObjectHelper.newShard(
                        "shardId-0", null, null, range0, ShardObjectHelper.newHashKeyRange("0", "99")),
                ShardObjectHelper.newShard(
                        "shardId-1", null, null, range0, ShardObjectHelper.newHashKeyRange("100", "199")),
                ShardObjectHelper.newShard(
                        "shardId-2", null, null, range0, ShardObjectHelper.newHashKeyRange("200", "299")),
                ShardObjectHelper.newShard(
                        "shardId-3", null, null, range0, ShardObjectHelper.newHashKeyRange("300", "399")),
                ShardObjectHelper.newShard(
                        "shardId-4", null, null, range1, ShardObjectHelper.newHashKeyRange("400", "499")),
                ShardObjectHelper.newShard(
                        "shardId-5",
                        null,
                        null,
                        range2,
                        ShardObjectHelper.newHashKeyRange("500", ShardObjectHelper.MAX_HASH_KEY)),
                ShardObjectHelper.newShard(
                        "shardId-6", "shardId-0", "shardId-1", range3, ShardObjectHelper.newHashKeyRange("0", "199")),
                ShardObjectHelper.newShard(
                        "shardId-7", "shardId-2", "shardId-3", range3, ShardObjectHelper.newHashKeyRange("200", "399")),
                ShardObjectHelper.newShard(
                        "shardId-8", "shardId-6", "shardId-7", range4, ShardObjectHelper.newHashKeyRange("0", "399")),
                ShardObjectHelper.newShard(
                        "shardId-9", "shardId-5", null, range4, ShardObjectHelper.newHashKeyRange("500", "799")),
                ShardObjectHelper.newShard(
                        "shardId-10",
                        null,
                        "shardId-5",
                        range4,
                        ShardObjectHelper.newHashKeyRange("800", ShardObjectHelper.MAX_HASH_KEY)));
    }

    /**
     * Helper method to mimic behavior of Kinesis ListShardsWithFilter calls.
     */
    private static List<Shard> getFilteredShards(List<Shard> shards, InitialPositionInStreamExtended initialPosition) {
        switch (initialPosition.getInitialPositionInStream()) {
            case LATEST:
                return shards.stream()
                        .filter(s -> s.sequenceNumberRange().endingSequenceNumber() == null)
                        .collect(Collectors.toList());
            case TRIM_HORIZON:
                String minSeqNum = shards.stream()
                        .min(Comparator.comparingLong(
                                s -> Long.parseLong(s.sequenceNumberRange().startingSequenceNumber())))
                        .map(s -> s.sequenceNumberRange().startingSequenceNumber())
                        .orElseThrow(RuntimeException::new);
                return shards.stream()
                        .filter(s ->
                                s.sequenceNumberRange().startingSequenceNumber().equals(minSeqNum))
                        .collect(Collectors.toList());
            case AT_TIMESTAMP:
                return shards.stream()
                        .filter(s ->
                                new Date(Long.parseLong(s.sequenceNumberRange().startingSequenceNumber()))
                                                .compareTo(initialPosition.getTimestamp())
                                        <= 0)
                        .filter(s -> s.sequenceNumberRange().endingSequenceNumber() == null
                                || new Date(Long.parseLong(
                                                        s.sequenceNumberRange().endingSequenceNumber()))
                                                .compareTo(initialPosition.getTimestamp())
                                        > 0)
                        .collect(Collectors.toList());
        }
        throw new RuntimeException("Unsupported initial position " + initialPosition);
    }

    /**
     * <pre>
     * Helper method to get expected shards for Graph A based on initial position in stream. Shard structure (y-axis is
     * epochs): 0 1 2 3 4   5- shards till
     *          \ / \ / |   |
     *           6   7  4   5- shards from epoch 103 - 205
     *            \ /   |  /\
     *             8    4 9 10 -
     * shards from epoch 206 (open - no ending sequenceNumber)
     * </pre>
     */
    private Set<Lease> getExpectedLeasesForGraphA(
            List<Shard> shards,
            ExtendedSequenceNumber sequenceNumber,
            InitialPositionInStreamExtended initialPosition) {
        final List<Shard> filteredShards;
        if (initialPosition.getInitialPositionInStream().equals(InitialPositionInStream.AT_TIMESTAMP)) {
            // Lease creation for AT_TIMESTAMP should work the same as for TRIM_HORIZON - ignore shard filters
            filteredShards = getFilteredShards(shards, INITIAL_POSITION_TRIM_HORIZON);
        } else {
            filteredShards = getFilteredShards(shards, initialPosition);
        }
        return new HashSet<>(createLeasesFromShards(filteredShards, sequenceNumber, null));
    }

    /**
     * Helper method to construct a shard list for graph B. Graph B is defined below.
     * Shard structure (x-axis is epochs):
     * <pre>
     * 0  3   6   9
     * \ / \ / \ /
     *  2   5   8
     * / \ / \ / \
     * 1  4   7  10
     * </pre>
     */
    private static List<Shard> constructShardListForGraphB() {
        final SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("1000", "1049");
        final SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("1050", "1099");
        final SequenceNumberRange range2 = ShardObjectHelper.newSequenceNumberRange("1100", "1149");
        final SequenceNumberRange range3 = ShardObjectHelper.newSequenceNumberRange("1150", "1199");
        final SequenceNumberRange range4 = ShardObjectHelper.newSequenceNumberRange("1200", "1249");
        final SequenceNumberRange range5 = ShardObjectHelper.newSequenceNumberRange("1250", "1299");
        final SequenceNumberRange range6 = ShardObjectHelper.newSequenceNumberRange("1300", null);

        final HashKeyRange hashRange0 = ShardObjectHelper.newHashKeyRange("0", "499");
        final HashKeyRange hashRange1 = ShardObjectHelper.newHashKeyRange("500", ShardObjectHelper.MAX_HASH_KEY);
        final HashKeyRange hashRange2 = ShardObjectHelper.newHashKeyRange("0", ShardObjectHelper.MAX_HASH_KEY);

        return Arrays.asList(
                ShardObjectHelper.newShard("shardId-0", null, null, range0, hashRange0),
                ShardObjectHelper.newShard("shardId-1", null, null, range0, hashRange1),
                ShardObjectHelper.newShard("shardId-2", "shardId-0", "shardId-1", range1, hashRange2),
                ShardObjectHelper.newShard("shardId-3", "shardId-2", null, range2, hashRange0),
                ShardObjectHelper.newShard("shardId-4", "shardId-2", null, range2, hashRange1),
                ShardObjectHelper.newShard("shardId-5", "shardId-3", "shardId-4", range3, hashRange2),
                ShardObjectHelper.newShard("shardId-6", "shardId-5", null, range4, hashRange0),
                ShardObjectHelper.newShard("shardId-7", "shardId-5", null, range4, hashRange1),
                ShardObjectHelper.newShard("shardId-8", "shardId-6", "shardId-7", range5, hashRange2),
                ShardObjectHelper.newShard("shardId-9", "shardId-8", null, range6, hashRange0),
                ShardObjectHelper.newShard("shardId-10", null, "shardId-8", range6, hashRange1));
    }

    /**
     * <pre>
     * Helper method to construct a shard list for graph C. Graph C is defined below. Shard structure (y-axis is
     * epochs):     0      1  2  3  - shards till
     *            /   \    |  \ /
     *           4     5   1   6  - shards from epoch 103 - 205
     *          / \   / \  |   |
     *         7   8 9  10 1   6
     * shards from epoch 206 (open - no ending sequenceNumber)
     * </pre>
     */
    private static List<Shard> constructShardListForGraphC() {
        final SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("11", "102");
        final SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("11", null);
        final SequenceNumberRange range2 = ShardObjectHelper.newSequenceNumberRange("103", null);
        final SequenceNumberRange range3 = ShardObjectHelper.newSequenceNumberRange("103", "205");
        final SequenceNumberRange range4 = ShardObjectHelper.newSequenceNumberRange("206", null);

        return Arrays.asList(
                ShardObjectHelper.newShard(
                        "shardId-0", null, null, range0, ShardObjectHelper.newHashKeyRange("0", "399")),
                ShardObjectHelper.newShard(
                        "shardId-1", null, null, range1, ShardObjectHelper.newHashKeyRange("400", "499")),
                ShardObjectHelper.newShard(
                        "shardId-2", null, null, range0, ShardObjectHelper.newHashKeyRange("500", "599")),
                ShardObjectHelper.newShard(
                        "shardId-3",
                        null,
                        null,
                        range0,
                        ShardObjectHelper.newHashKeyRange("600", ShardObjectHelper.MAX_HASH_KEY)),
                ShardObjectHelper.newShard(
                        "shardId-4", "shardId-0", null, range3, ShardObjectHelper.newHashKeyRange("0", "199")),
                ShardObjectHelper.newShard(
                        "shardId-5", "shardId-0", null, range3, ShardObjectHelper.newHashKeyRange("200", "399")),
                ShardObjectHelper.newShard(
                        "shardId-6",
                        "shardId-2",
                        "shardId-3",
                        range2,
                        ShardObjectHelper.newHashKeyRange("500", ShardObjectHelper.MAX_HASH_KEY)),
                ShardObjectHelper.newShard(
                        "shardId-7", "shardId-4", null, range4, ShardObjectHelper.newHashKeyRange("0", "99")),
                ShardObjectHelper.newShard(
                        "shardId-8", "shardId-4", null, range4, ShardObjectHelper.newHashKeyRange("100", "199")),
                ShardObjectHelper.newShard(
                        "shardId-9", "shardId-5", null, range4, ShardObjectHelper.newHashKeyRange("200", "299")),
                ShardObjectHelper.newShard(
                        "shardId-10", "shardId-5", null, range4, ShardObjectHelper.newHashKeyRange("300", "399")));
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors when shardId is null
     */
    @Test
    public void testCheckIfDescendantAndAddNewLeasesForAncestorsNullShardId() {
        final MemoizationContext memoizationContext = new MemoizationContext();

        assertFalse(HierarchicalShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(
                null, INITIAL_POSITION_LATEST, null, null, null, memoizationContext));
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors when shard has been trimmed
     */
    @Test
    public void testCheckIfDescendantAndAddNewLeasesForAncestorsTrimmedShard() {
        final String shardId = "shardId-trimmed";
        final MemoizationContext memoizationContext = new MemoizationContext();

        assertFalse(HierarchicalShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(
                shardId, INITIAL_POSITION_LATEST, null, new HashMap<>(), null, memoizationContext));
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors when there is a current lease for the shard
     */
    @Test
    public void testCheckIfDescendantAndAddNewLeasesForAncestorsForShardWithCurrentLease() {
        final String shardId = "shardId-current";
        final Set<String> shardIdsOfCurrentLeases = new HashSet<>(Collections.singletonList(shardId));
        final Map<String, Lease> newLeaseMap = Collections.emptyMap();
        final MemoizationContext memoizationContext = new MemoizationContext();
        final Map<String, Shard> kinesisShards = new HashMap<>();
        kinesisShards.put(shardId, ShardObjectHelper.newShard(shardId, null, null, null));

        assertTrue(HierarchicalShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(
                shardId,
                INITIAL_POSITION_LATEST,
                shardIdsOfCurrentLeases,
                kinesisShards,
                newLeaseMap,
                memoizationContext));
    }

    /**
     * Test CheckIfDescendantAndAddNewLeasesForAncestors - two parents, two ancestors, not descendant
     */
    @Test
    public void testCheckIfDescendantAndAddNewLeasesForAncestors2P2ANotDescendant() {
        final String parentShardId = "shardId-parent";
        final String adjacentParentShardId = "shardId-adjacentParent";
        final String shardId = "shardId-9-1";
        final Set<String> shardIdsOfCurrentLeases = Collections.emptySet();
        final Map<String, Lease> newLeaseMap = Collections.emptyMap();
        final MemoizationContext memoizationContext = new MemoizationContext();
        final Map<String, Shard> kinesisShards = new HashMap<>();

        kinesisShards.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null));
        kinesisShards.put(adjacentParentShardId, ShardObjectHelper.newShard(adjacentParentShardId, null, null, null));
        kinesisShards.put(shardId, ShardObjectHelper.newShard(shardId, parentShardId, adjacentParentShardId, null));

        assertFalse(HierarchicalShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(
                shardId,
                INITIAL_POSITION_LATEST,
                shardIdsOfCurrentLeases,
                kinesisShards,
                newLeaseMap,
                memoizationContext));
    }

    /**
     * Tests that when reading from TIP, we use the AT_LATEST shard filter.
     */
    @Test
    public void testEmptyLeaseTableBootstrapUsesShardFilterWithAtLatest() throws Exception {
        ShardFilter shardFilter =
                ShardFilter.builder().type(ShardFilterType.AT_LATEST).build();
        testEmptyLeaseTableBootstrapUsesListShardsWithFilter(INITIAL_POSITION_LATEST, shardFilter);
    }

    /**
     * Tests that when reading from TRIM, we use the TRIM_HORIZON shard filter.
     */
    @Test
    public void testEmptyLeaseTableBootstrapUsesShardFilterWithAtTrimHorizon() throws Exception {
        ShardFilter shardFilter =
                ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();
        testEmptyLeaseTableBootstrapUsesListShardsWithFilter(INITIAL_POSITION_TRIM_HORIZON, shardFilter);
    }

    /**
     * Tests that when reading from AT_TIMESTAMP, we use the AT_TIMESTAMP shard filter.
     */
    @Test
    public void testEmptyLeaseTableBootstrapUsesShardFilterWithAtTimestamp() throws Exception {
        ShardFilter shardFilter = ShardFilter.builder()
                .type(ShardFilterType.AT_TIMESTAMP)
                .timestamp(new Date(1000L).toInstant())
                .build();
        testEmptyLeaseTableBootstrapUsesListShardsWithFilter(INITIAL_POSITION_AT_TIMESTAMP, shardFilter);
    }

    public void testEmptyLeaseTableBootstrapUsesListShardsWithFilter(
            InitialPositionInStreamExtended initialPosition, ShardFilter shardFilter) throws Exception {
        final String shardId0 = "shardId-0";
        final List<Shard> shards = Arrays.asList(ShardObjectHelper.newShard(
                shardId0,
                null,
                null,
                ShardObjectHelper.newSequenceNumberRange("1", null),
                ShardObjectHelper.newHashKeyRange(ShardObjectHelper.MIN_HASH_KEY, ShardObjectHelper.MAX_HASH_KEY)));

        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenReturn(true);
        when(shardDetector.listShardsWithFilter(shardFilter)).thenReturn(shards);

        hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                initialPosition,
                SCOPE,
                ignoreUnexpectedChildShards,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());

        verify(shardDetector, atLeast(1)).listShardsWithFilter(shardFilter);
        verify(shardDetector, never()).listShards();
    }

    @Test
    public void testNonEmptyLeaseTableUsesListShards() throws Exception {
        final String shardId0 = "shardId-0";
        final String shardId1 = "shardId-1";

        final List<Shard> shardsWithLeases = Arrays.asList(
                ShardObjectHelper.newShard(shardId0, null, null, ShardObjectHelper.newSequenceNumberRange("1", "2")));
        final List<Shard> shardsWithoutLeases = Arrays.asList(
                ShardObjectHelper.newShard(shardId1, null, null, ShardObjectHelper.newSequenceNumberRange("3", "4")));

        final List<Lease> currentLeases =
                createLeasesFromShards(shardsWithLeases, ExtendedSequenceNumber.LATEST, "foo");

        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenReturn(false);
        when(shardDetector.listShardsWithoutConsumingResourceNotFoundException())
                .thenReturn(shardsWithoutLeases);
        when(dynamoDBLeaseRefresher.listLeases()).thenReturn(currentLeases);

        hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                INITIAL_POSITION_LATEST,
                SCOPE,
                ignoreUnexpectedChildShards,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());

        verify(shardDetector, atLeast(1)).listShardsWithoutConsumingResourceNotFoundException();
    }

    /**
     * Tries to boostrap empty lease table. Verifies that if we fail to get a complete hash range of shards after three
     * retries, we fast fail and throw an exception.
     */
    @Test(expected = KinesisClientLibIOException.class)
    public void testEmptyLeaseTableThrowsExceptionWhenHashRangeIsStillIncompleteAfterRetries() throws Exception {
        final List<Shard> shardsWithIncompleteHashRange = Arrays.asList(
                ShardObjectHelper.newShard(
                        "shardId-0",
                        null,
                        null,
                        ShardObjectHelper.newSequenceNumberRange("1", "2"),
                        ShardObjectHelper.newHashKeyRange("0", "1")),
                ShardObjectHelper.newShard(
                        "shardId-1",
                        null,
                        null,
                        ShardObjectHelper.newSequenceNumberRange("1", "2"),
                        ShardObjectHelper.newHashKeyRange("2", "3")));

        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenReturn(true);
        when(shardDetector.listShardsWithFilter(any(ShardFilter.class))).thenReturn(shardsWithIncompleteHashRange);

        try {
            hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                    shardDetector,
                    dynamoDBLeaseRefresher,
                    INITIAL_POSITION_LATEST,
                    SCOPE,
                    ignoreUnexpectedChildShards,
                    dynamoDBLeaseRefresher.isLeaseTableEmpty());
        } finally {
            verify(shardDetector, times(3)).listShardsWithFilter(any(ShardFilter.class)); // Verify retries.
        }
    }

    /**
     * Tries to bootstrap an empty lease table. Verifies that after getting an incomplete hash range of shards two times
     * and a complete hash range the final time, we create the leases.
     */
    @Test
    public void testEmptyLeaseTablePopulatesLeasesWithCompleteHashRangeAfterTwoRetries() throws Exception {
        final List<Shard> shardsWithIncompleteHashRange = Arrays.asList(
                ShardObjectHelper.newShard(
                        "shardId-0",
                        null,
                        null,
                        ShardObjectHelper.newSequenceNumberRange("1", "2"),
                        ShardObjectHelper.newHashKeyRange(ShardObjectHelper.MIN_HASH_KEY, "69")),
                ShardObjectHelper.newShard(
                        "shardId-1",
                        null,
                        null,
                        ShardObjectHelper.newSequenceNumberRange("1", "2"),
                        ShardObjectHelper.newHashKeyRange("71", ShardObjectHelper.MAX_HASH_KEY)));
        final List<Shard> shardsWithCompleteHashRange = Arrays.asList(
                ShardObjectHelper.newShard(
                        "shardId-2",
                        null,
                        null,
                        ShardObjectHelper.newSequenceNumberRange("1", "2"),
                        ShardObjectHelper.newHashKeyRange(ShardObjectHelper.MIN_HASH_KEY, "420")),
                ShardObjectHelper.newShard(
                        "shardId-3",
                        null,
                        null,
                        ShardObjectHelper.newSequenceNumberRange("1", "2"),
                        ShardObjectHelper.newHashKeyRange("421", ShardObjectHelper.MAX_HASH_KEY)));

        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenReturn(true);
        when(shardDetector.listShardsWithFilter(any(ShardFilter.class)))
                .thenReturn(shardsWithIncompleteHashRange)
                .thenReturn(shardsWithIncompleteHashRange)
                .thenReturn(shardsWithCompleteHashRange);

        hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                INITIAL_POSITION_LATEST,
                SCOPE,
                ignoreUnexpectedChildShards,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());

        verify(shardDetector, times(3)).listShardsWithFilter(any(ShardFilter.class)); // Verify retries.
        verify(dynamoDBLeaseRefresher, times(2)).createLeaseIfNotExists(any(Lease.class));
    }

    /**
     * Tries to bootstrap an empty lease table. Verifies that leases are created when we have a complete hash range of shards.
     */
    @Test
    public void testEmptyLeaseTablePopulatesLeasesWithCompleteHashRange() throws Exception {
        final List<Shard> shardsWithCompleteHashRange = Arrays.asList(
                ShardObjectHelper.newShard(
                        "shardId-2",
                        null,
                        null,
                        ShardObjectHelper.newSequenceNumberRange("1", "2"),
                        ShardObjectHelper.newHashKeyRange(ShardObjectHelper.MIN_HASH_KEY, "420")),
                ShardObjectHelper.newShard(
                        "shardId-3",
                        null,
                        null,
                        ShardObjectHelper.newSequenceNumberRange("1", "2"),
                        ShardObjectHelper.newHashKeyRange("421", ShardObjectHelper.MAX_HASH_KEY)));

        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenReturn(true);
        when(shardDetector.listShardsWithFilter(any(ShardFilter.class))).thenReturn(shardsWithCompleteHashRange);

        hierarchicalShardSyncer.checkAndCreateLeaseForNewShards(
                shardDetector,
                dynamoDBLeaseRefresher,
                INITIAL_POSITION_LATEST,
                SCOPE,
                ignoreUnexpectedChildShards,
                dynamoDBLeaseRefresher.isLeaseTableEmpty());

        verify(shardDetector, times(1)).listShardsWithFilter(any(ShardFilter.class)); // Verify retries.
        verify(dynamoDBLeaseRefresher, times(2)).createLeaseIfNotExists(any(Lease.class));
    }

    //    /**
    //     * Test CheckIfDescendantAndAddNewLeasesForAncestors - two parents, there is a lease for one parent.
    //     */
    //    @Test
    // public void testCheckIfDescendantAndAddNewLeasesForAncestors2P2A1PDescendant() {
    //        Set<String> shardIdsOfCurrentLeases = new HashSet<String>();
    //        Map<String, Lease> newLeaseMap = new HashMap<String, Lease>();
    //        Map<String, Shard> kinesisShards = new HashMap<String, Shard>();
    //
    //        String parentShardId = "shardId-parent";
    //        kinesisShards.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null));
    //        shardIdsOfCurrentLeases.add(parentShardId);
    //
    //        String adjacentParentShardId = "shardId-adjacentParent";
    //        kinesisShards.put(adjacentParentShardId, ShardObjectHelper.newShard(adjacentParentShardId, null, null,
    // null));
    //
    //        String shardId = "shardId-9-1";
    //        Shard shard = ShardObjectHelper.newShard(shardId, parentShardId, adjacentParentShardId, null);
    //        kinesisShards.put(shardId, shard);
    //
    //        Map<String, Boolean> memoizationContext = new HashMap<>();
    //        assertTrue(ShardSyncer.checkIfDescendantAndAddNewLeasesForAncestors(shardId, INITIAL_POSITION_LATEST,
    //                shardIdsOfCurrentLeases,
    //                kinesisShards,
    //                newLeaseMap,
    //                memoizationContext));
    //        assertEquals(1, newLeaseMap.size());
    //        assertTrue(newLeaseMap.containsKey(adjacentParentShardId));
    //        Lease adjacentParentLease = newLeaseMap.get(adjacentParentShardId);
    //        assertEquals(ExtendedSequenceNumber.LATEST, adjacentParentLease.checkpoint());
    //    }
    //
    //    /**
    //     * Test parentShardIds() when the shard has no parents.
    //     */
    //    @Test
    // public void testGetParentShardIdsNoParents() {
    //        Shard shard = new Shard();
    //        assertTrue(ShardSyncer.getParentShardIds(shard, null).isEmpty());
    //    }
    //
    //    /**
    //     * Test parentShardIds() when the shard has no parents.
    //     */
    //    @Test
    // public void testGetParentShardIdsTrimmedParents() {
    //        Map<String, Shard> shardMap = new HashMap<String, Shard>();
    //        Shard shard = ShardObjectHelper.newShard("shardId-test", "foo", "bar", null);
    //        assertTrue(ShardSyncer.getParentShardIds(shard, shardMap).isEmpty());
    //    }
    //
    //    /**
    //     * Test parentShardIds() when the shard has a single parent.
    //     */
    //    @Test
    // public void testGetParentShardIdsSingleParent() {
    //        Map<String, Shard> shardMap = new HashMap<String, Shard>();
    //
    //        String parentShardId = "shardId-parent";
    //        shardMap.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null));
    //
    //        Shard shard = ShardObjectHelper.newShard("shardId-test", parentShardId, null, null);
    //        Set<String> parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
    //        assertEquals(1, parentShardIds.size());
    //        assertTrue(parentShardIds.contains(parentShardId));
    //
    //        shard.setParentShardId(null);
    //        parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
    //        assertTrue(parentShardIds.isEmpty());
    //
    //        shard.setAdjacentParentShardId(parentShardId);
    //        parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
    //        assertEquals(1, parentShardIds.size());
    //        assertTrue(parentShardIds.contains(parentShardId));
    //    }
    //
    //    /**
    //     * Test parentShardIds() when the shard has two parents, one is trimmed.
    //     */
    //    @Test
    // public void testGetParentShardIdsOneTrimmedParent() {
    //        Map<String, Shard> shardMap = new HashMap<String, Shard>();
    //
    //        String parentShardId = "shardId-parent";
    //        Shard parent = ShardObjectHelper.newShard(parentShardId, null, null, null);
    //
    //        String adjacentParentShardId = "shardId-adjacentParent";
    //        Shard adjacentParent = ShardObjectHelper.newShard(adjacentParentShardId, null, null, null);
    //
    //        Shard shard = ShardObjectHelper.newShard("shardId-test", parentShardId, adjacentParentShardId, null);
    //
    //        shardMap.put(parentShardId, parent);
    //        Set<String> parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
    //        assertEquals(1, parentShardIds.size());
    //        assertTrue(parentShardIds.contains(parentShardId));
    //
    //        shardMap.remove(parentShardId);
    //        parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
    //        assertTrue(parentShardIds.isEmpty());
    //
    //        shardMap.put(adjacentParentShardId, adjacentParent);
    //        parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
    //        assertEquals(1, parentShardIds.size());
    //        assertTrue(parentShardIds.contains(adjacentParentShardId));
    //    }
    //
    //    /**
    //     * Test parentShardIds() when the shard has two parents.
    //     */
    //    @Test
    // public void testGetParentShardIdsTwoParents() {
    //        Map<String, Shard> shardMap = new HashMap<String, Shard>();
    //
    //        String parentShardId = "shardId-parent";
    //        shardMap.put(parentShardId, ShardObjectHelper.newShard(parentShardId, null, null, null));
    //
    //        String adjacentParentShardId = "shardId-adjacentParent";
    //        shardMap.put(adjacentParentShardId, ShardObjectHelper.newShard(adjacentParentShardId, null, null, null));
    //
    //        Shard shard = ShardObjectHelper.newShard("shardId-test", parentShardId, adjacentParentShardId, null);
    //
    //        Set<String> parentShardIds = ShardSyncer.getParentShardIds(shard, shardMap);
    //        assertEquals(2, parentShardIds.size());
    //        assertTrue(parentShardIds.contains(parentShardId));
    //        assertTrue(parentShardIds.contains(adjacentParentShardId));
    //    }
    //
    //    /**
    //     */
    //    @Test
    // public void testNewLease() {
    //        Shard shard = new Shard();
    //        String shardId = "shardId-95";
    //        shard.setShardId(shardId);
    //        String parentShardId = "shardId-parent";
    //        String adjacentParentShardId = "shardId-adjacentParent";
    //        shard.setParentShardId(parentShardId);
    //        shard.setAdjacentParentShardId(adjacentParentShardId);
    //
    //        Lease lease = ShardSyncer.newKCLLease(shard);
    //        assertEquals(shardId, lease.leaseKey());
    //        assertNull(lease.checkpoint());
    //        Set<String> parentIds = lease.parentShardIds();
    //        assertEquals(2, parentIds.size());
    //        assertTrue(parentIds.contains(parentShardId));
    //        assertTrue(parentIds.contains(adjacentParentShardId));
    //    }
    //
    //    /**
    //     * Test method for constructShardIdToShardMap.
    //     *
    //     * .
    //     */
    //    @Test
    // public void testConstructShardIdToShardMap() {
    //        List<Shard> shards = new ArrayList<Shard>(2);
    //        shards.add(ShardObjectHelper.newShard("shardId-0", null, null, null));
    //        shards.add(ShardObjectHelper.newShard("shardId-1", null, null, null));
    //
    //        Map<String, Shard> shardIdToShardMap = ShardSyncer.constructShardIdToShardMap(shards);
    //        assertEquals(shards.size(), shardIdToShardMap.size());
    //        for (Shard shard : shards) {
    //            assertSame(shard, shardIdToShardMap.get(shard.getShardId()));
    //        }
    //    }
    //
    //    /**
    //     * Test getOpenShards() - no shards are open.
    //     */
    //    @Test
    // public void testGetOpenShardsNoneOpen() {
    //        List<Shard> shards = new ArrayList<Shard>();
    //        shards.add(ShardObjectHelper.newShard("shardId-9384",
    //                null,
    //                null,
    //                ShardObjectHelper.newSequenceNumberRange("123", "345")));
    //        assertTrue(ShardSyncer.getOpenShards(shards).isEmpty());
    //    }
    //
    //    /**
    //     * Test getOpenShards() - test null and max end sequence number.
    //     */
    //    @Test
    // public void testGetOpenShardsNullAndMaxEndSeqNum() {
    //        List<Shard> shards = new ArrayList<Shard>();
    //        String shardId = "shardId-2738";
    //        SequenceNumberRange sequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("123", null);
    //        shards.add(ShardObjectHelper.newShard(shardId, null, null, sequenceNumberRange));
    //
    //        // Verify shard is considered open when it has a null end sequence number
    //        List<Shard> openShards = ShardSyncer.getOpenShards(shards);
    //        assertEquals(1, openShards.size());
    //        assertEquals(shardId, openShards.get(0).getShardId());
    //
    //        // Close shard before testing for max sequence number
    //        sequenceNumberRange.setEndingSequenceNumber("1000");
    //        openShards = ShardSyncer.getOpenShards(shards);
    //        assertTrue(openShards.isEmpty());
    //
    //        // Verify shard is considered closed when the end sequence number is set to max allowed sequence number
    //        sequenceNumberRange.setEndingSequenceNumber(MAX_SEQUENCE_NUMBER.toString());
    //        openShards = ShardSyncer.getOpenShards(shards);
    //        assertEquals(0, openShards.size());
    //    }
    //
    //    /**
    //     * Test isCandidateForCleanup
    //     *
    //     * @throws KinesisClientLibIOException
    //     */
    //    @Test
    // public void testIsCandidateForCleanup() throws KinesisClientLibIOException {
    //        String parentShardId = "shardId-0000";
    //        String adjacentParentShardId = "shardId-0001";
    //        String shardId = "shardId-0002";
    //        Lease lease = newLease(shardId);
    //        List<String> parentShardIds = new ArrayList<>();
    //        parentShardIds.add(parentShardId);
    //        parentShardIds.add(adjacentParentShardId);
    //        lease.parentShardIds(parentShardIds);
    //        Set<String> currentKinesisShardIds = new HashSet<>();
    //
    //        currentKinesisShardIds.add(shardId);
    //        assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    //
    //        currentKinesisShardIds.clear();
    //        assertTrue(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    //
    //        currentKinesisShardIds.add(parentShardId);
    //        // assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    //
    //        currentKinesisShardIds.clear();
    //        assertTrue(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    //
    //        currentKinesisShardIds.add(adjacentParentShardId);
    //        // assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    //        currentKinesisShardIds.add(parentShardId);
    //        // assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    //        currentKinesisShardIds.add(shardId);
    //        assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    //    }
    //
    //    /**
    //     * Test isCandidateForCleanup
    //     *
    //     * @throws KinesisClientLibIOException
    //     */
    //    @Test(expected = KinesisClientLibIOException.class)
    // public void testIsCandidateForCleanupParentExists() throws KinesisClientLibIOException {
    //        String parentShardId = "shardId-0000";
    //        String adjacentParentShardId = "shardId-0001";
    //        String shardId = "shardId-0002";
    //        Lease lease = newLease(shardId);
    //        List<String> parentShardIds = new ArrayList<>();
    //        parentShardIds.add(parentShardId);
    //        parentShardIds.add(adjacentParentShardId);
    //        lease.parentShardIds(parentShardIds);
    //        Set<String> currentKinesisShardIds = new HashSet<>();
    //
    //        currentKinesisShardIds.add(parentShardId);
    //        assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    //    }
    //
    //    /**
    //     * Test isCandidateForCleanup
    //     *
    //     * @throws KinesisClientLibIOException
    //     */
    //    @Test(expected = KinesisClientLibIOException.class)
    // public void testIsCandidateForCleanupAdjacentParentExists() throws KinesisClientLibIOException {
    //        String parentShardId = "shardId-0000";
    //        String adjacentParentShardId = "shardId-0001";
    //        String shardId = "shardId-0002";
    //        Lease lease = newLease(shardId);
    //        List<String> parentShardIds = new ArrayList<>();
    //        parentShardIds.add(parentShardId);
    //        parentShardIds.add(adjacentParentShardId);
    //        lease.parentShardIds(parentShardIds);
    //        Set<String> currentKinesisShardIds = new HashSet<>();
    //
    //        currentKinesisShardIds.add(adjacentParentShardId);
    //        assertFalse(ShardSyncer.isCandidateForCleanup(lease, currentKinesisShardIds));
    //    }
    //
    //    /**
    //     * Test cleanup of lease for a shard that has been fully processed (and processing of child shards has begun).
    //     *
    //     * @throws DependencyException
    //     * @throws InvalidStateException
    //     * @throws ProvisionedThroughputException
    //     */
    //    @Test
    // public void testCleanupLeaseForClosedShard()
    //        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
    //        String closedShardId = "shardId-2";
    //        Lease leaseForClosedShard = newLease(closedShardId);
    //        leaseForClosedShard.checkpoint(new ExtendedSequenceNumber("1234"));
    //        dynamoDBLeaseRefresher.createLeaseIfNotExists(leaseForClosedShard);
    //
    //        Set<String> childShardIds = new HashSet<>();
    //        List<Lease> trackedLeases = new ArrayList<>();
    //        Set<String> parentShardIds = new HashSet<>();
    //        parentShardIds.add(closedShardId);
    //        String childShardId1 = "shardId-5";
    //        Lease childLease1 = newLease(childShardId1);
    //        childLease1.parentShardIds(parentShardIds);
    //        childLease1.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
    //        String childShardId2 = "shardId-7";
    //        Lease childLease2 = newLease(childShardId2);
    //        childLease2.parentShardIds(parentShardIds);
    //        childLease2.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
    //        Map<String, Lease> trackedLeaseMap = ShardSyncer.constructShardIdToKCLLeaseMap(trackedLeases);
    //
    //        // empty list of leases
    //        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap,
    // dynamoDBLeaseRefresher);
    //        assertNotNull(dynamoDBLeaseRefresher.getLease(closedShardId));
    //
    //        // closed shard has not been fully processed yet (checkpoint != SHARD_END)
    //        trackedLeases.add(leaseForClosedShard);
    //        trackedLeaseMap = ShardSyncer.constructShardIdToKCLLeaseMap(trackedLeases);
    //        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap,
    // dynamoDBLeaseRefresher);
    //        assertNotNull(dynamoDBLeaseRefresher.getLease(closedShardId));
    //
    //        // closed shard has been fully processed yet (checkpoint == SHARD_END)
    //        leaseForClosedShard.checkpoint(ExtendedSequenceNumber.SHARD_END);
    //        dynamoDBLeaseRefresher.updateLease(leaseForClosedShard);
    //        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap,
    // dynamoDBLeaseRefresher);
    //        assertNull(dynamoDBLeaseRefresher.getLease(closedShardId));
    //
    //        // lease for only one child exists
    //        childShardIds.add(childShardId1);
    //        childShardIds.add(childShardId2);
    //        dynamoDBLeaseRefresher.createLeaseIfNotExists(leaseForClosedShard);
    //        dynamoDBLeaseRefresher.createLeaseIfNotExists(childLease1);
    //        trackedLeases.add(childLease1);
    //        trackedLeaseMap = ShardSyncer.constructShardIdToKCLLeaseMap(trackedLeases);
    //        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap,
    // dynamoDBLeaseRefresher);
    //        assertNotNull(dynamoDBLeaseRefresher.getLease(closedShardId));
    //
    //        // leases for both children exists, but they are both at TRIM_HORIZON
    //        dynamoDBLeaseRefresher.createLeaseIfNotExists(childLease2);
    //        trackedLeases.add(childLease2);
    //        trackedLeaseMap = ShardSyncer.constructShardIdToKCLLeaseMap(trackedLeases);
    //        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap,
    // dynamoDBLeaseRefresher);
    //        assertNotNull(dynamoDBLeaseRefresher.getLease(closedShardId));
    //
    //        // leases for both children exists, one is at TRIM_HORIZON
    //        childLease1.checkpoint(new ExtendedSequenceNumber("34890"));
    //        dynamoDBLeaseRefresher.updateLease(childLease1);
    //        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap,
    // dynamoDBLeaseRefresher);
    //        assertNotNull(dynamoDBLeaseRefresher.getLease(closedShardId));
    //
    //        // leases for both children exists, NONE of them are at TRIM_HORIZON
    //        childLease2.checkpoint(new ExtendedSequenceNumber("43789"));
    //        dynamoDBLeaseRefresher.updateLease(childLease2);
    //        ShardSyncer.cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap,
    // dynamoDBLeaseRefresher);
    //        assertNull(dynamoDBLeaseRefresher.getLease(closedShardId));
    //    }
    //
    //    /**
    //     * Test we can handle trimmed Kinesis shards (absent from the shard list), and valid closed shards.
    //     *
    //     * @throws KinesisClientLibIOException
    //     */
    //    @Test
    // public void testAssertShardCoveredOrAbsentTestAbsentAndValid() throws KinesisClientLibIOException {
    //        List<Shard> shards = new ArrayList<>();
    //        String expectedClosedShardId = "shardId-34098";
    //        SequenceNumberRange sequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("103", "205");
    //        HashKeyRange hashKeyRange = ShardObjectHelper.newHashKeyRange("10", "25");
    //        Shard closedShard =
    //                ShardObjectHelper.newShard(expectedClosedShardId, null, null, sequenceNumberRange, hashKeyRange);
    //        SequenceNumberRange childSequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("206", "300");
    //        Shard child1 =
    //                ShardObjectHelper.newShard("shardId-54879", expectedClosedShardId, null,
    // childSequenceNumberRange);
    //        Map<String, Shard> shardIdToShardMap = ShardSyncer.constructShardIdToShardMap(shards);
    //        Map<String, Set<String>> shardIdToChildShardIdsMap =
    //                ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
    //        Set<String> closedShardIds = new HashSet<>();
    //        closedShardIds.add(expectedClosedShardId);
    //        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap,
    // closedShardIds);
    //
    //        // test for case where shard has been trimmed (absent from list)
    //        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap,
    // closedShardIds);
    //
    //        // Populate shards.
    //        shards.add(closedShard);
    //        shards.add(child1);
    //        shardIdToShardMap.put(expectedClosedShardId, closedShard);
    //        shardIdToShardMap.put(child1.getShardId(), child1);
    //        shardIdToChildShardIdsMap = ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
    //
    //        // test degenerate split/merge
    //        child1.setHashKeyRange(hashKeyRange);
    //        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap,
    // closedShardIds);
    //
    //        // test merge
    //        child1.setHashKeyRange(ShardObjectHelper.newHashKeyRange("10", "2985"));
    //        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap,
    // closedShardIds);
    //        child1.setHashKeyRange(ShardObjectHelper.newHashKeyRange("3", "25"));
    //        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap,
    // closedShardIds);
    //
    //        // test split
    //        HashKeyRange childHashKeyRange1 = ShardObjectHelper.newHashKeyRange("10", "15");
    //        HashKeyRange childHashKeyRange2 = ShardObjectHelper.newHashKeyRange("16", "25");
    //        child1.setHashKeyRange(childHashKeyRange1);
    //        Shard child2 = ShardObjectHelper.newShard("shardId-43789",
    //                null,
    //                expectedClosedShardId,
    //                childSequenceNumberRange,
    //                childHashKeyRange2);
    //        shards.add(child2);
    //        shardIdToShardMap.put(child2.getShardId(), child2);
    //        shardIdToChildShardIdsMap = ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
    //        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap,
    // closedShardIds);
    //    }
    //
    //    /**
    //     * Test we throw an exception if the shard is open
    //     *
    //     * @throws KinesisClientLibIOException
    //     */
    //    @Test(expected = KinesisClientLibIOException.class)
    // public void testAssertShardCoveredOrAbsentTestOpen() throws KinesisClientLibIOException {
    //        List<Shard> shards = new ArrayList<>();
    //        String expectedClosedShardId = "shardId-34098";
    //        SequenceNumberRange sequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("103", null);
    //        HashKeyRange hashKeyRange = ShardObjectHelper.newHashKeyRange("10", "25");
    //        Shard openShard =
    //                ShardObjectHelper.newShard(expectedClosedShardId, null, null, sequenceNumberRange, hashKeyRange);
    //        shards.add(openShard);
    //        Map<String, Shard> shardIdToShardMap = ShardSyncer.constructShardIdToShardMap(shards);
    //        Map<String, Set<String>> shardIdToChildShardIdsMap =
    //                ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
    //        Set<String> closedShardIds = new HashSet<>();
    //        closedShardIds.add(expectedClosedShardId);
    //        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap,
    // closedShardIds);
    //    }
    //
    //    /**
    //     * Test we throw an exception if there are no children
    //     *
    //     * @throws KinesisClientLibIOException
    //     */
    //    @Test(expected = KinesisClientLibIOException.class)
    // public void testAssertShardCoveredOrAbsentTestNoChildren() throws KinesisClientLibIOException {
    //        List<Shard> shards = new ArrayList<>();
    //        String expectedClosedShardId = "shardId-34098";
    //        SequenceNumberRange sequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("103", "205");
    //        HashKeyRange hashKeyRange = ShardObjectHelper.newHashKeyRange("10", "25");
    //        Shard closedShard =
    //                ShardObjectHelper.newShard(expectedClosedShardId, null, null, sequenceNumberRange, hashKeyRange);
    //        shards.add(closedShard);
    //        Map<String, Shard> shardIdToShardMap = ShardSyncer.constructShardIdToShardMap(shards);
    //        Map<String, Set<String>> shardIdToChildShardIdsMap =
    //                ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
    //        Set<String> closedShardIds = new HashSet<>();
    //        closedShardIds.add(expectedClosedShardId);
    //        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap,
    // closedShardIds);
    //    }
    //
    //    /**
    //     * Test we throw an exception if children don't cover hash key range (min of children > min of parent)
    //     *
    //     * @throws KinesisClientLibIOException
    //     */
    //    @Test(expected = KinesisClientLibIOException.class)
    // public void testAssertShardCoveredOrAbsentTestIncompleteSplitMin() throws KinesisClientLibIOException {
    //        HashKeyRange hashKeyRange = ShardObjectHelper.newHashKeyRange("10", "25");
    //        HashKeyRange childHashKeyRange1 = ShardObjectHelper.newHashKeyRange("12", "15");
    //        HashKeyRange childHashKeyRange2 = ShardObjectHelper.newHashKeyRange("16", "25");
    //        testAssertShardCoveredOrAbsentTestIncompleteSplit(hashKeyRange, childHashKeyRange1, childHashKeyRange2);
    //    }
    //
    //    /**
    //     * Test we throw an exception if children don't cover hash key range (max of children < max of parent)
    //     *
    //     * @throws KinesisClientLibIOException
    //     */
    //    @Test(expected = KinesisClientLibIOException.class)
    // public void testAssertShardCoveredOrAbsentTestIncompleteSplitMax() throws KinesisClientLibIOException {
    //        HashKeyRange hashKeyRange = ShardObjectHelper.newHashKeyRange("10", "25");
    //        HashKeyRange childHashKeyRange1 = ShardObjectHelper.newHashKeyRange("10", "15");
    //        HashKeyRange childHashKeyRange2 = ShardObjectHelper.newHashKeyRange("16", "23");
    //        testAssertShardCoveredOrAbsentTestIncompleteSplit(hashKeyRange, childHashKeyRange1, childHashKeyRange2);
    //    }
    //
    //    private void testAssertShardCoveredOrAbsentTestIncompleteSplit(HashKeyRange parentHashKeyRange,
    //            HashKeyRange child1HashKeyRange,
    //            HashKeyRange child2HashKeyRange)
    //        throws KinesisClientLibIOException {
    //        List<Shard> shards = new ArrayList<>();
    //        String expectedClosedShardId = "shardId-34098";
    //        SequenceNumberRange sequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("103", "205");
    //        Shard closedShard =
    //                ShardObjectHelper.newShard(expectedClosedShardId, null, null, sequenceNumberRange,
    // parentHashKeyRange);
    //        shards.add(closedShard);
    //
    //        SequenceNumberRange childSequenceNumberRange = ShardObjectHelper.newSequenceNumberRange("206", "300");
    //        Shard child1 = ShardObjectHelper.newShard("shardId-43789",
    //                null,
    //                expectedClosedShardId,
    //                childSequenceNumberRange,
    //                child1HashKeyRange);
    //        shards.add(child1);
    //        Shard child2 = ShardObjectHelper.newShard("shardId-43789",
    //                null,
    //                expectedClosedShardId,
    //                childSequenceNumberRange,
    //                child2HashKeyRange);
    //        shards.add(child2);
    //
    //        Map<String, Shard> shardIdToShardMap = ShardSyncer.constructShardIdToShardMap(shards);
    //        Map<String, Set<String>> shardIdToChildShardIdsMap =
    //                ShardSyncer.constructShardIdToChildShardIdsMap(shardIdToShardMap);
    //        Set<String> closedShardIds = new HashSet<>();
    //        closedShardIds.add(expectedClosedShardId);
    //        ShardSyncer.assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap,
    // closedShardIds);
    //    }
    //
    /**
     * Helper method.
     */
    private static Lease newLease(final String shardId) {
        final Lease lease = new Lease();
        lease.leaseKey(shardId);

        return lease;
    }
}
