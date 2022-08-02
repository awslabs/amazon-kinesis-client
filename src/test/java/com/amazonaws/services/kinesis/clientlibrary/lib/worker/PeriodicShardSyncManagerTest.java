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

import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.impl.HashKeyRangeForLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.util.CollectionUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.PeriodicShardSyncManager.MAX_HASH_KEY;
import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.PeriodicShardSyncManager.MIN_HASH_KEY;
import static com.amazonaws.services.kinesis.leases.impl.HashKeyRangeForLease.deserialize;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PeriodicShardSyncManagerTest {

    private static final String WORKER_ID = "workerId";
    public static final long LEASES_RECOVERY_AUDITOR_EXECUTION_FREQUENCY_MILLIS = 2 * 60 * 1000L;
    public static final int LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD = 3;

    /** Manager for PERIODIC shard sync strategy */
    private PeriodicShardSyncManager periodicShardSyncManager;

    /** Manager for SHARD_END shard sync strategy */
    private PeriodicShardSyncManager auditorPeriodicShardSyncManager;

    @Mock
    private LeaderDecider leaderDecider;
    @Mock
    private ShardSyncTask shardSyncTask;
    @Mock
    private ILeaseManager<KinesisClientLease> leaseManager;
    @Mock
    private IKinesisProxy kinesisProxy;

    private IMetricsFactory metricsFactory = new NullMetricsFactory();

    @Before
    public void setup() {
        periodicShardSyncManager = new PeriodicShardSyncManager(WORKER_ID, leaderDecider, shardSyncTask,
                metricsFactory, leaseManager, kinesisProxy, false, LEASES_RECOVERY_AUDITOR_EXECUTION_FREQUENCY_MILLIS,
                LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD);
        auditorPeriodicShardSyncManager = new PeriodicShardSyncManager(WORKER_ID, leaderDecider, shardSyncTask,
                metricsFactory, leaseManager, kinesisProxy, true, LEASES_RECOVERY_AUDITOR_EXECUTION_FREQUENCY_MILLIS, 
                LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD);
    }

    @Test
    public void testForFailureWhenHashRangesAreIncomplete() {
        List<KinesisClientLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("25", MAX_HASH_KEY.toString())); // Missing interval here
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());
        Assert.assertTrue(PeriodicShardSyncManager
                .checkForHoleInHashKeyRanges(hashRanges).isPresent());
    }

    @Test
    public void testForSuccessWhenHashRangesAreComplete() {
        List<KinesisClientLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());
        Assert.assertFalse(PeriodicShardSyncManager
                .checkForHoleInHashKeyRanges(hashRanges).isPresent());
    }

    @Test
    public void testForSuccessWhenUnsortedHashRangesAreComplete() {
        List<KinesisClientLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("4", "23"));
            add(deserialize("2", "3"));
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
            add(deserialize("6", "23"));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());
        Assert.assertFalse(PeriodicShardSyncManager
                .checkForHoleInHashKeyRanges(hashRanges).isPresent());
    }

    @Test
    public void testForSuccessWhenHashRangesAreCompleteForOverlappingLeasesAtEnd() {
        List<KinesisClientLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
            add(deserialize("24", "45"));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());
        Assert.assertFalse(PeriodicShardSyncManager
                .checkForHoleInHashKeyRanges(hashRanges).isPresent());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenNoLeasesArePassed() throws Exception {
        when(leaseManager.listLeases()).thenReturn(null);
        Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        Assert.assertTrue(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());

    }

    @Test
    public void testIfShardSyncIsInitiatedWhenEmptyLeasesArePassed() throws Exception {
        when(leaseManager.listLeases()).thenReturn(Collections.emptyList());
        Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        Assert.assertTrue(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenConfidenceFactorIsNotReached() throws Exception {
        List<KinesisClientLease> leases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23")); // Hole between 23 and 25
            add(deserialize("25", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());

        when(leaseManager.listLeases()).thenReturn(leases);
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenConfidenceFactorIsReached() throws Exception {
        List<KinesisClientLease> leases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23")); // Hole between 23 and 25
            add(deserialize("25", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());

        when(leaseManager.listLeases()).thenReturn(leases);
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }
        Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        Assert.assertTrue(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenHoleIsDueToShardEnd() throws Exception {
        List<KinesisClientLease> leases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("6", "23")); // Introducing hole here through SHARD_END checkpoint
            add(deserialize("4", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            if (lease.getHashKeyRange().startingHashKey().toString().equals("4")) {
                lease.setCheckpoint(ExtendedSequenceNumber.SHARD_END);
            } else {
                lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            }
            return lease;
        }).collect(Collectors.toList());

        when(leaseManager.listLeases()).thenReturn(leases);
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }
        Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        Assert.assertTrue(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenNoLeasesAreUsedDueToShardEnd() throws Exception {
        List<KinesisClientLease> leases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.SHARD_END);
            return lease;
        }).collect(Collectors.toList());

        when(leaseManager.listLeases()).thenReturn(leases);
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }
        Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        Assert.assertTrue(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenHoleShifts() throws Exception {
        List<KinesisClientLease> leases1 = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23")); // Hole between 23 and 25
            add(deserialize("25", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());

        when(leaseManager.listLeases()).thenReturn(leases1);
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }

        List<KinesisClientLease> leases2 = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3")); // Hole between 3 and 5
            add(deserialize("5", "23"));
            add(deserialize("6", "23"));
            add(deserialize("25", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());

        // Resetting the holes
        when(leaseManager.listLeases()).thenReturn(leases2);
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }
        Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        Assert.assertTrue(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenHoleShiftsMoreThanOnce() throws Exception {
        List<KinesisClientLease> leases1 = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23")); // Hole between 23 and 25
            add(deserialize("25", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());

        when(leaseManager.listLeases()).thenReturn(leases1);
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }

        List<KinesisClientLease> leases2 = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3")); // Hole between 3 and 5
            add(deserialize("5", "23"));
            add(deserialize("6", "23"));
            add(deserialize("25", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setHashKeyRange(hashKeyRangeForLease);
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());

        // Resetting the holes
        when(leaseManager.listLeases()).thenReturn(leases2);
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }
        // Resetting the holes again
        when(leaseManager.listLeases()).thenReturn(leases1);
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }
        Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        Assert.assertTrue(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
    }

    @Test
    public void testIfMissingHashRangeInformationIsFilledBeforeEvaluatingForShardSync() throws Exception {
        final int[] shardCounter = { 0 };
        List<HashKeyRangeForLease> hashKeyRangeForLeases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "20"));
            add(deserialize("21", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }};

        List<Shard> kinesisShards = hashKeyRangeForLeases.stream()
                .map(hashKeyRange -> new Shard()
                        .withShardId("shard-" + ++shardCounter[0])
                        .withHashKeyRange(new HashKeyRange()
                                .withStartingHashKey(hashKeyRange.serializedStartingHashKey())
                                .withEndingHashKey(hashKeyRange.serializedEndingHashKey())))
                .collect(Collectors.toList());

        when(kinesisProxy.getShardList()).thenReturn(kinesisShards);

        final int[] leaseCounter = { 0 };
        List<KinesisClientLease> leases = hashKeyRangeForLeases.stream()
                .map(hashKeyRange -> {
                    KinesisClientLease lease = new KinesisClientLease();
                    lease.setLeaseKey("shard-" + ++leaseCounter[0]);
                    // Setting the hash range only for the last two leases
                    if (leaseCounter[0] >= 3) {
                        lease.setHashKeyRange(hashKeyRange);
                    }
                    lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                }).collect(Collectors.toList());

        when(leaseManager.listLeases()).thenReturn(leases);

        // Assert that SHARD_END shard sync should never trigger, but PERIODIC shard sync should always trigger
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }
        Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());

        // Assert that all the leases now have hash ranges set
        for (KinesisClientLease lease : leases) {
            Assert.assertNotNull(lease.getHashKeyRange());
        }
    }

    @Test
    public void testIfMissingHashRangeInformationIsFilledBeforeEvaluatingForShardSyncInHoleScenario() throws Exception {
        final int[] shardCounter = { 0 };
        List<HashKeyRangeForLease> hashKeyRangeForLeases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("5", "20")); // Hole between 3 and 5
            add(deserialize("21", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }};

        List<Shard> kinesisShards = hashKeyRangeForLeases.stream()
                .map(hashKeyRange -> new Shard()
                        .withShardId("shard-" + ++shardCounter[0])
                        .withHashKeyRange(new HashKeyRange()
                                .withStartingHashKey(hashKeyRange.serializedStartingHashKey())
                                .withEndingHashKey(hashKeyRange.serializedEndingHashKey())))
                .collect(Collectors.toList());

        when(kinesisProxy.getShardList()).thenReturn(kinesisShards);

        final int[] leaseCounter = { 0 };
        List<KinesisClientLease> leases = hashKeyRangeForLeases.stream()
                .map(hashKeyRange -> {
                    KinesisClientLease lease = new KinesisClientLease();
                    lease.setLeaseKey("shard-" + ++leaseCounter[0]);
                    // Setting the hash range only for the last two leases
                    if (leaseCounter[0] >= 3) {
                        lease.setHashKeyRange(hashKeyRange);
                    }
                    lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                }).collect(Collectors.toList());

        when(leaseManager.listLeases()).thenReturn(leases);

        // Assert that shard sync should trigger after breaching threshold
        for (int i = 1; i < LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD; i++) {
            Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
            Assert.assertFalse(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        }
        Assert.assertTrue(periodicShardSyncManager.checkForShardSync().shouldDoShardSync());
        Assert.assertTrue(auditorPeriodicShardSyncManager.checkForShardSync().shouldDoShardSync());

        // Assert that all the leases now have hash ranges set
        for (KinesisClientLease lease : leases) {
            Assert.assertNotNull(lease.getHashKeyRange());
        }
    }

    @Test
    public void testFor1000DifferentValidSplitHierarchyTreeTheHashRangesAreAlwaysComplete() {
        for (int i = 0; i < 1000; i++) {
            int maxInitialLeaseCount = 100;
            List<KinesisClientLease> leases = generateInitialLeases(maxInitialLeaseCount);
            reshard(leases, 5, ReshardType.SPLIT, maxInitialLeaseCount, false);
            Collections.shuffle(leases);
            Assert.assertFalse(periodicShardSyncManager.hasHoleInLeases(leases).isPresent());
            Assert.assertFalse(auditorPeriodicShardSyncManager.hasHoleInLeases(leases).isPresent());
        }
    }

    @Test
    public void testFor1000DifferentValidMergeHierarchyTreeWithSomeInProgressParentsTheHashRangesAreAlwaysComplete() {
        for (int i = 0; i < 1000; i++) {
            int maxInitialLeaseCount = 100;
            List<KinesisClientLease> leases = generateInitialLeases(maxInitialLeaseCount);
            reshard(leases, 5, ReshardType.MERGE, maxInitialLeaseCount, true);
            Collections.shuffle(leases);
            Assert.assertFalse(periodicShardSyncManager.hasHoleInLeases(leases).isPresent());
            Assert.assertFalse(auditorPeriodicShardSyncManager.hasHoleInLeases(leases).isPresent());
        }
    }

    @Test
    public void testFor1000DifferentValidReshardHierarchyTreeWithSomeInProgressParentsTheHashRangesAreAlwaysComplete() {
        for (int i = 0; i < 1000; i++) {
            int maxInitialLeaseCount = 100;
            List<KinesisClientLease> leases = generateInitialLeases(maxInitialLeaseCount);
            reshard(leases, 5, ReshardType.ANY, maxInitialLeaseCount, true);
            Collections.shuffle(leases);
            Assert.assertFalse(periodicShardSyncManager.hasHoleInLeases(leases).isPresent());
            Assert.assertFalse(auditorPeriodicShardSyncManager.hasHoleInLeases(leases).isPresent());
        }
    }

    private List<KinesisClientLease> generateInitialLeases(int initialShardCount) {
        long hashRangeInternalMax = 10000000;
        List<KinesisClientLease> initialLeases = new ArrayList<>();
        long leaseStartKey = 0;
        for (int i = 1; i <= initialShardCount; i++) {
            final KinesisClientLease lease = new KinesisClientLease();
            long leaseEndKey;
            if (i != initialShardCount) {
                leaseEndKey = (hashRangeInternalMax / initialShardCount) * i;
                lease.setHashKeyRange(deserialize(leaseStartKey + "", leaseEndKey + ""));
            } else {
                leaseEndKey = 0;
                lease.setHashKeyRange(deserialize(leaseStartKey + "", MAX_HASH_KEY.toString()));
            }
            lease.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            lease.setLeaseKey("shard-" + i);
            initialLeases.add(lease);
            leaseStartKey = leaseEndKey + 1;
        }

        return initialLeases;
    }

    private void reshard(List<KinesisClientLease> initialLeases, int depth, ReshardType reshardType, int leaseCounter,
                         boolean shouldKeepSomeParentsInProgress) {
        for (int i = 0; i < depth; i++) {
            if (reshardType == ReshardType.SPLIT) {
                leaseCounter = split(initialLeases, leaseCounter);
            } else if (reshardType == ReshardType.MERGE) {
                leaseCounter = merge(initialLeases, leaseCounter, shouldKeepSomeParentsInProgress);
            } else {
                if (isHeads()) {
                    leaseCounter = split(initialLeases, leaseCounter);
                } else {
                    leaseCounter = merge(initialLeases, leaseCounter, shouldKeepSomeParentsInProgress);
                }
            }
        }
    }

    private int merge(List<KinesisClientLease> initialLeases, int leaseCounter, boolean shouldKeepSomeParentsInProgress) {
        List<KinesisClientLease> leasesEligibleForMerge = initialLeases.stream()
                .filter(l -> CollectionUtils.isNullOrEmpty(l.getChildShardIds())).collect(Collectors.toList());

        int leasesToMerge = (int) ((leasesEligibleForMerge.size() - 1) / 2.0 * Math.random());
        for (int i = 0; i < leasesToMerge; i += 2) {
            KinesisClientLease parent1 = leasesEligibleForMerge.get(i);
            KinesisClientLease parent2 = leasesEligibleForMerge.get(i + 1);
            if (parent2.getHashKeyRange().startingHashKey()
                    .subtract(parent1.getHashKeyRange().endingHashKey()).equals(BigInteger.ONE)) {
                parent1.setCheckpoint(ExtendedSequenceNumber.SHARD_END);

                if (!shouldKeepSomeParentsInProgress || (shouldKeepSomeParentsInProgress && isOneFromDiceRoll())) {
                    parent2.setCheckpoint(ExtendedSequenceNumber.SHARD_END);
                }

                KinesisClientLease child = new KinesisClientLease();
                child.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                child.setLeaseKey("shard-" + ++leaseCounter);
                child.setHashKeyRange(new HashKeyRangeForLease(parent1.getHashKeyRange().startingHashKey(),
                        parent2.getHashKeyRange().endingHashKey()));
                parent1.setChildShardIds(Collections.singletonList(child.getLeaseKey()));
                parent2.setChildShardIds(Collections.singletonList(child.getLeaseKey()));
                child.setParentShardIds(Sets.newHashSet(parent1.getLeaseKey(), parent2.getLeaseKey()));

                initialLeases.add(child);
            }
        }

        return leaseCounter;
    }

    private int split(List<KinesisClientLease> initialLeases, int leaseCounter) {
        List<KinesisClientLease> leasesEligibleForSplit = initialLeases.stream()
                .filter(l -> CollectionUtils.isNullOrEmpty(l.getChildShardIds())).collect(Collectors.toList());

        int leasesToSplit = (int) (leasesEligibleForSplit.size() * Math.random());
        for (int i = 0; i < leasesToSplit; i++) {
            KinesisClientLease parent = leasesEligibleForSplit.get(i);
            parent.setCheckpoint(ExtendedSequenceNumber.SHARD_END);

            KinesisClientLease child1 = new KinesisClientLease();
            child1.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            child1.setHashKeyRange(new HashKeyRangeForLease(parent.getHashKeyRange().startingHashKey(),
                    parent.getHashKeyRange().startingHashKey().add(parent.getHashKeyRange().endingHashKey())
                            .divide(new BigInteger("2"))));
            child1.setLeaseKey("shard-" + ++leaseCounter);

            KinesisClientLease child2 = new KinesisClientLease();
            child2.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            child2.setHashKeyRange(new HashKeyRangeForLease(parent.getHashKeyRange().startingHashKey()
                    .add(parent.getHashKeyRange().endingHashKey()).divide(new BigInteger("2")).add(BigInteger.ONE),
                    parent.getHashKeyRange().endingHashKey()));
            child2.setLeaseKey("shard-" + ++leaseCounter);

            child1.setParentShardIds(Sets.newHashSet(parent.getLeaseKey()));
            child2.setParentShardIds(Sets.newHashSet(parent.getLeaseKey()));
            parent.setChildShardIds(Lists.newArrayList(child1.getLeaseKey(), child2.getLeaseKey()));

            initialLeases.add(child1);
            initialLeases.add(child2);
        }

        return leaseCounter;
    }

    private boolean isHeads() {
        return Math.random() <= 0.5;
    }

    private boolean isOneFromDiceRoll() {
        return Math.random() <= 0.16;
    }

    private enum ReshardType {
        SPLIT,
        MERGE,
        ANY
    }
}
