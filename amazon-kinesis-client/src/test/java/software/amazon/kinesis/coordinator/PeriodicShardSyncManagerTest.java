/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.coordinator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.common.HashKeyRangeForLease;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.common.HashKeyRangeForLease.deserialize;
import static software.amazon.kinesis.coordinator.PeriodicShardSyncManager.MAX_HASH_KEY;
import static software.amazon.kinesis.coordinator.PeriodicShardSyncManager.MIN_HASH_KEY;
import static software.amazon.kinesis.leases.LeaseManagementConfig.DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY;

@RunWith(MockitoJUnitRunner.class)
public class PeriodicShardSyncManagerTest {

    private static final int MAX_DEPTH_WITH_IN_PROGRESS_PARENTS = 1;

    private StreamIdentifier streamIdentifier;
    private PeriodicShardSyncManager periodicShardSyncManager;

    @Mock
    private LeaderDecider leaderDecider;

    @Mock
    private LeaseRefresher leaseRefresher;

    @Mock
    Map<StreamIdentifier, StreamConfig> currentStreamConfigMap;

    @Mock
    Function<StreamConfig, ShardSyncTaskManager> shardSyncTaskManagerProvider;

    @Mock
    Map<StreamConfig, ShardSyncTaskManager> streamToShardSyncTaskManagerMap;

    @Mock
    ScheduledExecutorService mockScheduledExecutor;

    @Before
    public void setup() {
        streamIdentifier = StreamIdentifier.multiStreamInstance("123456789012:stream:456");
        periodicShardSyncManager = new PeriodicShardSyncManager(
                "worker",
                leaseRefresher,
                currentStreamConfigMap,
                shardSyncTaskManagerProvider,
                streamToShardSyncTaskManagerMap,
                mockScheduledExecutor,
                true,
                new NullMetricsFactory(),
                2 * 60 * 1000,
                3,
                new AtomicBoolean(true));
        periodicShardSyncManager.start(leaderDecider);
    }

    @Test
    public void testForFailureWhenHashRangesAreIncomplete() {
        List<Lease> hashRanges = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize("0", "1"));
                add(deserialize("2", "3"));
                add(deserialize("4", "23"));
                add(deserialize("6", "23"));
                add(deserialize("25", MAX_HASH_KEY.toString())); // Missing interval here
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    Lease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());
        Assert.assertTrue(PeriodicShardSyncManager.checkForHoleInHashKeyRanges(streamIdentifier, hashRanges)
                .isPresent());
    }

    @Test
    public void testForSuccessWhenHashRangesAreComplete() {
        List<Lease> hashRanges = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize("0", "1"));
                add(deserialize("2", "3"));
                add(deserialize("4", "23"));
                add(deserialize("6", "23"));
                add(deserialize("24", MAX_HASH_KEY.toString()));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    Lease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());
        Assert.assertFalse(PeriodicShardSyncManager.checkForHoleInHashKeyRanges(streamIdentifier, hashRanges)
                .isPresent());
    }

    @Test
    public void testForSuccessWhenUnSortedHashRangesAreComplete() {
        List<Lease> hashRanges = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize("4", "23"));
                add(deserialize("2", "3"));
                add(deserialize("0", "1"));
                add(deserialize("24", MAX_HASH_KEY.toString()));
                add(deserialize("6", "23"));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    Lease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());
        Assert.assertFalse(PeriodicShardSyncManager.checkForHoleInHashKeyRanges(streamIdentifier, hashRanges)
                .isPresent());
    }

    @Test
    public void testForSuccessWhenHashRangesAreCompleteForOverlappingLeasesAtEnd() {
        List<Lease> hashRanges = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize("0", "1"));
                add(deserialize("2", "3"));
                add(deserialize("4", "23"));
                add(deserialize("6", "23"));
                add(deserialize("24", MAX_HASH_KEY.toString()));
                add(deserialize("24", "45"));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    Lease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());
        Assert.assertFalse(PeriodicShardSyncManager.checkForHoleInHashKeyRanges(streamIdentifier, hashRanges)
                .isPresent());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenNoLeasesArePassed() {
        Assert.assertTrue(periodicShardSyncManager
                .checkForShardSync(streamIdentifier, null)
                .shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenEmptyLeasesArePassed() {
        Assert.assertTrue(periodicShardSyncManager
                .checkForShardSync(streamIdentifier, new ArrayList<>())
                .shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsNotInitiatedWhenConfidenceFactorIsNotReached() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize(MIN_HASH_KEY.toString(), "1"));
                add(deserialize("2", "3"));
                add(deserialize("4", "23"));
                add(deserialize("6", "23")); // Hole between 23 and 25
                add(deserialize("25", MAX_HASH_KEY.toString()));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    MultiStreamLease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases)
                        .shouldDoShardSync()));
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenConfidenceFactorIsReached() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize(MIN_HASH_KEY.toString(), "1"));
                add(deserialize("2", "3"));
                add(deserialize("4", "23"));
                add(deserialize("6", "23")); // Hole between 23 and 25
                add(deserialize("25", MAX_HASH_KEY.toString()));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    MultiStreamLease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases)
                        .shouldDoShardSync()));
        Assert.assertTrue(periodicShardSyncManager
                .checkForShardSync(streamIdentifier, multiStreamLeases)
                .shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenHoleIsDueToShardEnd() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize(MIN_HASH_KEY.toString(), "1"));
                add(deserialize("2", "3"));
                add(deserialize("4", "23")); // introducing hole here through SHARD_END checkpoint
                add(deserialize("6", "23"));
                add(deserialize("24", MAX_HASH_KEY.toString()));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    MultiStreamLease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    if (lease.hashKeyRangeForLease()
                            .startingHashKey()
                            .toString()
                            .equals("4")) {
                        lease.checkpoint(ExtendedSequenceNumber.SHARD_END);
                    } else {
                        lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    }
                    return lease;
                })
                .collect(Collectors.toList());
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases)
                        .shouldDoShardSync()));
        Assert.assertTrue(periodicShardSyncManager
                .checkForShardSync(streamIdentifier, multiStreamLeases)
                .shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenNoLeasesAreUsedDueToShardEnd() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize(MIN_HASH_KEY.toString(), "1"));
                add(deserialize("2", "3"));
                add(deserialize("4", "23"));
                add(deserialize("6", "23"));
                add(deserialize("24", MAX_HASH_KEY.toString()));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    MultiStreamLease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.SHARD_END);
                    return lease;
                })
                .collect(Collectors.toList());
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases)
                        .shouldDoShardSync()));
        Assert.assertTrue(periodicShardSyncManager
                .checkForShardSync(streamIdentifier, multiStreamLeases)
                .shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsNotInitiatedWhenHoleShifts() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize(MIN_HASH_KEY.toString(), "1"));
                add(deserialize("2", "3"));
                add(deserialize("4", "23"));
                add(deserialize("6", "23")); // Hole between 23 and 25
                add(deserialize("25", MAX_HASH_KEY.toString()));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    MultiStreamLease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases)
                        .shouldDoShardSync()));
        List<Lease> multiStreamLeases2 = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize(MIN_HASH_KEY.toString(), "1"));
                add(deserialize("2", "3")); // Hole between 3 and 5
                add(deserialize("5", "23"));
                add(deserialize("6", "23"));
                add(deserialize("24", MAX_HASH_KEY.toString()));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    MultiStreamLease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());
        // Resetting the holes
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases2)
                        .shouldDoShardSync()));
        Assert.assertTrue(periodicShardSyncManager
                .checkForShardSync(streamIdentifier, multiStreamLeases2)
                .shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsNotInitiatedWhenHoleShiftsMoreThanOnce() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize(MIN_HASH_KEY.toString(), "1"));
                add(deserialize("2", "3"));
                add(deserialize("4", "23"));
                add(deserialize("6", "23")); // Hole between 23 and 25
                add(deserialize("25", MAX_HASH_KEY.toString()));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    MultiStreamLease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases)
                        .shouldDoShardSync()));
        List<Lease> multiStreamLeases2 = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize(MIN_HASH_KEY.toString(), "1"));
                add(deserialize("2", "3")); // Hole between 3 and 5
                add(deserialize("5", "23"));
                add(deserialize("6", "23"));
                add(deserialize("24", MAX_HASH_KEY.toString()));
            }
        }.stream()
                .map(hashKeyRangeForLease -> {
                    MultiStreamLease lease = new MultiStreamLease();
                    lease.hashKeyRange(hashKeyRangeForLease);
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());
        // Resetting the holes
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases2)
                        .shouldDoShardSync()));
        // Resetting the holes
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases)
                        .shouldDoShardSync()));
        Assert.assertTrue(periodicShardSyncManager
                .checkForShardSync(streamIdentifier, multiStreamLeases)
                .shouldDoShardSync());
    }

    @Test
    public void testIfMissingHashRangeInformationIsFilledBeforeEvaluatingForShardSync() {
        ShardSyncTaskManager shardSyncTaskManager = mock(ShardSyncTaskManager.class);
        ShardDetector shardDetector = mock(ShardDetector.class);
        when(shardSyncTaskManagerProvider.apply(any())).thenReturn(shardSyncTaskManager);
        when(shardSyncTaskManager.shardDetector()).thenReturn(shardDetector);

        final int[] shardCounter = {0};
        List<HashKeyRangeForLease> hashKeyRangeForLeases = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize(MIN_HASH_KEY.toString(), "1"));
                add(deserialize("2", "3"));
                add(deserialize("4", "20"));
                add(deserialize("21", "23"));
                add(deserialize("24", MAX_HASH_KEY.toString()));
            }
        };

        List<Shard> kinesisShards = hashKeyRangeForLeases.stream()
                .map(hashKeyRangeForLease -> Shard.builder()
                        .shardId("shard-" + (++shardCounter[0]))
                        .hashKeyRange(HashKeyRange.builder()
                                .startingHashKey(hashKeyRangeForLease.serializedStartingHashKey())
                                .endingHashKey(hashKeyRangeForLease.serializedEndingHashKey())
                                .build())
                        .build())
                .collect(Collectors.toList());

        when(shardDetector.listShards()).thenReturn(kinesisShards);

        final int[] leaseCounter = {0};
        List<Lease> multiStreamLeases = hashKeyRangeForLeases.stream()
                .map(hashKeyRangeForLease -> {
                    MultiStreamLease lease = new MultiStreamLease();
                    lease.leaseKey(
                            MultiStreamLease.getLeaseKey(streamIdentifier.serialize(), "shard-" + (++leaseCounter[0])));
                    lease.shardId("shard-" + (leaseCounter[0]));
                    // Setting the hashrange only for last two leases
                    if (leaseCounter[0] >= 3) {
                        lease.hashKeyRange(hashKeyRangeForLease);
                    }
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());

        // Assert that shard sync should never trigger
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases)
                        .shouldDoShardSync()));
        Assert.assertFalse(periodicShardSyncManager
                .checkForShardSync(streamIdentifier, multiStreamLeases)
                .shouldDoShardSync());

        // Assert that all the leases now has hashRanges set.
        for (Lease lease : multiStreamLeases) {
            Assert.assertNotNull(lease.hashKeyRangeForLease());
        }
    }

    @Test
    public void testIfMissingHashRangeInformationIsFilledBeforeEvaluatingForShardSyncInHoleScenario() {
        ShardSyncTaskManager shardSyncTaskManager = mock(ShardSyncTaskManager.class);
        ShardDetector shardDetector = mock(ShardDetector.class);
        when(shardSyncTaskManagerProvider.apply(any())).thenReturn(shardSyncTaskManager);
        when(shardSyncTaskManager.shardDetector()).thenReturn(shardDetector);

        final int[] shardCounter = {0};
        List<HashKeyRangeForLease> hashKeyRangeForLeases = new ArrayList<HashKeyRangeForLease>() {
            {
                add(deserialize(MIN_HASH_KEY.toString(), "1"));
                add(deserialize("2", "3"));
                add(deserialize("5", "20")); // Hole between 3 and 5
                add(deserialize("21", "23"));
                add(deserialize("24", MAX_HASH_KEY.toString()));
            }
        };

        List<Shard> kinesisShards = hashKeyRangeForLeases.stream()
                .map(hashKeyRangeForLease -> Shard.builder()
                        .shardId("shard-" + (++shardCounter[0]))
                        .hashKeyRange(HashKeyRange.builder()
                                .startingHashKey(hashKeyRangeForLease.serializedStartingHashKey())
                                .endingHashKey(hashKeyRangeForLease.serializedEndingHashKey())
                                .build())
                        .build())
                .collect(Collectors.toList());

        when(shardDetector.listShards()).thenReturn(kinesisShards);

        final int[] leaseCounter = {0};
        List<Lease> multiStreamLeases = hashKeyRangeForLeases.stream()
                .map(hashKeyRangeForLease -> {
                    MultiStreamLease lease = new MultiStreamLease();
                    lease.leaseKey(
                            MultiStreamLease.getLeaseKey(streamIdentifier.serialize(), "shard-" + (++leaseCounter[0])));
                    lease.shardId("shard-" + (leaseCounter[0]));
                    // Setting the hashrange only for last two leases
                    if (leaseCounter[0] >= 3) {
                        lease.hashKeyRange(hashKeyRangeForLease);
                    }
                    lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    return lease;
                })
                .collect(Collectors.toList());

        // Assert that shard sync should never trigger
        IntStream.range(1, DEFAULT_CONSECUTIVE_HOLES_FOR_TRIGGERING_LEASE_RECOVERY)
                .forEach(i -> Assert.assertFalse(periodicShardSyncManager
                        .checkForShardSync(streamIdentifier, multiStreamLeases)
                        .shouldDoShardSync()));
        Assert.assertTrue(periodicShardSyncManager
                .checkForShardSync(streamIdentifier, multiStreamLeases)
                .shouldDoShardSync());

        // Assert that all the leases now has hashRanges set.
        for (Lease lease : multiStreamLeases) {
            Assert.assertNotNull(lease.hashKeyRangeForLease());
        }
    }

    @Test
    public void testFor1000DifferentValidSplitHierarchyTreeTheHashRangesAreAlwaysComplete() {
        for (int i = 0; i < 1000; i++) {
            int maxInitialLeaseCount = 100;
            List<Lease> leases = generateInitialLeases(maxInitialLeaseCount);
            reshard(leases, 5, ReshardType.SPLIT, maxInitialLeaseCount, false);
            Collections.shuffle(leases);
            //            System.out.println(
            //                    leases.stream().map(l -> l.checkpoint().sequenceNumber() + ":" +
            // l.hashKeyRangeForLease()).collect(Collectors.toList()));
            Assert.assertFalse(periodicShardSyncManager
                    .hasHoleInLeases(streamIdentifier, leases)
                    .isPresent());
        }
    }

    @Test
    public void testFor1000DifferentValidMergeHierarchyTreeTheHashRangesAreAlwaysComplete() {
        for (int i = 0; i < 1000; i++) {
            int maxInitialLeaseCount = 100;
            List<Lease> leases = generateInitialLeases(maxInitialLeaseCount);
            reshard(leases, 5, ReshardType.MERGE, maxInitialLeaseCount, false);
            Collections.shuffle(leases);
            Assert.assertFalse(periodicShardSyncManager
                    .hasHoleInLeases(streamIdentifier, leases)
                    .isPresent());
        }
    }

    @Test
    public void testFor1000DifferentValidReshardHierarchyTreeTheHashRangesAreAlwaysComplete() {
        for (int i = 0; i < 1000; i++) {
            int maxInitialLeaseCount = 100;
            List<Lease> leases = generateInitialLeases(maxInitialLeaseCount);
            reshard(leases, 5, ReshardType.ANY, maxInitialLeaseCount, false);
            Collections.shuffle(leases);
            Assert.assertFalse(periodicShardSyncManager
                    .hasHoleInLeases(streamIdentifier, leases)
                    .isPresent());
        }
    }

    @Test
    public void testFor1000DifferentValidMergeHierarchyTreeWithSomeInProgressParentsTheHashRangesAreAlwaysComplete() {
        for (int i = 0; i < 1000; i++) {
            int maxInitialLeaseCount = 100;
            List<Lease> leases = generateInitialLeases(maxInitialLeaseCount);
            reshard(leases, MAX_DEPTH_WITH_IN_PROGRESS_PARENTS, ReshardType.MERGE, maxInitialLeaseCount, true);
            Collections.shuffle(leases);
            Assert.assertFalse(periodicShardSyncManager
                    .hasHoleInLeases(streamIdentifier, leases)
                    .isPresent());
        }
    }

    @Test
    public void testFor1000DifferentValidReshardHierarchyTreeWithSomeInProgressParentsTheHashRangesAreAlwaysComplete() {
        for (int i = 0; i < 1000; i++) {
            int maxInitialLeaseCount = 100;
            List<Lease> leases = generateInitialLeases(maxInitialLeaseCount);
            reshard(leases, MAX_DEPTH_WITH_IN_PROGRESS_PARENTS, ReshardType.ANY, maxInitialLeaseCount, true);
            Collections.shuffle(leases);
            Assert.assertFalse(periodicShardSyncManager
                    .hasHoleInLeases(streamIdentifier, leases)
                    .isPresent());
        }
    }

    // ==================== Tests for memory leak fix: hashRangeHoleTrackerMap cleanup ====================

    @Test
    public void testCheckForShardSync_holeDetected_populatesHashRangeHoleTrackerMap() {
        final StreamIdentifier stream1 = StreamIdentifier.singleStreamInstance("stream1");
        final Map<StreamIdentifier, StreamConfig> realStreamConfigMap = new HashMap<>();
        realStreamConfigMap.put(stream1, new StreamConfig(stream1, null));

        final PeriodicShardSyncManager manager = new PeriodicShardSyncManager(
                "worker",
                leaseRefresher,
                realStreamConfigMap,
                shardSyncTaskManagerProvider,
                new HashMap<>(),
                mockScheduledExecutor,
                false,
                new NullMetricsFactory(),
                60000L,
                3,
                new AtomicBoolean(true));

        // Create leases with a hole in hash range coverage
        List<Lease> leasesWithHole = createLeasesWithHole();

        // Call checkForShardSync which should populate hashRangeHoleTrackerMap
        manager.checkForShardSync(stream1, leasesWithHole);

        // The hashRangeHoleTrackerMap should now contain an entry for stream1
        Assert.assertTrue(manager.getHashRangeHoleTrackerMap().containsKey(stream1));
    }

    @Test
    public void testCheckForShardSync_noHole_removesFromHashRangeHoleTrackerMap() {
        final StreamIdentifier stream1 = StreamIdentifier.singleStreamInstance("stream1");
        final Map<StreamIdentifier, StreamConfig> realStreamConfigMap = new HashMap<>();
        realStreamConfigMap.put(stream1, new StreamConfig(stream1, null));

        final PeriodicShardSyncManager manager = new PeriodicShardSyncManager(
                "worker",
                leaseRefresher,
                realStreamConfigMap,
                shardSyncTaskManagerProvider,
                new HashMap<>(),
                mockScheduledExecutor,
                false,
                new NullMetricsFactory(),
                60000L,
                3,
                new AtomicBoolean(true));

        // First call: create hole to populate tracker
        List<Lease> leasesWithHole = createLeasesWithHole();
        manager.checkForShardSync(stream1, leasesWithHole);
        Assert.assertTrue(manager.getHashRangeHoleTrackerMap().containsKey(stream1));

        // Second call: no hole, complete hash range
        List<Lease> completeLeases = createCompleteLeaseCoverage();
        manager.checkForShardSync(stream1, completeLeases);

        // hashRangeHoleTrackerMap should no longer contain stream1
        Assert.assertFalse(manager.getHashRangeHoleTrackerMap().containsKey(stream1));
    }

    @Test
    public void testRunShardSync_deletedStreamCleanedUpFromHashRangeHoleTrackerMap() throws Exception {
        final StreamIdentifier stream1 = StreamIdentifier.multiStreamInstance("123456789012:stream1:1");
        final StreamIdentifier stream2 = StreamIdentifier.multiStreamInstance("123456789012:stream2:2");
        final StreamIdentifier stream3 = StreamIdentifier.multiStreamInstance("123456789012:stream3:3");

        final Map<StreamIdentifier, StreamConfig> realStreamConfigMap = new HashMap<>();
        realStreamConfigMap.put(stream1, new StreamConfig(stream1, null));
        realStreamConfigMap.put(stream2, new StreamConfig(stream2, null));
        realStreamConfigMap.put(stream3, new StreamConfig(stream3, null));

        final ScheduledExecutorService localMockExecutor = mock(ScheduledExecutorService.class);
        final Runnable[] capturedRunnable = new Runnable[1];
        when(localMockExecutor.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    capturedRunnable[0] = invocation.getArgument(0);
                    return mock(ScheduledFuture.class);
                });

        final PeriodicShardSyncManager manager = new PeriodicShardSyncManager(
                "worker",
                leaseRefresher,
                realStreamConfigMap,
                shardSyncTaskManagerProvider,
                new HashMap<>(),
                localMockExecutor,
                true,
                new NullMetricsFactory(),
                60000L,
                3,
                new AtomicBoolean(true));

        // Populate hashRangeHoleTrackerMap for all 3 streams by calling checkForShardSync with holes
        List<Lease> leasesWithHole = createLeasesWithHole();
        manager.checkForShardSync(stream1, leasesWithHole);
        manager.checkForShardSync(stream2, leasesWithHole);
        manager.checkForShardSync(stream3, leasesWithHole);

        Assert.assertEquals(3, manager.getHashRangeHoleTrackerMap().size());

        // Now remove stream2 and stream3 from currentStreamConfigMap to simulate deletion
        realStreamConfigMap.remove(stream2);
        realStreamConfigMap.remove(stream3);

        // Setup mock to return complete leases for the remaining stream (no shard sync needed)
        List<Lease> completeMultiStreamLeases = createCompleteMultiStreamLeaseCoverage(stream1);
        when(leaseRefresher.listLeases()).thenReturn(completeMultiStreamLeases);

        // Start the manager to capture the runnable and then invoke it
        LeaderDecider localLeaderDecider = mock(LeaderDecider.class);
        when(localLeaderDecider.isLeader(any())).thenReturn(true);
        manager.start(localLeaderDecider);
        capturedRunnable[0].run();

        // After runShardSync, hashRangeHoleTrackerMap should not contain stream2 or stream3
        Assert.assertFalse(
                "Deleted stream2 should be removed from hashRangeHoleTrackerMap",
                manager.getHashRangeHoleTrackerMap().containsKey(stream2));
        Assert.assertFalse(
                "Deleted stream3 should be removed from hashRangeHoleTrackerMap",
                manager.getHashRangeHoleTrackerMap().containsKey(stream3));
        // stream1 was cleaned by checkForShardSync since it has complete coverage now
        Assert.assertFalse(
                "stream1 should be removed since leases have complete coverage",
                manager.getHashRangeHoleTrackerMap().containsKey(stream1));
    }

    @Test
    public void testRunShardSync_deletedStreamWithHoleTracker_retainAllCleansUp() throws Exception {
        final StreamIdentifier activeStream = StreamIdentifier.multiStreamInstance("123456789012:activeStream:1");
        final StreamIdentifier deletedStream = StreamIdentifier.multiStreamInstance("123456789012:deletedStream:2");

        final Map<StreamIdentifier, StreamConfig> realStreamConfigMap = new HashMap<>();
        realStreamConfigMap.put(activeStream, new StreamConfig(activeStream, null));
        realStreamConfigMap.put(deletedStream, new StreamConfig(deletedStream, null));

        final ScheduledExecutorService localMockExecutor = mock(ScheduledExecutorService.class);
        final Runnable[] capturedRunnable = new Runnable[1];
        when(localMockExecutor.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    capturedRunnable[0] = invocation.getArgument(0);
                    return mock(ScheduledFuture.class);
                });

        final PeriodicShardSyncManager manager = new PeriodicShardSyncManager(
                "worker",
                leaseRefresher,
                realStreamConfigMap,
                shardSyncTaskManagerProvider,
                new HashMap<>(),
                localMockExecutor,
                true,
                new NullMetricsFactory(),
                60000L,
                3,
                new AtomicBoolean(true));

        // Populate hashRangeHoleTrackerMap for both streams
        List<Lease> leasesWithHole = createLeasesWithHole();
        manager.checkForShardSync(activeStream, leasesWithHole);
        manager.checkForShardSync(deletedStream, leasesWithHole);

        Assert.assertEquals(2, manager.getHashRangeHoleTrackerMap().size());

        // Simulate stream deletion: remove deletedStream from config
        realStreamConfigMap.remove(deletedStream);

        // Setup mock: return leases with hole for active stream (so its tracker entry persists)
        List<Lease> multiStreamLeasesWithHole = createMultiStreamLeasesWithHole(activeStream);
        when(leaseRefresher.listLeases()).thenReturn(multiStreamLeasesWithHole);

        // Trigger runShardSync
        LeaderDecider localLeaderDecider = mock(LeaderDecider.class);
        when(localLeaderDecider.isLeader(any())).thenReturn(true);
        manager.start(localLeaderDecider);
        capturedRunnable[0].run();

        // Active stream should still have its tracker entry (hole still present)
        Assert.assertTrue(
                "Active stream with hole should retain its tracker entry",
                manager.getHashRangeHoleTrackerMap().containsKey(activeStream));
        // Deleted stream should be cleaned up
        Assert.assertFalse(
                "Deleted stream should be cleaned from hashRangeHoleTrackerMap",
                manager.getHashRangeHoleTrackerMap().containsKey(deletedStream));
        Assert.assertEquals(1, manager.getHashRangeHoleTrackerMap().size());
    }

    @Test
    public void testRunShardSync_noDeletedStreams_hashRangeHoleTrackerMapUnchanged() throws Exception {
        final StreamIdentifier stream1 = StreamIdentifier.multiStreamInstance("123456789012:stream1:1");
        final StreamIdentifier stream2 = StreamIdentifier.multiStreamInstance("123456789012:stream2:2");

        final Map<StreamIdentifier, StreamConfig> realStreamConfigMap = new HashMap<>();
        realStreamConfigMap.put(stream1, new StreamConfig(stream1, null));
        realStreamConfigMap.put(stream2, new StreamConfig(stream2, null));

        final ScheduledExecutorService localMockExecutor = mock(ScheduledExecutorService.class);
        final Runnable[] capturedRunnable = new Runnable[1];
        when(localMockExecutor.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    capturedRunnable[0] = invocation.getArgument(0);
                    return mock(ScheduledFuture.class);
                });

        final PeriodicShardSyncManager manager = new PeriodicShardSyncManager(
                "worker",
                leaseRefresher,
                realStreamConfigMap,
                shardSyncTaskManagerProvider,
                new HashMap<>(),
                localMockExecutor,
                true,
                new NullMetricsFactory(),
                60000L,
                3,
                new AtomicBoolean(true));

        // Populate hashRangeHoleTrackerMap for both streams
        List<Lease> leasesWithHole = createLeasesWithHole();
        manager.checkForShardSync(stream1, leasesWithHole);
        manager.checkForShardSync(stream2, leasesWithHole);

        Assert.assertEquals(2, manager.getHashRangeHoleTrackerMap().size());

        // Both streams remain in config - no deletions. Return leases with holes for both.
        List<Lease> allMultiStreamLeases = new ArrayList<>();
        allMultiStreamLeases.addAll(createMultiStreamLeasesWithHole(stream1));
        allMultiStreamLeases.addAll(createMultiStreamLeasesWithHole(stream2));
        when(leaseRefresher.listLeases()).thenReturn(allMultiStreamLeases);

        // Trigger runShardSync
        LeaderDecider localLeaderDecider = mock(LeaderDecider.class);
        when(localLeaderDecider.isLeader(any())).thenReturn(true);
        manager.start(localLeaderDecider);
        capturedRunnable[0].run();

        // Both entries should still be in the map since no streams were deleted
        Assert.assertTrue(manager.getHashRangeHoleTrackerMap().containsKey(stream1));
        Assert.assertTrue(manager.getHashRangeHoleTrackerMap().containsKey(stream2));
        Assert.assertEquals(2, manager.getHashRangeHoleTrackerMap().size());
    }

    @Test
    public void testRunShardSync_allStreamsDeleted_hashRangeHoleTrackerMapCleared() throws Exception {
        final StreamIdentifier stream1 = StreamIdentifier.multiStreamInstance("123456789012:stream1:1");
        final StreamIdentifier stream2 = StreamIdentifier.multiStreamInstance("123456789012:stream2:2");

        final Map<StreamIdentifier, StreamConfig> realStreamConfigMap = new HashMap<>();
        realStreamConfigMap.put(stream1, new StreamConfig(stream1, null));
        realStreamConfigMap.put(stream2, new StreamConfig(stream2, null));

        final ScheduledExecutorService localMockExecutor = mock(ScheduledExecutorService.class);
        final Runnable[] capturedRunnable = new Runnable[1];
        when(localMockExecutor.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    capturedRunnable[0] = invocation.getArgument(0);
                    return mock(ScheduledFuture.class);
                });

        final PeriodicShardSyncManager manager = new PeriodicShardSyncManager(
                "worker",
                leaseRefresher,
                realStreamConfigMap,
                shardSyncTaskManagerProvider,
                new HashMap<>(),
                localMockExecutor,
                true,
                new NullMetricsFactory(),
                60000L,
                3,
                new AtomicBoolean(true));

        // Populate hashRangeHoleTrackerMap
        List<Lease> leasesWithHole = createLeasesWithHole();
        manager.checkForShardSync(stream1, leasesWithHole);
        manager.checkForShardSync(stream2, leasesWithHole);

        Assert.assertEquals(2, manager.getHashRangeHoleTrackerMap().size());

        // Remove all streams from config
        realStreamConfigMap.clear();

        // Mock returns empty list since no leases
        when(leaseRefresher.listLeases()).thenReturn(Collections.emptyList());

        // Trigger runShardSync
        LeaderDecider localLeaderDecider = mock(LeaderDecider.class);
        when(localLeaderDecider.isLeader(any())).thenReturn(true);
        manager.start(localLeaderDecider);
        capturedRunnable[0].run();

        // All entries should be cleaned up
        Assert.assertTrue(
                "hashRangeHoleTrackerMap should be empty when all streams are deleted",
                manager.getHashRangeHoleTrackerMap().isEmpty());
    }

    /**
     * Creates leases with a hole in hash range coverage.
     * Covers [0, mid] but NOT (mid, MAX] — creating a detectable hole.
     */
    private List<Lease> createLeasesWithHole() {
        final BigInteger mid = MAX_HASH_KEY.divide(BigInteger.valueOf(2));
        List<Lease> leases = new ArrayList<>();

        Lease lease1 = new Lease();
        lease1.leaseKey("shard-001");
        lease1.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        lease1.hashKeyRange(new HashKeyRangeForLease(MIN_HASH_KEY, mid));
        leases.add(lease1);

        // Gap from mid+1 to MAX_HASH_KEY — no lease covers this range
        return leases;
    }

    /**
     * Creates leases that cover the complete hash range [0, MAX] with no holes.
     */
    private List<Lease> createCompleteLeaseCoverage() {
        final BigInteger mid = MAX_HASH_KEY.divide(BigInteger.valueOf(2));
        List<Lease> leases = new ArrayList<>();

        Lease lease1 = new Lease();
        lease1.leaseKey("shard-001");
        lease1.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        lease1.hashKeyRange(new HashKeyRangeForLease(MIN_HASH_KEY, mid));
        leases.add(lease1);

        Lease lease2 = new Lease();
        lease2.leaseKey("shard-002");
        lease2.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        lease2.hashKeyRange(new HashKeyRangeForLease(mid.add(BigInteger.ONE), MAX_HASH_KEY));
        leases.add(lease2);

        return leases;
    }

    /**
     * Creates MultiStreamLease objects with a hole for a given stream (for multi-stream mode runShardSync tests).
     */
    private List<Lease> createMultiStreamLeasesWithHole(StreamIdentifier stream) {
        final BigInteger mid = MAX_HASH_KEY.divide(BigInteger.valueOf(2));
        List<Lease> leases = new ArrayList<>();

        MultiStreamLease lease1 = new MultiStreamLease();
        lease1.leaseKey(MultiStreamLease.getLeaseKey(stream.serialize(), "shard-001"));
        lease1.shardId("shard-001");
        lease1.streamIdentifier(stream.serialize());
        lease1.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        lease1.hashKeyRange(new HashKeyRangeForLease(MIN_HASH_KEY, mid));
        leases.add(lease1);

        // Gap from mid+1 to MAX_HASH_KEY — no lease covers this range
        return leases;
    }

    /**
     * Creates MultiStreamLease objects with complete hash range coverage for a given stream.
     */
    private List<Lease> createCompleteMultiStreamLeaseCoverage(StreamIdentifier stream) {
        final BigInteger mid = MAX_HASH_KEY.divide(BigInteger.valueOf(2));
        List<Lease> leases = new ArrayList<>();

        MultiStreamLease lease1 = new MultiStreamLease();
        lease1.leaseKey(MultiStreamLease.getLeaseKey(stream.serialize(), "shard-001"));
        lease1.shardId("shard-001");
        lease1.streamIdentifier(stream.serialize());
        lease1.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        lease1.hashKeyRange(new HashKeyRangeForLease(MIN_HASH_KEY, mid));
        leases.add(lease1);

        MultiStreamLease lease2 = new MultiStreamLease();
        lease2.leaseKey(MultiStreamLease.getLeaseKey(stream.serialize(), "shard-002"));
        lease2.shardId("shard-002");
        lease2.streamIdentifier(stream.serialize());
        lease2.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        lease2.hashKeyRange(new HashKeyRangeForLease(mid.add(BigInteger.ONE), MAX_HASH_KEY));
        leases.add(lease2);

        return leases;
    }

    private List<Lease> generateInitialLeases(int initialShardCount) {
        long hashRangeInternalMax = 10000000;
        List<Lease> initialLeases = new ArrayList<>();
        long leaseStartKey = 0;
        for (int i = 1; i <= initialShardCount; i++) {
            final Lease lease = new Lease();
            long leaseEndKey;
            if (i != initialShardCount) {
                leaseEndKey = (hashRangeInternalMax / initialShardCount) * i;
                lease.hashKeyRange(HashKeyRangeForLease.deserialize(leaseStartKey + "", leaseEndKey + ""));
            } else {
                leaseEndKey = 0;
                lease.hashKeyRange(HashKeyRangeForLease.deserialize(leaseStartKey + "", MAX_HASH_KEY.toString()));
            }
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            lease.leaseKey("shard-" + i);
            initialLeases.add(lease);
            leaseStartKey = leaseEndKey + 1;
        }
        return initialLeases;
    }

    private void reshard(
            List<Lease> initialLeases,
            int depth,
            ReshardType reshardType,
            int leaseCounter,
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

    private int merge(List<Lease> initialLeases, int leaseCounter, boolean shouldKeepSomeParentsInProgress) {
        List<Lease> leasesEligibleForMerge = initialLeases.stream()
                .filter(l -> CollectionUtils.isNullOrEmpty(l.childShardIds()))
                .collect(Collectors.toList());
        //        System.out.println("Leases to merge : " + leasesEligibleForMerge);
        int leasesToMerge = (int) ((leasesEligibleForMerge.size() - 1) / 2.0 * Math.random());
        for (int i = 0; i < leasesToMerge; i += 2) {
            Lease parent1 = leasesEligibleForMerge.get(i);
            Lease parent2 = leasesEligibleForMerge.get(i + 1);
            if (parent2.hashKeyRangeForLease()
                    .startingHashKey()
                    .subtract(parent1.hashKeyRangeForLease().endingHashKey())
                    .equals(BigInteger.ONE)) {
                parent1.checkpoint(ExtendedSequenceNumber.SHARD_END);
                if (!shouldKeepSomeParentsInProgress || (shouldKeepSomeParentsInProgress && isOneFromDiceRoll())) {
                    //                    System.out.println("Deciding to keep parent in progress  : " + parent2);
                    parent2.checkpoint(ExtendedSequenceNumber.SHARD_END);
                }
                Lease child = new Lease();
                child.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                child.leaseKey("shard-" + (++leaseCounter));
                //                System.out.println("Parent " + parent1 + " and " + parent2 + " merges into " + child);
                child.hashKeyRange(new HashKeyRangeForLease(
                        parent1.hashKeyRangeForLease().startingHashKey(),
                        parent2.hashKeyRangeForLease().endingHashKey()));
                parent1.childShardIds(Collections.singletonList(child.leaseKey()));
                parent2.childShardIds(Collections.singletonList(child.leaseKey()));
                child.parentShardIds(Sets.newHashSet(parent1.leaseKey(), parent2.leaseKey()));

                initialLeases.add(child);
            }
        }
        return leaseCounter;
    }

    private int split(List<Lease> initialLeases, int leaseCounter) {
        List<Lease> leasesEligibleForSplit = initialLeases.stream()
                .filter(l -> CollectionUtils.isNullOrEmpty(l.childShardIds()))
                .collect(Collectors.toList());
        //        System.out.println("Leases to split : " + leasesEligibleForSplit);
        int leasesToSplit = (int) (leasesEligibleForSplit.size() * Math.random());
        for (int i = 0; i < leasesToSplit; i++) {
            Lease parent = leasesEligibleForSplit.get(i);
            parent.checkpoint(ExtendedSequenceNumber.SHARD_END);
            Lease child1 = new Lease();
            child1.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            child1.hashKeyRange(new HashKeyRangeForLease(
                    parent.hashKeyRangeForLease().startingHashKey(),
                    parent.hashKeyRangeForLease()
                            .startingHashKey()
                            .add(parent.hashKeyRangeForLease().endingHashKey())
                            .divide(new BigInteger("2"))));
            child1.leaseKey("shard-" + (++leaseCounter));
            Lease child2 = new Lease();
            child2.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            child2.hashKeyRange(new HashKeyRangeForLease(
                    parent.hashKeyRangeForLease()
                            .startingHashKey()
                            .add(parent.hashKeyRangeForLease().endingHashKey())
                            .divide(new BigInteger("2"))
                            .add(new BigInteger("1")),
                    parent.hashKeyRangeForLease().endingHashKey()));
            child2.leaseKey("shard-" + (++leaseCounter));

            child1.parentShardIds(Sets.newHashSet(parent.leaseKey()));
            child2.parentShardIds(Sets.newHashSet(parent.leaseKey()));
            parent.childShardIds(Lists.newArrayList(child1.leaseKey(), child2.leaseKey()));

            //            System.out.println("Parent " + parent + " splits into " + child1 + " and " + child2);

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
