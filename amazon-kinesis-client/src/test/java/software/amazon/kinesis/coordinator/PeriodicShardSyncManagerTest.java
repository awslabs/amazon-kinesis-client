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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.common.HashKeyRangeForLease;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.common.HashKeyRangeForLease.deserialize;
import static software.amazon.kinesis.coordinator.PeriodicShardSyncManager.CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY;
import static software.amazon.kinesis.coordinator.PeriodicShardSyncManager.MAX_HASH_KEY;
import static software.amazon.kinesis.coordinator.PeriodicShardSyncManager.MIN_HASH_KEY;

@RunWith(MockitoJUnitRunner.class)

public class PeriodicShardSyncManagerTest {

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

    @Before
    public void setup() {
        streamIdentifier = StreamIdentifier.multiStreamInstance("123:stream:456");
        periodicShardSyncManager = new PeriodicShardSyncManager("worker", leaderDecider, leaseRefresher, currentStreamConfigMap,
                shardSyncTaskManagerProvider, true);
    }

    @Test
    public void testIfHashRangesAreNotMergedWhenNoOverlappingIntervalsGiven() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> sortAndMergedHashRanges = PeriodicShardSyncManager
                .sortAndMergeOverlappingHashRanges(hashRanges);
        Assert.assertEquals(hashRanges, sortAndMergedHashRanges);
    }

    @Test
    public void testIfHashRangesAreSortedWhenNoOverlappingIntervalsGiven() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("2", "3"));
            add(deserialize("0", "1"));
            add(deserialize("24", "30"));
            add(deserialize("4", "23"));
        }};
        List<HashKeyRangeForLease> hashRangesCopy = new ArrayList<>();
        hashRangesCopy.addAll(hashRanges);
        List<HashKeyRangeForLease> sortAndMergedHashRanges = PeriodicShardSyncManager
                .sortAndMergeOverlappingHashRanges(hashRangesCopy);
        Assert.assertEquals(hashRangesCopy, sortAndMergedHashRanges);
        Assert.assertNotEquals(hashRanges, sortAndMergedHashRanges);
    }

    @Test
    public void testIfHashRangesAreMergedWhenOverlappingIntervalsGivenCase1() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> expectedHashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> sortAndMergedHashRanges = PeriodicShardSyncManager
                .sortAndMergeOverlappingHashRanges(hashRanges);
        Assert.assertEquals(expectedHashRanges, sortAndMergedHashRanges);
    }

    @Test
    public void testIfHashRangesAreMergedWhenOverlappingIntervalsGivenCase2() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "5"));
            add(deserialize("4", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> expectedHashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> sortAndMergedHashRanges = PeriodicShardSyncManager
                .sortAndMergeOverlappingHashRanges(hashRanges);
        Assert.assertEquals(expectedHashRanges, sortAndMergedHashRanges);
    }

    @Test
    public void testIfHashRangesAreMergedWhenOverlappingIntervalsGivenCase3() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("4", "5"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> expectedHashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("24", "30"));
        }};
        List<HashKeyRangeForLease> sortAndMergedHashRanges = PeriodicShardSyncManager
                .sortAndMergeOverlappingHashRanges(hashRanges);
        Assert.assertEquals(expectedHashRanges, sortAndMergedHashRanges);
    }

    @Test
    public void testForFailureWhenHashRangesAreIncomplete() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("25", "30")); // Missing interval here
        }};
        Assert.assertTrue(PeriodicShardSyncManager
                .checkForHoleInHashKeyRanges(streamIdentifier, hashRanges, BigInteger.ZERO, BigInteger.valueOf(30)).isPresent());
    }

    @Test
    public void testForSuccessWhenHashRangesAreComplete() {
        List<HashKeyRangeForLease> hashRanges = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize("0", "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("24", "30"));
        }};
        Assert.assertFalse(PeriodicShardSyncManager
                .checkForHoleInHashKeyRanges(streamIdentifier, hashRanges, BigInteger.ZERO, BigInteger.valueOf(30)).isPresent());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenNoLeasesArePassed() {
        Assert.assertTrue(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, null));
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenEmptyLeasesArePassed() {
        Assert.assertTrue(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, new ArrayList<>()));
    }

    @Test
    public void testIfShardSyncIsNotInitiatedWhenConfidenceFactorIsNotReached() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23")); // Hole between 23 and 25
            add(deserialize("25", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            MultiStreamLease lease = new MultiStreamLease();
            lease.hashKeyRange(hashKeyRangeForLease);
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases)));
    }

    @Test
    public void testIfShardSyncIsNotInitiatedWhenConfidenceFactorIsReached() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23")); // Hole between 23 and 25
            add(deserialize("25", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            MultiStreamLease lease = new MultiStreamLease();
            lease.hashKeyRange(hashKeyRangeForLease);
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases)));
        Assert.assertTrue(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases));
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenHoleIsDueToShardEnd() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23")); // introducing hole here through SHARD_END checkpoint
            add(deserialize("6", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            MultiStreamLease lease = new MultiStreamLease();
            lease.hashKeyRange(hashKeyRangeForLease);
            if(lease.hashKeyRangeForLease().startingHashKey().toString().equals("4")) {
                lease.checkpoint(ExtendedSequenceNumber.SHARD_END);
            } else {
                lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            }
            return lease;
        }).collect(Collectors.toList());
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases)));
        Assert.assertTrue(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases));
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenNoLeasesAreUsedDueToShardEnd() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            MultiStreamLease lease = new MultiStreamLease();
            lease.hashKeyRange(hashKeyRangeForLease);
            lease.checkpoint(ExtendedSequenceNumber.SHARD_END);
            return lease;
        }).collect(Collectors.toList());
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases)));
        Assert.assertTrue(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases));
    }

    @Test
    public void testIfShardSyncIsNotInitiatedWhenHoleShifts() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23")); // Hole between 23 and 25
            add(deserialize("25", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            MultiStreamLease lease = new MultiStreamLease();
            lease.hashKeyRange(hashKeyRangeForLease);
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases)));
        List<Lease> multiStreamLeases2 = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3")); // Hole between 3 and 5
            add(deserialize("5", "23"));
            add(deserialize("6", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            MultiStreamLease lease = new MultiStreamLease();
            lease.hashKeyRange(hashKeyRangeForLease);
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());
        // Resetting the holes
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases2)));
        Assert.assertTrue(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases2));
    }

    @Test
    public void testIfShardSyncIsNotInitiatedWhenHoleShiftsMoreThanOnce() {
        List<Lease> multiStreamLeases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "23"));
            add(deserialize("6", "23")); // Hole between 23 and 25
            add(deserialize("25", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            MultiStreamLease lease = new MultiStreamLease();
            lease.hashKeyRange(hashKeyRangeForLease);
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases)));
        List<Lease> multiStreamLeases2 = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3")); // Hole between 3 and 5
            add(deserialize("5", "23"));
            add(deserialize("6", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }}.stream().map(hashKeyRangeForLease -> {
            MultiStreamLease lease = new MultiStreamLease();
            lease.hashKeyRange(hashKeyRangeForLease);
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());
        // Resetting the holes
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases2)));
        // Resetting the holes
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases)));
        Assert.assertTrue(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases));
    }

    @Test
    public void testIfMissingHashRangeInformationIsFilledBeforeEvaluatingForShardSync() {
        ShardSyncTaskManager shardSyncTaskManager = mock(ShardSyncTaskManager.class);
        ShardDetector shardDetector = mock(ShardDetector.class);
        when(shardSyncTaskManagerProvider.apply(any())).thenReturn(shardSyncTaskManager);
        when(shardSyncTaskManager.shardDetector()).thenReturn(shardDetector);

        final int[] shardCounter = { 0 };
        List<HashKeyRangeForLease> hashKeyRangeForLeases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("4", "20"));
            add(deserialize("21", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }};

        List<Shard> kinesisShards = hashKeyRangeForLeases.stream()
                .map(hashKeyRangeForLease -> Shard.builder().shardId("shard-" + (++shardCounter[0])).hashKeyRange(
                        HashKeyRange.builder().startingHashKey(hashKeyRangeForLease.serializedStartingHashKey())
                                .endingHashKey(hashKeyRangeForLease.serializedEndingHashKey()).build()).build())
                .collect(Collectors.toList());

        when(shardDetector.listShards()).thenReturn(kinesisShards);

        final int[] leaseCounter = { 0 };
        List<Lease> multiStreamLeases = hashKeyRangeForLeases.stream().map(hashKeyRangeForLease -> {
            MultiStreamLease lease = new MultiStreamLease();
            lease.leaseKey(MultiStreamLease.getLeaseKey(streamIdentifier.serialize(), "shard-"+(++leaseCounter[0])));
            lease.shardId("shard-"+(leaseCounter[0]));
            // Setting the hashrange only for last two leases
            if(leaseCounter[0] >= 3) {
                lease.hashKeyRange(hashKeyRangeForLease);
            }
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());

        // Assert that shard sync should never trigger
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases)));
        Assert.assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases));

        // Assert that all the leases now has hashRanges set.
        for(Lease lease : multiStreamLeases) {
            Assert.assertNotNull(lease.hashKeyRangeForLease());
        }
    }

    @Test
    public void testIfMissingHashRangeInformationIsFilledBeforeEvaluatingForShardSyncInHoleScenario() {
        ShardSyncTaskManager shardSyncTaskManager = mock(ShardSyncTaskManager.class);
        ShardDetector shardDetector = mock(ShardDetector.class);
        when(shardSyncTaskManagerProvider.apply(any())).thenReturn(shardSyncTaskManager);
        when(shardSyncTaskManager.shardDetector()).thenReturn(shardDetector);

        final int[] shardCounter = { 0 };
        List<HashKeyRangeForLease> hashKeyRangeForLeases = new ArrayList<HashKeyRangeForLease>() {{
            add(deserialize(MIN_HASH_KEY.toString(), "1"));
            add(deserialize("2", "3"));
            add(deserialize("5", "20")); // Hole between 3 and 5
            add(deserialize("21", "23"));
            add(deserialize("24", MAX_HASH_KEY.toString()));
        }};

        List<Shard> kinesisShards = hashKeyRangeForLeases.stream()
                .map(hashKeyRangeForLease -> Shard.builder().shardId("shard-" + (++shardCounter[0])).hashKeyRange(
                        HashKeyRange.builder().startingHashKey(hashKeyRangeForLease.serializedStartingHashKey())
                                .endingHashKey(hashKeyRangeForLease.serializedEndingHashKey()).build()).build())
                .collect(Collectors.toList());

        when(shardDetector.listShards()).thenReturn(kinesisShards);

        final int[] leaseCounter = { 0 };
        List<Lease> multiStreamLeases = hashKeyRangeForLeases.stream().map(hashKeyRangeForLease -> {
            MultiStreamLease lease = new MultiStreamLease();
            lease.leaseKey(MultiStreamLease.getLeaseKey(streamIdentifier.serialize(), "shard-"+(++leaseCounter[0])));
            lease.shardId("shard-"+(leaseCounter[0]));
            // Setting the hashrange only for last two leases
            if(leaseCounter[0] >= 3) {
                lease.hashKeyRange(hashKeyRangeForLease);
            }
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            return lease;
        }).collect(Collectors.toList());

        // Assert that shard sync should never trigger
        IntStream.range(1, CONSECUTIVE_HOLES_FOR_TRIGGERING_RECOVERY).forEach(i -> Assert
                .assertFalse(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases)));
        Assert.assertTrue(periodicShardSyncManager.shouldDoShardSync(streamIdentifier, multiStreamLeases));

        // Assert that all the leases now has hashRanges set.
        for(Lease lease : multiStreamLeases) {
            Assert.assertNotNull(lease.hashKeyRangeForLease());
        }
    }

}
