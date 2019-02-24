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

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseBuilder;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ShardReprocessingDetectionUtilTest {

    @Test
    public void testEmptyCurrentLeases() {
        // Set up.
        String shardId1 = "shard-1";
        String shardId2 = "shard-2";
        KinesisClientLease lease1 = new KinesisClientLeaseBuilder().withLeaseKey(shardId1)
                .withCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON).build();
        KinesisClientLease lease2 = new KinesisClientLeaseBuilder().withLeaseKey(shardId2)
                .withCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON)
                .withParentShardIds(Sets.newHashSet(shardId1)).build();
        List<KinesisClientLease> newLeasesToCreate = Arrays.asList(lease1, lease2);

        Map<String, Set<String>> shardIdToChildShardIdsDescribeStream = new HashMap<>();
        shardIdToChildShardIdsDescribeStream.put(shardId1, Sets.newHashSet(shardId2));

        // Test.
        List<KinesisClientLease> finalLeases = new ShardReprocessingDetectionUtil(newLeasesToCreate,
                Arrays.asList(), shardIdToChildShardIdsDescribeStream).removeShardsWhoseDescendantsExist();

        // Verify.
        Assert.assertEquals(finalLeases, newLeasesToCreate);
    }

    @Test
    public void tesEmptyNewLeasesToCreate() {
        // Set up.
        String shardId1 = "shard-1";
        String shardId2 = "shard-2";
        KinesisClientLease lease1 = new KinesisClientLeaseBuilder().withLeaseKey(shardId1)
                .withCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON).build();
        KinesisClientLease lease2 = new KinesisClientLeaseBuilder().withLeaseKey(shardId2)
                .withCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON)
                .withParentShardIds(Sets.newHashSet(shardId1)).build();
        List<KinesisClientLease> currentLeases = Arrays.asList(lease1, lease2);

        Map<String, Set<String>> shardIdToChildShardIdsDescribeStream = new HashMap<>();
        shardIdToChildShardIdsDescribeStream.put(shardId1, Sets.newHashSet(shardId2));

        // Test.
        List<KinesisClientLease> finalLeases = new ShardReprocessingDetectionUtil(Arrays.asList(),
                currentLeases, shardIdToChildShardIdsDescribeStream).removeShardsWhoseDescendantsExist();

        // Verify.
        Assert.assertTrue(finalLeases.isEmpty());
    }

    /**
     *
     *   1
     *  / \
     * 2  3
     *
     *  DescribeStream returned empty result.
     *  Leases for 1, 2, 3 are new leases to be create.
     *
     *  1. No currentLeases: All leases are created.
     *  2. Lease for descendant 2 checkpointed at Trim_Horizon, Latest, or At_timestamp:
     *     All leases are created.
     *  3. Lease for descendant 2 checkpointed at a sequence number:
     *     Only 1 and 2 are created.
     */
    @Test
    public void testEmptyDescribeStream() {
        // Set up.
        String shardId1 = "shard-1";
        String shardId2 = "shard-2";
        String shardId3 = "shard-3";

        KinesisClientLease lease1 = new KinesisClientLeaseBuilder()
                .withLeaseKey(shardId1).build();
        KinesisClientLease lease2 = new KinesisClientLeaseBuilder()
                .withLeaseKey(shardId2)
                .withParentShardIds(Sets.newHashSet(shardId1)).build();
        KinesisClientLease lease3 = new KinesisClientLeaseBuilder()
                .withLeaseKey(shardId3)
                .withParentShardIds(Sets.newHashSet(shardId1)).build();
        List<KinesisClientLease> newLeasesToCreate = Arrays.asList(lease1, lease2, lease3);

        emptyCurrentLeases(newLeasesToCreate);

        lease2.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        descendantUnprocessed(newLeasesToCreate, Collections.emptyMap(), lease2);

        lease2.setCheckpoint(ExtendedSequenceNumber.LATEST);
        descendantUnprocessed(newLeasesToCreate, Collections.emptyMap(), lease2);

        lease2.setCheckpoint(ExtendedSequenceNumber.AT_TIMESTAMP);
        descendantUnprocessed(newLeasesToCreate, Collections.emptyMap(), lease2);

        lease2.setCheckpoint(new ExtendedSequenceNumber("1234567890"));
        descendantBeganProcessing(newLeasesToCreate, Collections.emptyMap(), lease2, Collections.emptyList());
    }


    /**
     *   0
     *   |
     *   1
     *  / \
     * 2  3
     *   / \
     *  4   5
     *       \
     *        6
     *
     *  The above shard graph is obtained from describeStream.
     *  Leases for 0, 1, 2, 3, 4, 5, 6 are new leases to be created.
     *  Lease for 4 exists.
     *
     *  1. Lease for descendant 4 at Trim_Horizon, Latest or At_Timestamp:
     *     All leases are created.
     *  2. Lease for descendant 4 checkpointed at a sequence number:
     *     2, 5, 6 are created.
     */
    @Test
    public void testDescendantExists() {
        // Set up.
        String shardId0 = "shard-0";
        String shardId1 = "shard-1";
        String shardId2 = "shard-2";
        String shardId3 = "shard-3";
        String shardId4 = "shard-4";
        String shardId5 = "shard-5";
        String shardId6 = "shard-6";

        KinesisClientLease lease0 = new KinesisClientLeaseBuilder().withLeaseKey(shardId0)
                .build();
        KinesisClientLease lease1 = new KinesisClientLeaseBuilder().withLeaseKey(shardId1)
                .withParentShardIds(Sets.newHashSet(shardId0)).build();
        KinesisClientLease lease2 = new KinesisClientLeaseBuilder().withLeaseKey(shardId2)
                .withParentShardIds(Sets.newHashSet(shardId1)).build();
        KinesisClientLease lease3 = new KinesisClientLeaseBuilder().withLeaseKey(shardId3)
                .withParentShardIds(Sets.newHashSet(shardId1)).build();
        KinesisClientLease lease4 = new KinesisClientLeaseBuilder().withLeaseKey(shardId4)
                .withParentShardIds(Sets.newHashSet(shardId3)).build();
        KinesisClientLease lease5 = new KinesisClientLeaseBuilder().withLeaseKey(shardId5)
                .withParentShardIds(Sets.newHashSet(shardId3)).build();
        KinesisClientLease lease6 = new KinesisClientLeaseBuilder().withLeaseKey(shardId6)
                .withParentShardIds(Sets.newHashSet(shardId5)).build();

        List<KinesisClientLease> newLeasesToCreate = Arrays.asList(lease0, lease1, lease2,
                lease3, lease4, lease5, lease6);

        Map<String, Set<String>> shardIdToChildShardIdsDescribeStream = new HashMap<>();
        shardIdToChildShardIdsDescribeStream.put(shardId0, Sets.newHashSet(shardId1));
        shardIdToChildShardIdsDescribeStream.put(shardId1, Sets.newHashSet(shardId2, shardId3));
        shardIdToChildShardIdsDescribeStream.put(shardId3, Sets.newHashSet(shardId4, shardId5));
        shardIdToChildShardIdsDescribeStream.put(shardId5, Sets.newHashSet(shardId6));

        lease4.setCheckpoint(ExtendedSequenceNumber.LATEST);
        descendantUnprocessed(newLeasesToCreate, shardIdToChildShardIdsDescribeStream, lease4);

        lease4.setCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        descendantUnprocessed(newLeasesToCreate, shardIdToChildShardIdsDescribeStream, lease4);

        lease4.setCheckpoint(ExtendedSequenceNumber.AT_TIMESTAMP);
        descendantUnprocessed(newLeasesToCreate, shardIdToChildShardIdsDescribeStream, lease4);

        lease4.setCheckpoint(new ExtendedSequenceNumber("1234567890"));
        descendantBeganProcessing(newLeasesToCreate, shardIdToChildShardIdsDescribeStream, lease4,
                Arrays.asList(lease0, lease1, lease3));
    }

    private void descendantUnprocessed(List<KinesisClientLease> newLeasesToCreate,
            Map<String, Set<String>> shardIdToChildShardIdsDescribeStream,
            KinesisClientLease existingDescendant) {
        List<KinesisClientLease> finalLeases = new ShardReprocessingDetectionUtil(newLeasesToCreate,
                Arrays.asList(existingDescendant), shardIdToChildShardIdsDescribeStream).removeShardsWhoseDescendantsExist();

        Assert.assertEquals(finalLeases, newLeasesToCreate);
    }

    private void descendantBeganProcessing(List<KinesisClientLease> newLeasesToCreate,
            Map<String, Set<String>> shardIdToChildShardIdsDescribeStream,
            KinesisClientLease existingDescendant,
            List<KinesisClientLease> ancestorsOfDescendant) {
        List<KinesisClientLease> finalLeases = new ShardReprocessingDetectionUtil(newLeasesToCreate,
                Arrays.asList(existingDescendant), shardIdToChildShardIdsDescribeStream).removeShardsWhoseDescendantsExist();
        List<KinesisClientLease> expectedLeases = newLeasesToCreate.stream()
                .filter(lease -> !lease.equals(existingDescendant) && !ancestorsOfDescendant.contains(lease))
                .collect(Collectors.toList());

        Assert.assertEquals(finalLeases, expectedLeases);
    }

    private void emptyCurrentLeases(List<KinesisClientLease> newLeasesToCreate) {
        List<KinesisClientLease> finalLeases = new ShardReprocessingDetectionUtil(newLeasesToCreate,
                Collections.emptyList(), Collections.emptyMap()).removeShardsWhoseDescendantsExist();

        Assert.assertEquals(newLeasesToCreate, finalLeases);
    }
}
