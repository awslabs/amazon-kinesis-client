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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

public class ShardInfoTest {
    private static final String CONCURRENCY_TOKEN = UUID.randomUUID().toString();
    private static final String SHARD_ID = "shardId-test";
    private final Set<String> parentShardIds = new HashSet<>();
    private ShardInfo testShardInfo;

    @Before
    public void setUpPacboyShardInfo() {
        // Add parent shard Ids
        parentShardIds.add("shard-1");
        parentShardIds.add("shard-2");

        testShardInfo = new ShardInfo(SHARD_ID, CONCURRENCY_TOKEN, parentShardIds, ExtendedSequenceNumber.LATEST);
    }

    @Test
    public void testPacboyShardInfoEqualsWithSameArgs() {
        ShardInfo equalShardInfo = new ShardInfo(SHARD_ID, CONCURRENCY_TOKEN, parentShardIds, ExtendedSequenceNumber.LATEST);
        Assert.assertTrue("Equal should return true for arguments all the same", testShardInfo.equals(equalShardInfo));
    }

    @Test
    public void testPacboyShardInfoEqualsWithNull() {
        Assert.assertFalse("Equal should return false when object is null", testShardInfo.equals(null));
    }

    @Test
    public void testPacboyShardInfoEqualsForShardId() {
        ShardInfo diffShardInfo = new ShardInfo("shardId-diff", CONCURRENCY_TOKEN, parentShardIds, ExtendedSequenceNumber.LATEST);
        Assert.assertFalse("Equal should return false with different shard id", diffShardInfo.equals(testShardInfo));
        diffShardInfo = new ShardInfo(null, CONCURRENCY_TOKEN, parentShardIds, ExtendedSequenceNumber.LATEST);
        Assert.assertFalse("Equal should return false with null shard id", diffShardInfo.equals(testShardInfo));
    }

    @Test
    public void testPacboyShardInfoEqualsForfToken() {
        ShardInfo diffShardInfo = new ShardInfo(SHARD_ID, UUID.randomUUID().toString(), parentShardIds, ExtendedSequenceNumber.LATEST);
        Assert.assertFalse("Equal should return false with different concurrency token",
                diffShardInfo.equals(testShardInfo));
        diffShardInfo = new ShardInfo(SHARD_ID, null, parentShardIds, ExtendedSequenceNumber.LATEST);
        Assert.assertFalse("Equal should return false for null concurrency token", diffShardInfo.equals(testShardInfo));
    }

    @Test
    public void testPacboyShardInfoEqualsForDifferentlyOrderedParentIds() {
        List<String> differentlyOrderedParentShardIds = new ArrayList<>();
        differentlyOrderedParentShardIds.add("shard-2");
        differentlyOrderedParentShardIds.add("shard-1");
        ShardInfo shardInfoWithDifferentlyOrderedParentShardIds =
                new ShardInfo(SHARD_ID, CONCURRENCY_TOKEN, differentlyOrderedParentShardIds, ExtendedSequenceNumber.LATEST);
        Assert.assertTrue("Equal should return true even with parent shard Ids reordered",
                shardInfoWithDifferentlyOrderedParentShardIds.equals(testShardInfo));
    }

    @Test
    public void testPacboyShardInfoEqualsForParentIds() {
        Set<String> diffParentIds = new HashSet<>();
        diffParentIds.add("shard-3");
        diffParentIds.add("shard-4");
        ShardInfo diffShardInfo = new ShardInfo(SHARD_ID, CONCURRENCY_TOKEN, diffParentIds, ExtendedSequenceNumber.LATEST);
        Assert.assertFalse("Equal should return false with different parent shard Ids",
                diffShardInfo.equals(testShardInfo));
        diffShardInfo = new ShardInfo(SHARD_ID, CONCURRENCY_TOKEN, null, ExtendedSequenceNumber.LATEST);
        Assert.assertFalse("Equal should return false with null parent shard Ids", diffShardInfo.equals(testShardInfo));
    }

    @Test
    public void testShardInfoCheckpointEqualsHashCode() {
        ShardInfo baseInfo = new ShardInfo(SHARD_ID, CONCURRENCY_TOKEN, parentShardIds,
                ExtendedSequenceNumber.TRIM_HORIZON);
        ShardInfo differentCheckpoint = new ShardInfo(SHARD_ID, CONCURRENCY_TOKEN, parentShardIds,
                new ExtendedSequenceNumber("1234"));
        ShardInfo nullCheckpoint = new ShardInfo(SHARD_ID, CONCURRENCY_TOKEN, parentShardIds, null);

        assertThat("Checkpoint should not be included in equality.", baseInfo.equals(differentCheckpoint), is(true));
        assertThat("Checkpoint should not be included in equality.", baseInfo.equals(nullCheckpoint), is(true));

        assertThat("Checkpoint should not be included in hash code.", baseInfo.hashCode(),
                equalTo(differentCheckpoint.hashCode()));
        assertThat("Checkpoint should not be included in hash code.", baseInfo.hashCode(),
                equalTo(nullCheckpoint.hashCode()));
    }

    @Test
    public void testPacboyShardInfoSameHashCode() {
        ShardInfo equalShardInfo = new ShardInfo(SHARD_ID, CONCURRENCY_TOKEN, parentShardIds, ExtendedSequenceNumber.LATEST);
        Assert.assertTrue("Shard info objects should have same hashCode for the same arguments",
                equalShardInfo.hashCode() == testShardInfo.hashCode());
    }
}
