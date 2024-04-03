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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

public class ShardInfoTest {
    private static final StreamIdentifier TEST_STREAM_IDENTIFIER = StreamIdentifier.singleStreamInstance("streamName");
    private static final InitialPositionInStreamExtended TEST_INITIAL_POSITION_IN_STREAM_EXTENDED =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final StreamConfig TEST_STREAM_CONFIG =
            new StreamConfig(TEST_STREAM_IDENTIFIER, TEST_INITIAL_POSITION_IN_STREAM_EXTENDED);
    private static final String CONCURRENCY_TOKEN = UUID.randomUUID().toString();
    private static final String SHARD_ID = "shardId-test";
    private final Set<String> parentShardIds = new HashSet<>();
    private ShardInfo testShardInfo;

    @Before
    public void setUpPacboyShardInfo() {
        // Add parent shard Ids
        parentShardIds.add("shard-1");
        parentShardIds.add("shard-2");

        testShardInfo = new ShardInfo(
                SHARD_ID, CONCURRENCY_TOKEN, parentShardIds, ExtendedSequenceNumber.LATEST, TEST_STREAM_CONFIG);
    }

    @Test
    public void testPacboyShardInfoEqualsWithSameArgs() {
        final ShardInfo equalShardInfo = new ShardInfo(
                SHARD_ID, CONCURRENCY_TOKEN, parentShardIds, ExtendedSequenceNumber.LATEST, TEST_STREAM_CONFIG);
        assertTrue("Equal should return true for arguments all the same", testShardInfo.equals(equalShardInfo));
    }

    @Test
    public void testPacboyShardInfoEqualsWithNull() {
        assertFalse("Equal should return false when object is null", testShardInfo.equals(null));
    }

    @Test
    public void testPacboyShardInfoEqualsForfToken() {
        final ShardInfo shardInfoWithDifferentConcurrencyToken = new ShardInfo(SHARD_ID, UUID.randomUUID().toString(),
                parentShardIds, ExtendedSequenceNumber.LATEST, TEST_STREAM_CONFIG);
        assertNotEquals("Equal should return false with different concurrency token",
                shardInfoWithDifferentConcurrencyToken, testShardInfo);

        final ShardInfo shardInfoWithNullConcurrencyToken =
                new ShardInfo(SHARD_ID, null, parentShardIds, ExtendedSequenceNumber.LATEST, TEST_STREAM_CONFIG);
        assertNotEquals("Equal should return false for null concurrency token",
                shardInfoWithNullConcurrencyToken, testShardInfo);
    }

    @Test
    public void testPacboyShardInfoEqualsForDifferentlyOrderedParentIds() {
        List<String> differentlyOrderedParentShardIds = new ArrayList<>();
        differentlyOrderedParentShardIds.add("shard-2");
        differentlyOrderedParentShardIds.add("shard-1");
        ShardInfo shardInfoWithDifferentlyOrderedParentShardIds =
                new ShardInfo(SHARD_ID, CONCURRENCY_TOKEN, differentlyOrderedParentShardIds,
                        ExtendedSequenceNumber.LATEST, TEST_STREAM_CONFIG);
        assertTrue("Equal should return true even with parent shard Ids reordered",
                shardInfoWithDifferentlyOrderedParentShardIds.equals(testShardInfo));
    }

    @Test
    public void testPacboyShardInfoEqualsForParentIds() {
        Set<String> diffParentIds = new HashSet<>();
        diffParentIds.add("shard-3");
        diffParentIds.add("shard-4");
        final ShardInfo shardInfoWithDifferentParents = new ShardInfo(
                SHARD_ID, CONCURRENCY_TOKEN, diffParentIds, ExtendedSequenceNumber.LATEST, TEST_STREAM_CONFIG);
        assertNotEquals("Equal should return false with different parent shard Ids",
                shardInfoWithDifferentParents, testShardInfo);
        final ShardInfo shardInfoWithNullParents = new ShardInfo(
                SHARD_ID, CONCURRENCY_TOKEN, null, ExtendedSequenceNumber.LATEST, TEST_STREAM_CONFIG);
        assertNotEquals("Equal should return false with null parent shard Ids",
                shardInfoWithNullParents, testShardInfo);
    }

    @Test
    public void testShardInfoCheckpointEqualsHashCode() {
        final ShardInfo baseInfo = new ShardInfo(
                SHARD_ID, CONCURRENCY_TOKEN, parentShardIds, ExtendedSequenceNumber.TRIM_HORIZON, TEST_STREAM_CONFIG);
        final ShardInfo differentCheckpoint = new ShardInfo(
                SHARD_ID, CONCURRENCY_TOKEN, parentShardIds, new ExtendedSequenceNumber("1234"), TEST_STREAM_CONFIG);
        final ShardInfo nullCheckpoint = new ShardInfo(
                SHARD_ID, CONCURRENCY_TOKEN, parentShardIds, null, TEST_STREAM_CONFIG);

        assertThat("Checkpoint should not be included in equality.", baseInfo.equals(differentCheckpoint), is(true));
        assertThat("Checkpoint should not be included in equality.", baseInfo.equals(nullCheckpoint), is(true));

        assertThat("Checkpoint should not be included in hash code.", baseInfo.hashCode(),
                equalTo(differentCheckpoint.hashCode()));
        assertThat("Checkpoint should not be included in hash code.", baseInfo.hashCode(),
                equalTo(nullCheckpoint.hashCode()));
    }

    @Test
    public void testPacboyShardInfoSameHashCode() {
        final ShardInfo equalShardInfo = new ShardInfo(
                SHARD_ID, CONCURRENCY_TOKEN, parentShardIds, ExtendedSequenceNumber.LATEST, TEST_STREAM_CONFIG);
        assertTrue("Shard info objects should have same hashCode for the same arguments",
                equalShardInfo.hashCode() == testShardInfo.hashCode());
    }
}
