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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

public class ParentsFirstShardPrioritizationUnitTest {
    private static final StreamIdentifier TEST_STREAM_IDENTIFIER = StreamIdentifier.singleStreamInstance("streamName");
    private static final InitialPositionInStreamExtended TEST_INITIAL_POSITION_IN_STREAM_EXTENDED =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final StreamConfig TEST_STREAM_CONFIG =
            new StreamConfig(TEST_STREAM_IDENTIFIER, TEST_INITIAL_POSITION_IN_STREAM_EXTENDED);

    @Test(expected = IllegalArgumentException.class)
    public void testMaxDepthNegativeShouldFail() {
        new ParentsFirstShardPrioritization(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxDepthZeroShouldFail() {
        new ParentsFirstShardPrioritization(0);
    }

    @Test
    public void testMaxDepthPositiveShouldNotFail() {
        new ParentsFirstShardPrioritization(1);
    }

    @Test
    public void testSorting() {
        Random random = new Random(987654);
        int numberOfShards = 7;

        List<String> shardIdsDependencies = new ArrayList<>();
        shardIdsDependencies.add("unknown");
        List<ShardInfo> original = new ArrayList<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            original.add(shardInfo(shardId, shardIdsDependencies));
            shardIdsDependencies.add(shardId);
        }

        ParentsFirstShardPrioritization ordering = new ParentsFirstShardPrioritization(Integer.MAX_VALUE);

        // shuffle original list as it is already ordered in right way
        Collections.shuffle(original, random);
        List<ShardInfo> ordered = ordering.prioritize(original);

        assertEquals(numberOfShards, ordered.size());
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            assertEquals(shardId, ordered.get(shardNumber).shardId());
        }
    }

    @Test
    public void testSortingAndFiltering() {
        Random random = new Random(45677);
        int numberOfShards = 10;

        List<String> shardIdsDependencies = new ArrayList<>();
        shardIdsDependencies.add("unknown");
        List<ShardInfo> original = new ArrayList<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            original.add(shardInfo(shardId, shardIdsDependencies));
            shardIdsDependencies.add(shardId);
        }

        int maxDepth = 3;
        ParentsFirstShardPrioritization ordering = new ParentsFirstShardPrioritization(maxDepth);

        // shuffle original list as it is already ordered in right way
        Collections.shuffle(original, random);
        List<ShardInfo> ordered = ordering.prioritize(original);
        // in this case every shard has its own level, so we don't expect to
        // have more shards than max depth
        assertEquals(maxDepth, ordered.size());

        for (int shardNumber = 0; shardNumber < maxDepth; shardNumber++) {
            String shardId = shardId(shardNumber);
            assertEquals(shardId, ordered.get(shardNumber).shardId());
        }
    }

    @Test
    public void testSimpleOrdering() {
        Random random = new Random(1234);
        int numberOfShards = 10;

        String parentId = "unknown";
        List<ShardInfo> original = new ArrayList<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            original.add(shardInfo(shardId, parentId));
            parentId = shardId;
        }

        ParentsFirstShardPrioritization ordering = new ParentsFirstShardPrioritization(Integer.MAX_VALUE);

        // shuffle original list as it is already ordered in right way
        Collections.shuffle(original, random);
        List<ShardInfo> ordered = ordering.prioritize(original);
        assertEquals(numberOfShards, ordered.size());
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            assertEquals(shardId, ordered.get(shardNumber).shardId());
        }
    }

    /**
     * This should be impossible as shards don't have circular dependencies,
     * but this code should handle it properly and fail
     */
    @Test
    public void testCircularDependencyBetweenShards() {
        Random random = new Random(13468798);
        int numberOfShards = 10;

        // shard-0 will point in middle shard (shard-5) in current test
        String parentId = shardId(numberOfShards / 2);
        List<ShardInfo> original = new ArrayList<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            original.add(shardInfo(shardId, parentId));
            parentId = shardId;
        }

        ParentsFirstShardPrioritization ordering = new ParentsFirstShardPrioritization(Integer.MAX_VALUE);

        // shuffle original list as it is already ordered in right way
        Collections.shuffle(original, random);
        try {
            ordering.prioritize(original);
            fail("Processing should fail in case we have circular dependency");
        } catch (IllegalArgumentException expected) {

        }
    }

    private String shardId(int shardNumber) {
        return "shardId-" + shardNumber;
    }

    /**
     * Builder class for ShardInfo.
     */
    static class ShardInfoBuilder {
        private String shardId;
        private String concurrencyToken;
        private List<String> parentShardIds = Collections.emptyList();
        private ExtendedSequenceNumber checkpoint = ExtendedSequenceNumber.LATEST;

        ShardInfoBuilder() {
        }

        ShardInfoBuilder withShardId(String shardId) {
            this.shardId = shardId;
            return this;
        }

        ShardInfoBuilder withConcurrencyToken(String concurrencyToken) {
            this.concurrencyToken = concurrencyToken;
            return this;
        }

        ShardInfoBuilder withParentShards(List<String> parentShardIds) {
            this.parentShardIds = parentShardIds;
            return this;
        }

        ShardInfoBuilder withCheckpoint(ExtendedSequenceNumber checkpoint) {
            this.checkpoint = checkpoint;
            return this;
        }

        ShardInfo build() {
            return new ShardInfo(shardId, concurrencyToken, parentShardIds, checkpoint, TEST_STREAM_CONFIG);
        }
    }

    private static ShardInfo shardInfo(String shardId, List<String> parentShardIds) {
        // copy into new list just in case ShardInfo will stop doing it
        List<String> newParentShardIds = new ArrayList<>(parentShardIds);
        return new ShardInfoBuilder()
                .withShardId(shardId)
                .withParentShards(newParentShardIds)
                .build();
    }

    private static ShardInfo shardInfo(String shardId, String... parentShardIds) {
        return new ShardInfoBuilder()
                .withShardId(shardId)
                .withParentShards(Arrays.asList(parentShardIds))
                .build();
    }
}
