/*
 * Copyright 2026 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.coordinator.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber.AT_TIMESTAMP;
import static software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber.SHARD_END;
import static software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber.TRIM_HORIZON;

/**
 * Tests {@link ShardTreeSorter} on hand-written and generated shard trees. Only the sort order is tested here, not how
 * the LAM assigns leases in that order.
 *
 * <h2>What the trees model, and what they leave out</h2>
 * <ul>
 *   <li><b>Topology only.</b> The sorter reads each lease's key and parent shard IDs, nothing else. So a tree here is
 *       just which shards came from which: a split child has one parent, a merge child has two. Hash key ranges
 *       aren't modeled, because the sorter never looks at them.</li>
 *   <li><b>Left-to-right order instead of hash key ranges.</b> The one thing ranges decide that matters here is which
 *       shards can merge: only neighbors can. The builder keeps the open shards in a list, and list order stands in
 *       for hash key order, so "adjacent in the list" means "adjacent in the hash key space".</li>
 *   <li><b>Checkpoints are for realism, not for the sort.</b> The sorter ignores checkpoints. The trees still get
 *       checkpoints a real lease table could have, so they read like real cases and so
 *       {@link #assertValidShardTree} can check them.</li>
 * </ul>
 *
 * <h2>How generated trees are built ({@link ShardTree})</h2>
 * A tree is built by replaying reshard steps on a stream. The builder starts with some original shards (step 0) and
 * tracks the open shards: those that haven't been split or merged yet. Each step gives the stream's new total open
 * shard count:
 * <ul>
 *   <li><b>More open shards: splits.</b> That many open shards split, spread evenly across the list. Each split
 *       replaces one shard with two adjacent children, so one step can at most double the count.</li>
 *   <li><b>Fewer open shards: merges.</b> That many disjoint pairs of neighbors merge, spread evenly. Each merge
 *       replaces two neighbors with one child, so one step can at most halve the count. Pairs start at odd
 *       positions when possible, so after a step where every shard split, each merge joins pieces of two different
 *       parents. That's the shape of a marginal reshard, where the new boundaries don't line up with the old
 *       ones.</li>
 *   <li><b>Untouched shards stay open</b> at their current depth, while shards that changed get children one level
 *       deeper. That's what makes a multi-step history uneven.</li>
 *   <li><b>Each step is all splits or all merges.</b> A real UpdateShardCount to an uneven count reaches its exactly
 *       even layout by splitting first and then merging, so here that reshard is two steps: one up, one down. A
 *       merge of three pieces into one is modeled across two merge steps.</li>
 * </ul>
 *
 * <h2>The trees</h2>
 * <ul>
 *   <li><b>Marginal scaling tree</b> (20 to 40 to 21 open shards): every original shard splits, then neighboring
 *       pieces across the split boundaries merge. The tree fans out to about twice the original width before
 *       narrowing again, like a small scale-up or scale-down.</li>
 *   <li><b>Large scale-up tree</b> (64 doubled six times, to 4,096; 8,128 leases): every shard splits at every step.
 *       The tree widens at every level.</li>
 *   <li><b>Random trees</b> (50 seeds): 1 to 16 original shards, then 1 to 8 random steps, each a random number of
 *       splits or merges within what one step can do.</li>
 *   <li><b>Tree deeper than the recursion guard</b> (1, 2, ..., 201 open shards): the leftmost shard splits at every
 *       step, giving a chain 200 deep, deep enough that the sorter's first lease is past {@code MAX_RECURSION_DEPTH}
 *       (32) and the guard fires.</li>
 *   <li><b>Small hand-written trees</b> for specific cases: a 2 to 3 reshard, a merge child next to split children
 *       at the same depth, children whose parent lease is gone, and multi-stream leases with the same shard IDs.</li>
 * </ul>
 *
 * <h2>Checkpoints and missing parents</h2>
 * <ul>
 *   <li><b>Marginal scaling and large scale-up trees:</b> every original shard is at SHARD_END and every newer shard
 *       is at its initial position (run once with TRIM_HORIZON and once with AT_TIMESTAMP). That's the lease table
 *       right after a reshard, for a consumer caught up across the whole stream (MillisBehindLatest = 0): all the
 *       original shards reach SHARD_END together, nothing newer has started, and nothing has been cleaned up yet. So
 *       these trees have no missing parents.</li>
 *   <li><b>Random trees:</b> first, part of the top of the tree is removed at random
 *       ({@link ShardTree#withTopTrimmed}). This stands in for SHARD_END leases that cleanup has already deleted,
 *       without modeling cleanup itself: a lease is only removed once all of its parents have been removed, so every
 *       remaining lease still has its children, and some remaining leases have parents with no lease (the sorter's
 *       {@code NO_LEASE} case). Then each remaining lease gets a random checkpoint
 *       ({@link ShardTree#withRandomCheckpoints}): SHARD_END (only if it has children), its initial position, or a
 *       mid-shard sequence number. A lease only gets past its initial position once every existing parent lease is
 *       at SHARD_END.</li>
 * </ul>
 *
 * <h2>Validation and assertions</h2>
 * Every tree, hand-written or generated, is first checked by {@link #assertValidShardTree}: unique keys, at most two
 * parents, no cycles, split and merge structure that matches the parent counts, the checkpoint rule above, and a
 * child for every SHARD_END lease. Then each test checks the sort: every lease returned, parents before children,
 * split siblings next to each other, and, for the marginal scaling and large scale-up trees where every shard changes
 * at every step, every step's leases before the next step's. Past the recursion guard, depths saturate, so the deep
 * tree only checks that every lease is returned and the original shard is first.
 */
class ShardTreeSorterTest {

    // =====================================================================================================
    // Small, hand-written trees
    // =====================================================================================================

    /** 2 -> 3 scale-up: t1 splits into a, b; t2 splits into c, d; b and c merge into e. */
    @Test
    void sort_ordersParentsBeforeChildren_keepsSplitSiblingsTogether_andIncludesShardEndLeases() {
        final List<Lease> leases = Arrays.asList(
                lease("e", TRIM_HORIZON, "b", "c"),
                lease("d", TRIM_HORIZON, "t2"),
                lease("a", TRIM_HORIZON, "t1"),
                lease("t2", SHARD_END),
                lease("c", TRIM_HORIZON, "t2"),
                lease("t1", SHARD_END),
                lease("b", TRIM_HORIZON, "t1"));
        assertValidShardTree(leases);

        final List<String> order = keys(ShardTreeSorter.sort(leases));

        assertEquals(7, order.size(), "every lease, SHARD_END included: " + order);
        assertTrue(order.indexOf("t1") < 2 && order.indexOf("t2") < 2, "roots first: " + order);
        assertEquals("e", order.get(6), "merge child last: " + order);
        assertEquals(1, Math.abs(order.indexOf("a") - order.indexOf("b")), "split siblings together: " + order);
        assertEquals(1, Math.abs(order.indexOf("c") - order.indexOf("d")), "split siblings together: " + order);
    }

    /** r1 and r2 merge into m while r3 splits into a and b: all three children are at depth 1. */
    @Test
    void sort_mergeChildSortsAheadOfSplitChildrenAtTheSameDepth() {
        final List<Lease> leases = Arrays.asList(
                lease("a", TRIM_HORIZON, "r3"),
                lease("b", TRIM_HORIZON, "r3"),
                lease("m", TRIM_HORIZON, "r1", "r2"),
                lease("r1", SHARD_END),
                lease("r2", SHARD_END),
                lease("r3", SHARD_END));
        assertValidShardTree(leases);

        final List<String> order = keys(ShardTreeSorter.sort(leases));

        assertEquals("m", order.get(3), "merge child first at depth 1: " + order);
    }

    /**
     * r1 and r2 merge into m, but r2 is still processing, so m isn't ready; r3 has split into a and b, which are.
     * All three children are at depth 1, and the ready split children come before the merge child.
     */
    @Test
    void sort_readyLeasesSortAheadOfNotReadyLeasesAtTheSameDepth() {
        final List<Lease> leases = Arrays.asList(
                lease("m", TRIM_HORIZON, "r1", "r2"),
                lease("a", TRIM_HORIZON, "r3"),
                lease("b", TRIM_HORIZON, "r3"),
                lease("r1", SHARD_END),
                lease("r2", new ExtendedSequenceNumber("5")),
                lease("r3", SHARD_END));
        assertValidShardTree(leases);

        final List<String> order = keys(ShardTreeSorter.sort(leases));

        assertEquals("m", order.get(5), "not-ready merge child last at depth 1: " + order);
        assertEquals(1, Math.abs(order.indexOf("a") - order.indexOf("b")), "split siblings together: " + order);
    }

    /**
     * With no shard tree relationships (every lease at depth 0, as when no reshard is in progress), the order is the
     * one the lease assignment deciders used before: unassigned leases (lastCounterIncrementNanos 0) first, then
     * expired leases, least recently renewed first.
     */
    @Test
    void sort_noShardTree_ordersByLastCounterIncrementNanos() {
        final List<Lease> leases = Arrays.asList(
                withLastCounterIncrementNanos(lease("expiredRecently", new ExtendedSequenceNumber("1")), 200L),
                withLastCounterIncrementNanos(lease("unassigned", TRIM_HORIZON), 0L),
                withLastCounterIncrementNanos(lease("expiredLongAgo", new ExtendedSequenceNumber("1")), 100L));

        assertEquals(
                Arrays.asList("unassigned", "expiredLongAgo", "expiredRecently"), keys(ShardTreeSorter.sort(leases)));
    }

    /**
     * The shard tree comes before lastCounterIncrementNanos: r2's worker died (expired, renewed at 500), and the
     * unassigned merge child m (0) is waiting on it, so r2 comes first. Sorting by lastCounterIncrementNanos alone
     * would put m first, where it would take a slot and sit blocked on r2.
     */
    @Test
    void sort_shardTreeOrderComesBeforeLastCounterIncrementNanos() {
        final List<Lease> leases = Arrays.asList(
                withLastCounterIncrementNanos(lease("m", TRIM_HORIZON, "r1", "r2"), 0L),
                lease("r1", SHARD_END),
                withLastCounterIncrementNanos(lease("r2", new ExtendedSequenceNumber("5")), 500L));
        assertValidShardTree(leases);

        final List<String> order = keys(ShardTreeSorter.sort(leases));

        assertTrue(order.indexOf("r2") < order.indexOf("m"), "expired parent before its unassigned child: " + order);
    }

    /**
     * Ready leases come before not-ready ones regardless of depth: m (depth 1) waits on r2, which is still
     * processing, while c (depth 2) is ready because its parent b has reached SHARD_END.
     */
    @Test
    void sort_readyLeaseComesBeforeShallowerNotReadyLease() {
        final List<Lease> leases = Arrays.asList(
                lease("m", TRIM_HORIZON, "r1", "r2"),
                lease("r1", SHARD_END),
                lease("r2", new ExtendedSequenceNumber("5")),
                lease("r3", SHARD_END),
                lease("a", new ExtendedSequenceNumber("5"), "r3"),
                lease("b", SHARD_END, "r3"),
                lease("c", TRIM_HORIZON, "b"),
                lease("d", TRIM_HORIZON, "b"));
        assertValidShardTree(leases);

        final List<String> order = keys(ShardTreeSorter.sort(leases));

        assertEquals("m", order.get(order.size() - 1), "the only not-ready lease is last: " + order);
        assertTrue(order.indexOf("b") < order.indexOf("c"), "parents still before children: " + order);
    }

    /**
     * At the same depth: merge children before split children, and split sibling groups ordered by their smallest
     * lastCounterIncrementNanos, with each group's siblings kept together.
     */
    @Test
    void sort_splitSiblingGroupsOrderedByLastCounterIncrementNanos() {
        final List<Lease> leases = Arrays.asList(
                withLastCounterIncrementNanos(lease("p1a", TRIM_HORIZON, "p1"), 300L),
                withLastCounterIncrementNanos(lease("p1b", TRIM_HORIZON, "p1"), 300L),
                withLastCounterIncrementNanos(lease("p2a", TRIM_HORIZON, "p2"), 100L),
                withLastCounterIncrementNanos(lease("p2b", TRIM_HORIZON, "p2"), 100L),
                withLastCounterIncrementNanos(lease("m", TRIM_HORIZON, "q1", "q2"), 500L),
                lease("p1", SHARD_END),
                lease("p2", SHARD_END),
                lease("q1", SHARD_END),
                lease("q2", SHARD_END));
        assertValidShardTree(leases);

        final List<String> order = keys(ShardTreeSorter.sort(leases));
        final int m = order.indexOf("m");
        final int p2a = order.indexOf("p2a");
        final int p2b = order.indexOf("p2b");
        final int p1a = order.indexOf("p1a");
        final int p1b = order.indexOf("p1b");

        assertTrue(m < Math.min(p2a, p2b), "merge child before the split children: " + order);
        assertTrue(Math.max(p2a, p2b) < Math.min(p1a, p1b), "p2 group (100) before p1 group (300): " + order);
        assertEquals(1, Math.abs(p2a - p2b), "p2 siblings together: " + order);
        assertEquals(1, Math.abs(p1a - p1b), "p1 siblings together: " + order);
    }

    /** A child whose parent lease was already deleted is at depth 0, ahead of a child whose parent still exists. */
    @Test
    void sort_leaseWhoseParentsAreGone_isAtTheTop() {
        final List<Lease> leases = Arrays.asList(
                lease("orphanA", TRIM_HORIZON, "deletedParent"),
                lease("orphanB", TRIM_HORIZON, "deletedParent"),
                lease("childA", TRIM_HORIZON, "root"),
                lease("childB", TRIM_HORIZON, "root"),
                lease("root", SHARD_END));
        assertValidShardTree(leases);

        final List<String> order = keys(ShardTreeSorter.sort(leases));

        assertTrue(order.indexOf("orphanA") < order.indexOf("childA"), "orphaned child is at depth 0: " + order);
        assertTrue(order.indexOf("orphanB") < order.indexOf("childB"), "orphaned child is at depth 0: " + order);
    }

    /** Every stream numbers its shards from shardId-000000000000, so keys must include the stream. */
    @Test
    void sort_multiStream_sameShardIdsInTwoStreams_doNotCollide() {
        final List<Lease> leases = new ArrayList<>();
        for (String stream : Arrays.asList("streamA", "streamB")) {
            leases.add(multiStreamLease(stream, "shardId-000000000000", SHARD_END));
            leases.add(multiStreamLease(stream, "shardId-000000000001", TRIM_HORIZON, "shardId-000000000000"));
            leases.add(multiStreamLease(stream, "shardId-000000000002", TRIM_HORIZON, "shardId-000000000000"));
        }
        assertValidShardTree(leases);

        final List<String> order = keys(ShardTreeSorter.sort(leases));

        assertEquals(6, order.size(), order.toString());
        for (String stream : Arrays.asList("streamA", "streamB")) {
            assertTrue(
                    order.indexOf(stream + ":shardId-000000000000") < order.indexOf(stream + ":shardId-000000000001"));
            assertEquals(
                    1,
                    Math.abs(order.indexOf(stream + ":shardId-000000000001")
                            - order.indexOf(stream + ":shardId-000000000002")),
                    "split siblings within each stream stay together: " + order);
        }
    }

    @Test
    void sort_returnsUnmodifiableList() {
        final List<Lease> sorted = ShardTreeSorter.sort(Collections.singletonList(lease("a", TRIM_HORIZON)));
        assertThrows(UnsupportedOperationException.class, () -> sorted.add(lease("b", TRIM_HORIZON)));
    }

    // =====================================================================================================
    // Generated trees
    // =====================================================================================================

    /**
     * Marginal scaling: 20 shards all split (to 40), then 19 adjacent pairs across the split boundaries merge (to
     * 21). Every original shard is at SHARD_END and every newer shard is at the initial position.
     */
    @ParameterizedTest
    @ValueSource(strings = {"TRIM_HORIZON", "AT_TIMESTAMP"})
    void sort_marginalScalingTree(final String initialPosition) {
        final ShardTree tree =
                ShardTree.fromOpenShardCounts(20, 40, 21).withOriginalShardsEndedAndRestAt(initial(initialPosition));
        assertValidShardTree(tree.leases());

        final List<Lease> sorted = ShardTreeSorter.sort(tree.leases());

        assertEquals(20 + 40 + 19, sorted.size());
        assertParentsBeforeChildren(sorted);
        assertSplitSiblingsAdjacent(sorted);
        assertStepsInOrder(sorted, tree);
    }

    /** Large scale-up: 64 shards doubled 6 times (to 4,096), 8,128 leases in all, every step still present. */
    @ParameterizedTest
    @ValueSource(strings = {"TRIM_HORIZON", "AT_TIMESTAMP"})
    void sort_largeScaleUpTree(final String initialPosition) {
        final ShardTree tree = ShardTree.fromOpenShardCounts(64, 128, 256, 512, 1024, 2048, 4096)
                .withOriginalShardsEndedAndRestAt(initial(initialPosition));
        assertValidShardTree(tree.leases());

        final List<Lease> sorted = ShardTreeSorter.sort(tree.leases());

        assertEquals(8128, sorted.size());
        assertParentsBeforeChildren(sorted);
        assertSplitSiblingsAdjacent(sorted);
        assertStepsInOrder(sorted, tree);
    }

    static IntStream seeds() {
        return IntStream.range(0, 50);
    }

    /**
     * Random reshard history, with a random part of the top of the tree removed (so some leases' parents have no
     * lease) and random checkpoints: any shard can be at SHARD_END, its initial position, or a mid-shard checkpoint,
     * as long as it's only past its initial position once all of its remaining parents reached SHARD_END.
     */
    @ParameterizedTest
    @MethodSource("seeds")
    void sort_randomTree(final int seed) {
        final Random random = new Random(seed);
        final ShardTree tree = ShardTree.random(random).withTopTrimmed(random).withRandomCheckpoints(random);
        assertValidShardTree(tree.leases());

        final List<Lease> sorted = ShardTreeSorter.sort(tree.leases());

        assertEquals(tree.leases().size(), sorted.size(), "seed " + seed);
        assertParentsBeforeChildren(sorted);
        assertSplitSiblingsAdjacent(sorted);
    }

    /**
     * A tree deep enough that the recursion guard fires: the leftmost shard splits on each of 200 steps, giving a
     * chain of 401 leases at depths 0 to 200, where each shard's depth is the step that created it.
     *
     * <p>The guard limits how many resolveNode calls are on the stack at once (MAX_RECURSION_DEPTH, 32), not how deep
     * a lease can be. The sorter resolves leases in its own HashMap's order, and the first lease it resolves has no
     * cached ancestors, so it recurses all the way up toward the root. If that lease is at depth 32 or more, the call
     * for its 32nd ancestor hits the guard. This test checks that precondition directly (with
     * {@link TopLevelResolveRecordingLease}) instead of trusting the iteration order. A shallower tree, like 40 steps,
     * happens to start resolving at depth 10, caches the top of the tree first, and never reaches the guard.
     *
     * <p>When the guard fires, the leases on the stack, and their descendants, get a saturated depth, so the order
     * of the deep leases isn't checked. What must still hold: the sort doesn't overflow the stack or throw, every
     * lease comes back (the lease where the guard fired isn't cached then, so it has to be resolved later), and the
     * original shard, the only lease with no parents, is still first.
     */
    @Test
    void sort_treeDeeperThanRecursionGuard_returnsEveryLease() {
        final int[] counts = IntStream.rangeClosed(1, 201).toArray();
        final ShardTree tree = ShardTree.fromOpenShardCounts(counts).withOriginalShardsEndedAndRestAt(TRIM_HORIZON);
        assertValidShardTree(tree.leases());
        final List<Lease> topLevelResolved = new ArrayList<>();
        final List<Lease> leases = tree.leases().stream()
                .map(lease -> new TopLevelResolveRecordingLease(lease, topLevelResolved))
                .collect(Collectors.toList());

        final List<Lease> sorted = ShardTreeSorter.sort(leases);

        final Lease firstResolved = topLevelResolved.get(0);
        assertTrue(
                tree.stepOf(firstResolved) >= 32,
                "the sorter started at " + firstResolved.leaseKey() + " (depth " + tree.stepOf(firstResolved)
                        + "), too shallow to reach the recursion guard; use a deeper tree");
        assertEquals(leases.size(), sorted.size());
        assertEquals(leases.get(0).leaseKey(), sorted.get(0).leaseKey(), "the original shard is first");
    }

    /**
     * Records the leases ShardTreeSorter starts resolving from its outer loop, in order, so the deep-tree test can
     * check which lease the sorter started from.
     *
     * <p>How it tells top-level calls apart: the sorter's outer loop calls resolveNode once per lease, and resolveNode
     * calls itself for each parent. So when a lease's resolveNode is the only one on the stack, the outer loop called
     * it directly; with more frames, it's being resolved on the way up from a descendant. resolveNode reads
     * parentShardIds() exactly once, and only for a lease not resolved yet (a cached lease returns before that), so
     * a parentShardIds() call with exactly one resolveNode frame on the stack means: the outer loop started resolving
     * this lease from scratch.
     *
     * <p>Why the first one matters: when the outer loop starts its first lease, nothing is cached yet, so resolveNode
     * recurses through every ancestor up to the root, one frame per level. If that lease is at depth 32 or more, the
     * call for its 32nd ancestor finds 32 leases on the stack and hits the guard. Later top-level leases can find
     * cached ancestors partway up, so whether they reach the guard depends on what was resolved before them; the
     * first one is the only one where depth alone decides it.
     *
     * <p>Why the input order doesn't decide this: the sorter copies the leases into a HashMap and loops over that, so
     * the visit order comes from the lease keys' hashes and the map's size, not from the order of the input list
     * (insertion order only matters between keys that land in the same hash bucket).
     * That's why the test checks the starting lease at runtime instead of arranging the input.
     *
     * <p>Relies on the method name {@code resolveNode}: if it's renamed, nothing is recorded and the test fails on an
     * empty list rather than passing silently.
     */
    private static final class TopLevelResolveRecordingLease extends Lease {
        private final List<Lease> topLevelResolved;

        TopLevelResolveRecordingLease(final Lease lease, final List<Lease> topLevelResolved) {
            super(lease);
            this.topLevelResolved = topLevelResolved;
        }

        @Override
        public Set<String> parentShardIds() {
            final long resolveNodeFrames = Arrays.stream(Thread.currentThread().getStackTrace())
                    .filter(frame -> ShardTreeSorter.class.getName().equals(frame.getClassName())
                            && "resolveNode".equals(frame.getMethodName()))
                    .count();
            if (resolveNodeFrames == 1) {
                topLevelResolved.add(this);
            }
            return super.parentShardIds();
        }
    }

    // =====================================================================================================
    // Tree builder
    // =====================================================================================================

    /**
     * Builds a shard tree by replaying reshard steps on a stream. The builder tracks the stream's open shards (shards
     * with no children yet) as a list in left-to-right order, which stands in for hash key order; hash key ranges
     * aren't modeled. Each step names the stream's open shard count after that step: going up splits that many open
     * shards (each split replaces one shard with two adjacent children), going down merges that many disjoint
     * adjacent pairs (each merge replaces two neighbors with one child). Shards a step doesn't touch stay open. Each
     * step is all splits or all merges, so a split-then-merge reshard is two steps.
     */
    private static final class ShardTree {
        /** In creation order, so parents always come before their children. */
        private final List<Lease> leases = new ArrayList<>();

        private final Map<String, List<String>> children = new HashMap<>();
        /** Which reshard step created each shard; 0 for the original shards. */
        private final Map<String, Integer> stepOf = new HashMap<>();

        private int nextShardId = 0;

        /**
         * {@code openShardCounts[0]} is the number of original shards; each following value is the stream's total
         * open shard count after one more reshard step (not the width of a tree level).
         */
        static ShardTree fromOpenShardCounts(final int... openShardCounts) {
            final ShardTree tree = new ShardTree();
            List<String> openShards = new ArrayList<>();
            for (int i = 0; i < openShardCounts[0]; i++) {
                openShards.add(tree.newShard(0));
            }
            for (int step = 1; step < openShardCounts.length; step++) {
                openShards = tree.reshard(openShards, openShardCounts[step], step);
            }
            return tree;
        }

        /**
         * 1 to 16 original shards, then 1 to 8 random reshard steps, each within what one step can do. Shards a step
         * leaves alone stay at their depth while others go deeper, which makes the tree uneven.
         *
         * <p>Next step's open shard count, from the current count c:
         * <ul>
         *   <li>Direction: always grow below 2 shards, always shrink at 64 or more, otherwise a coin flip. (So counts
         *       stay between 1 and 126, the most one step can grow 63 to.)</li>
         *   <li>Grow: c + 1 to 2c, uniformly, i.e. 1 to c splits; one step can at most double.</li>
         *   <li>Shrink: c - c/2 to c - 1, uniformly, i.e. 1 to c/2 merges (rounded down); one step can at most
         *       halve. ({@code Math.max(1, ...)} is only a guard: shrinking needs c &gt;= 2, where c/2 is already at
         *       least 1.)</li>
         * </ul>
         */
        static ShardTree random(final Random random) {
            final ShardTree tree = new ShardTree();
            List<String> openShards = new ArrayList<>();
            final int originalShards = 1 + random.nextInt(16);
            for (int i = 0; i < originalShards; i++) {
                openShards.add(tree.newShard(0));
            }
            final int steps = 1 + random.nextInt(8);
            for (int step = 1; step <= steps; step++) {
                final int count = openShards.size();
                final boolean grow = count < 2 || (count < 64 && random.nextBoolean());
                final int targetOpenShards =
                        grow ? count + 1 + random.nextInt(count) : count - 1 - random.nextInt(Math.max(1, count / 2));
                openShards = tree.reshard(openShards, targetOpenShards, step);
            }
            return tree;
        }

        List<Lease> leases() {
            return leases;
        }

        int stepOf(final Lease lease) {
            return stepOf.get(lease.leaseKey());
        }

        /** The original shards are at SHARD_END and every newer shard is at {@code initialPosition}. */
        ShardTree withOriginalShardsEndedAndRestAt(final ExtendedSequenceNumber initialPosition) {
            for (Lease lease : leases) {
                lease.checkpoint(stepOf(lease) == 0 ? SHARD_END : initialPosition);
            }
            return this;
        }

        /**
         * Removes a random part of the top of the tree, so some remaining leases have parents with no lease. Purely
         * structural: a lease is only removed once all of its parents have been removed, so every remaining lease
         * still has its children. (This is the shape a lease table has after cleanup, without modeling cleanup.)
         *
         * <p>How: {@link #leases} is in creation order, so every lease comes after its parents (not layer order). One pass visits each
         * lease once; a lease is removed on a coin flip, but only if all its parents were already removed
         * ({@code allMatch} is true for original shards, which have none). So removal always starts at the top.
         */
        ShardTree withTopTrimmed(final Random random) {
            final Set<String> removed = new HashSet<>();
            for (Lease lease : leases) {
                if (lease.parentShardIds().stream().allMatch(removed::contains) && random.nextBoolean()) {
                    removed.add(lease.leaseKey());
                }
            }
            leases.removeIf(lease -> removed.contains(lease.leaseKey()));
            return this;
        }

        /**
         * Random checkpoints for the remaining leases: SHARD_END (only for shards with children), the initial
         * position, or a mid-shard checkpoint. A lease can only be past its initial position once every existing
         * parent lease (one not removed by {@link #withTopTrimmed}) is at SHARD_END, since a consumer can't start a
         * shard before its parents finish.
         *
         * <p>How: {@link #leases} is in creation order, so each lease's parents get their checkpoints first. Then for
         * each lease:
         * <ul>
         *   <li>If an existing parent isn't at SHARD_END, the lease stays at its initial position.</li>
         *   <li>Otherwise, each allowed checkpoint is equally likely: the initial position or mid-shard (1/2 each),
         *       plus SHARD_END if the shard has children (1/3 each).</li>
         * </ul>
         * One initial position (TRIM_HORIZON or AT_TIMESTAMP) is picked for the whole tree, as one application
         * would use.
         */
        ShardTree withRandomCheckpoints(final Random random) {
            final ExtendedSequenceNumber initialPosition = random.nextBoolean() ? TRIM_HORIZON : AT_TIMESTAMP;
            final Map<String, Lease> byKey = new HashMap<>();
            for (Lease lease : leases) {
                byKey.put(lease.leaseKey(), lease);
            }
            for (Lease lease : leases) {
                final boolean existingParentsAtShardEnd = lease.parentShardIds().stream()
                        .map(byKey::get)
                        .allMatch(parent -> parent == null || SHARD_END.equals(parent.checkpoint()));
                final boolean hasChildren = !children.getOrDefault(lease.leaseKey(), Collections.emptyList())
                        .isEmpty();

                final ExtendedSequenceNumber checkpoint;
                if (!existingParentsAtShardEnd) {
                    // can't have started yet
                    checkpoint = initialPosition;
                } else if (hasChildren && random.nextInt(3) == 0) {
                    // finished; only possible once the shard has children (1 in 3)
                    checkpoint = SHARD_END;
                } else if (random.nextBoolean()) {
                    // started, somewhere in the middle of the shard (the rest split evenly with the next case)
                    checkpoint = new ExtendedSequenceNumber(String.valueOf(1 + random.nextInt(1000)));
                } else {
                    // allowed to start, but hasn't yet
                    checkpoint = initialPosition;
                }
                lease.checkpoint(checkpoint);
            }
            return this;
        }

        /** Applies one reshard step to the open shards and returns the open shards after it. */
        private List<String> reshard(final List<String> openShards, final int targetOpenShards, final int step) {
            if (targetOpenShards > openShards.size()) {
                return split(openShards, targetOpenShards - openShards.size(), step);
            }
            if (targetOpenShards < openShards.size()) {
                return merge(openShards, openShards.size() - targetOpenShards, step);
            }
            return openShards;
        }

        /** Splits {@code splits} open shards, spread evenly across the list. */
        private List<String> split(final List<String> openShards, final int splits, final int step) {
            if (splits > openShards.size()) {
                throw new IllegalArgumentException("one step can at most double the shard count");
            }
            final Set<Integer> splitAt = evenlySpread(openShards.size(), splits);
            final List<String> next = new ArrayList<>();
            for (int i = 0; i < openShards.size(); i++) {
                if (splitAt.contains(i)) {
                    next.add(newShard(step, openShards.get(i)));
                    next.add(newShard(step, openShards.get(i)));
                } else {
                    next.add(openShards.get(i));
                }
            }
            return next;
        }

        /**
         * Merges {@code merges} disjoint pairs of neighbors, spread evenly across the list.
         *
         * <p>Which pairs: after a step where every shard split, the list looks like a1 a2 b1 b2 c1 c2 ..., with each
         * shard's two children side by side. Pairing from index 0, (a1 a2) (b1 b2) ..., would merge split siblings
         * back together, undoing the split. Pairing from index 1, (a2 b1) (b2 c1) ..., joins pieces of two different
         * originals (the new merge child's grandparents), so the new boundaries don't line up with the old ones, as
         * in a real marginal reshard. This only mimics that shape; it doesn't model how Kinesis actually picks
         * hash key ranges.
         *
         * <p>Pairing from index 1 leaves the first shard out (and the last, for an even count), so it has one fewer
         * pair when the count is even. Halving an even count exactly (e.g. 6 to 3, or 2 to 1) needs every shard in a
         * pair, so only then does it pair from index 0.
         */
        private List<String> merge(final List<String> openShards, final int merges, final int step) {
            if (merges > openShards.size() / 2) {
                throw new IllegalArgumentException("one step can at most halve the shard count");
            }
            final int firstPairIndex = merges <= pairsFrom(1, openShards.size()) ? 1 : 0;
            // pair p is (firstPairIndex + 2p, firstPairIndex + 2p + 1)
            final Set<Integer> mergeAt = new HashSet<>();
            for (int pair : evenlySpread(pairsFrom(firstPairIndex, openShards.size()), merges)) {
                mergeAt.add(firstPairIndex + 2 * pair);
            }
            final List<String> next = new ArrayList<>();
            for (int i = 0; i < openShards.size(); i++) {
                if (mergeAt.contains(i)) {
                    next.add(newShard(step, openShards.get(i), openShards.get(i + 1)));
                    i++;
                } else {
                    next.add(openShards.get(i));
                }
            }
            return next;
        }

        /** Number of disjoint neighbor pairs in a list of {@code size} when pairing from {@code firstIndex}. */
        private static int pairsFrom(final int firstIndex, final int size) {
            return (size - firstIndex) / 2;
        }

        private String newShard(final int step, final String... parents) {
            final String shardId = String.format("shardId-%012d", nextShardId++);
            leases.add(lease(shardId, TRIM_HORIZON, parents));
            stepOf.put(shardId, step);
            for (String parent : parents) {
                children.computeIfAbsent(parent, k -> new ArrayList<>()).add(shardId);
            }
            return shardId;
        }

        /**
         * {@code count} distinct indexes in [0, size), spread evenly. Deterministic, not random: the random trees pick
         * a random count, but where the splits or merges land is always spread out this way.
         */
        private static Set<Integer> evenlySpread(final int size, final int count) {
            if (count > size) {
                throw new IllegalArgumentException(
                        "can't pick " + count + " distinct indexes from " + size + "; count must be at most size");
            }
            final Set<Integer> indexes = new HashSet<>();
            for (int j = 0; j < count; j++) {
                // start of slot j, rounded down; long so j * size can't overflow
                indexes.add((int) ((long) j * size / count));
            }
            return indexes;
        }
    }

    // =====================================================================================================
    // Validation and assertions
    // =====================================================================================================

    /**
     * Checks that the leases form a shard tree the KCL could produce:
     * <ul>
     *   <li>unique lease keys, at most two parents per lease, and no cycles;</li>
     *   <li>split/merge consistency among leases present: a lease with one parent ID is a split child, so its parent
     *       (if present) has exactly two children; a lease with two parent IDs is a merge child, so each present
     *       parent has exactly one child;</li>
     *   <li>a lease is only past its initial position once every present parent is at SHARD_END;</li>
     *   <li>a SHARD_END lease has at least one child (its children's leases are created before it checkpoints).</li>
     * </ul>
     * Parents missing from the leases are treated as already deleted.
     */
    private static void assertValidShardTree(final Collection<Lease> leases) {
        final Map<String, Lease> byKey = new HashMap<>();
        for (Lease lease : leases) {
            assertTrue(byKey.put(lease.leaseKey(), lease) == null, "duplicate lease key " + lease.leaseKey());
        }
        final Map<String, List<Lease>> childrenOf = new HashMap<>();
        for (Lease lease : leases) {
            assertTrue(lease.parentShardIds().size() <= 2, lease.leaseKey() + " has more than two parents");
            for (String parentKey : parentKeys(lease)) {
                if (byKey.containsKey(parentKey)) {
                    childrenOf
                            .computeIfAbsent(parentKey, k -> new ArrayList<>())
                            .add(lease);
                }
            }
        }
        for (Lease lease : leases) {
            final int expectedSiblings = lease.parentShardIds().size() == 1 ? 2 : 1;
            for (String parentKey : parentKeys(lease)) {
                // if the parent no longer exists, then we cannot assert that another sibling still exists
                // and we would also not check the parent's checkpoint since the lease doesn't exist
                if (byKey.containsKey(parentKey)) {
                    assertEquals(
                            expectedSiblings,
                            childrenOf.get(parentKey).size(),
                            parentKey + " has the wrong number of children for " + lease.leaseKey());
                    if (!isInitialPosition(lease.checkpoint())) {
                        assertEquals(
                                SHARD_END,
                                byKey.get(parentKey).checkpoint(),
                                lease.leaseKey() + " is past its initial position before parent " + parentKey
                                        + " reached SHARD_END");
                    }
                }
            }
            if (SHARD_END.equals(lease.checkpoint())) {
                assertTrue(
                        childrenOf.containsKey(lease.leaseKey()), lease.leaseKey() + " is at SHARD_END with no child");
            }
        }
        final Set<String> done = new HashSet<>();
        for (Lease lease : leases) {
            assertAcyclic(lease, byKey, new HashSet<>(), done);
        }
    }

    private static void assertAcyclic(
            final Lease lease, final Map<String, Lease> byKey, final Set<String> onPath, final Set<String> done) {
        if (lease == null || done.contains(lease.leaseKey())) {
            return;
        }
        assertTrue(onPath.add(lease.leaseKey()), "cycle at " + lease.leaseKey());
        for (String parentKey : parentKeys(lease)) {
            assertAcyclic(byKey.get(parentKey), byKey, onPath, done);
        }
        onPath.remove(lease.leaseKey());
        done.add(lease.leaseKey());
    }

    private static void assertParentsBeforeChildren(final List<Lease> sorted) {
        final Map<String, Integer> position = positions(sorted);
        for (Lease lease : sorted) {
            for (String parentKey : parentKeys(lease)) {
                if (position.containsKey(parentKey)) {
                    assertTrue(
                            position.get(parentKey) < position.get(lease.leaseKey()),
                            parentKey + " should come before its child " + lease.leaseKey());
                }
            }
        }
    }

    /**
     * Split siblings sit next to each other: for every split child (one parent ID) whose parent lease still exists,
     * the parent's two children are at consecutive positions in the sorted list.
     *
     * <p>Only split children with a present parent are checked. If the parent is gone, the sorter treats each
     * remaining sibling as a top-level lease (depth 0, no grouping), so it doesn't keep them together, and one of
     * them may be gone too. When the parent is present, {@link #assertValidShardTree} has already checked it has
     * exactly two children, so both are in the list.
     */
    private static void assertSplitSiblingsAdjacent(final List<Lease> sorted) {
        final Set<String> present = positions(sorted).keySet();
        // parent key -> positions of its split children in the sorted list, in increasing order
        final Map<String, List<Integer>> splitChildPositions = new HashMap<>();
        for (int i = 0; i < sorted.size(); i++) {
            final Lease lease = sorted.get(i);
            final List<String> parents = parentKeys(lease);
            if (parents.size() == 1 && present.contains(parents.get(0))) {
                splitChildPositions
                        .computeIfAbsent(parents.get(0), k -> new ArrayList<>())
                        .add(i);
            }
        }
        for (Map.Entry<String, List<Integer>> siblings : splitChildPositions.entrySet()) {
            final List<Integer> at = siblings.getValue();
            assertEquals(2, at.size(), siblings.getKey() + " should have two split children: " + at);
            assertEquals(at.get(0) + 1, at.get(1), "children of " + siblings.getKey() + " should be adjacent: " + at);
        }
    }

    /** Every lease created at step k comes before every lease created at step k + 1. */
    private static void assertStepsInOrder(final List<Lease> sorted, final ShardTree tree) {
        int previousStep = 0;
        for (Lease lease : sorted) {
            final int step = tree.stepOf(lease);
            assertTrue(step >= previousStep, lease.leaseKey() + " from step " + step + " after step " + previousStep);
            previousStep = step;
        }
    }

    // =====================================================================================================
    // Helpers
    // =====================================================================================================

    private static boolean isInitialPosition(final ExtendedSequenceNumber checkpoint) {
        return TRIM_HORIZON.equals(checkpoint) || AT_TIMESTAMP.equals(checkpoint);
    }

    private static ExtendedSequenceNumber initial(final String position) {
        return "AT_TIMESTAMP".equals(position) ? AT_TIMESTAMP : TRIM_HORIZON;
    }

    private static List<String> parentKeys(final Lease lease) {
        return lease.parentShardIds().stream()
                .map(parentShardId -> ShardTreeSorter.leaseKeyFor(lease, parentShardId))
                .collect(Collectors.toList());
    }

    private static Map<String, Integer> positions(final List<Lease> sorted) {
        final Map<String, Integer> position = new HashMap<>();
        for (int i = 0; i < sorted.size(); i++) {
            position.put(sorted.get(i).leaseKey(), i);
        }
        return position;
    }

    private static Lease lease(final String key, final ExtendedSequenceNumber checkpoint, final String... parents) {
        final Lease lease = new Lease();
        lease.leaseKey(key);
        lease.checkpoint(checkpoint);
        lease.parentShardIds(Arrays.asList(parents));
        return lease;
    }

    private static MultiStreamLease multiStreamLease(
            final String streamIdentifier,
            final String shardId,
            final ExtendedSequenceNumber checkpoint,
            final String... parents) {
        final MultiStreamLease lease = new MultiStreamLease();
        lease.streamIdentifier(streamIdentifier);
        lease.shardId(shardId);
        lease.leaseKey(MultiStreamLease.getLeaseKey(streamIdentifier, shardId));
        lease.checkpoint(checkpoint);
        lease.parentShardIds(Arrays.asList(parents));
        return lease;
    }

    private static List<String> keys(final List<Lease> leases) {
        return leases.stream().map(Lease::leaseKey).collect(Collectors.toList());
    }

    private static Lease withLastCounterIncrementNanos(final Lease lease, final long lastCounterIncrementNanos) {
        lease.lastCounterIncrementNanos(lastCounterIncrementNanos);
        return lease;
    }
}
