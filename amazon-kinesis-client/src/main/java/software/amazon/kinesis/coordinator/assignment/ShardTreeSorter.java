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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.MultiStreamLease;

import static software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber.SHARD_END;

/**
 * Orders leases through the shard tree, for SHARD_END lease cleanup (parents before children) and for lease
 * assignment (leases that unblock the rest of the tree first).
 *
 * <p>Sort order: ready leases first, then by depth (top of the tree first), then by type, then by
 * lastCounterIncrementNanos, with split siblings kept next to each other. The full key order is in the table above
 * {@code SortingValue.compareTo}.
 *
 * <p>Parents still come before their children: a ready lease's parents are at SHARD_END, so they're ready too and
 * at a smaller depth; a not-ready lease's parents are either ready, or not ready at a smaller depth. With no shard
 * tree relationships among the leases (every lease ready, at depth 0, with no parent lease), the order is by
 * lastCounterIncrementNanos alone.
 *
 * <p>Pure: {@link #sort(Collection)} takes leases and returns them in order, with no side effects.
 */
final class ShardTreeSorter {

    private static final int MAX_RECURSION_DEPTH = 32;

    /** Returned when a parent lease doesn't exist (above the top of the tree, or already cleaned up). */
    private static final int NO_LEASE = -1;

    /**
     * Keyed by lease key, not shard ID: in multi-stream mode every stream has its own shardId-000000000000, so
     * shard IDs alone would collide across streams.
     */
    private final Map<String, Lease> leaseByLeaseKey;

    private final Map<String, SortingValue> nodes = new HashMap<>();
    private final Set<String> visiting = new HashSet<>();
    private final Map<String, Integer> groupingKeys = new HashMap<>();

    private ShardTreeSorter(final Collection<Lease> leases) {
        this.leaseByLeaseKey = new HashMap<>();
        for (Lease lease : leases) {
            leaseByLeaseKey.putIfAbsent(lease.leaseKey(), lease);
        }
    }

    /** Returns the given leases (including SHARD_END leases) in shard tree order, as an unmodifiable list. */
    static List<Lease> sort(final Collection<Lease> leases) {
        return new ShardTreeSorter(leases).buildSortedList();
    }

    /**
     * Lease key for a shard related to {@code lease} (a parent or child). Lease parent and child fields hold shard
     * IDs; in multi-stream mode the lease key adds the stream prefix.
     */
    static String leaseKeyFor(final Lease lease, final String shardId) {
        return lease instanceof MultiStreamLease
                ? MultiStreamLease.getLeaseKey(((MultiStreamLease) lease).streamIdentifier(), shardId)
                : shardId;
    }

    /**
     * Builds the sorted list in three steps:
     * <ol>
     *   <li>Resolve every lease ({@link #resolveNode}): its depth, readiness, type, and grouping key.</li>
     *   <li>Give every lease its group's lastCounterIncrementNanos: for split children, the smallest among the
     *       siblings (the ones sharing a grouping key); for any other lease, its own.</li>
     *   <li>Sort by {@link SortingValue#compareTo}.</li>
     * </ol>
     *
     * <p>Why step 2 is needed: a sort on each lease's own fields can't keep split siblings together and also order
     * them by lastCounterIncrementNanos. Say split siblings A (100) and B (500) and another group's split child C
     * (300) tie on readiness, depth, and type. Sorting by their own lastCounterIncrementNanos gives A, C, B, which
     * separates the siblings. Sorting by grouping key first keeps the siblings together, but puts the groups in an
     * arbitrary order (grouping keys are handed out in resolution order), which loses the lease assignment deciders'
     * original order of unassigned leases (0) first, then the least recently renewed. What's needed is one value per
     * group, the smallest among its siblings, so the group sorts where its most urgent member would. That value
     * depends on the other siblings, so it can only be computed after every lease has been resolved, which is why
     * it's set in a second pass rather than when each lease is resolved.
     */
    private List<Lease> buildSortedList() {
        // process all leases for sorting values
        for (Lease lease : leaseByLeaseKey.values()) {
            resolveNode(lease);
        }

        // second pass: a split sibling group sorts by its smallest lastCounterIncrementNanos, so every sibling gets
        // the group's value (and the siblings stay together); every other lease is its own group
        final Map<Integer, Long> splitGroupNanos = new HashMap<>();
        for (SortingValue value : nodes.values()) {
            if (value.getType() == NodeType.SPLIT_CHILD) {
                splitGroupNanos.merge(value.getGroupingKey(), value.getLastCounterIncrementNanos(), Math::min);
            }
        }
        for (SortingValue value : nodes.values()) {
            value.groupLastCounterIncrementNanos = value.getType() == NodeType.SPLIT_CHILD
                    ? splitGroupNanos.get(value.getGroupingKey())
                    : value.getLastCounterIncrementNanos();
        }

        // sort leases by their sorting values
        return Collections.unmodifiableList(nodes.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .map(entry -> leaseByLeaseKey.get(entry.getKey()))
                .collect(Collectors.toList()));
    }

    /**
     * Resolves a lease's {@link SortingValue} and returns its depth, resolving its parents first.
     *
     * <p>A depth-first walk up the parent links, cached per lease: a lease's depth is one more than its deepest
     * parent's, counting only parents that are in the given leases (a parent with no lease counts as {@link
     * #NO_LEASE}, so a lease whose parents are all gone is at depth 0). Each lease is resolved once; later calls,
     * from the outer loop or from a child, return the cached depth. Along the way it works out whether the lease is
     * ready (every parent lease at SHARD_END or gone), its type (parent, merge child, or split child, by which parent
     * leases exist), and, for a split child, a grouping key shared with its siblings.
     *
     * <p>Guards: {@code visiting} holds the leases on the current recursion path. A lease already on the path means a
     * cycle in the lease table data, which throws. If the path is {@link #MAX_RECURSION_DEPTH} deep, this returns
     * {@code Integer.MAX_VALUE} without caching, so the descendants on this path get a saturated depth, and the lease
     * is resolved properly when the outer loop reaches it directly.
     */
    private int resolveNode(final Lease lease) {
        // base case: we've gone over the top of the shard tree
        if (lease == null) {
            return NO_LEASE;
        }

        // reuse the cached shard depth value if available
        final SortingValue cached = nodes.get(lease.leaseKey());
        if (cached != null) {
            return cached.getMaxDepth();
        }

        // avoid stack overflow; visiting holds exactly the leases on the current
        // recursion stack, so its size is the recursion depth. Don't cache here:
        // this lease's depth is only unknown relative to the current path, and it
        // will be resolved properly when the outer loop reaches it directly.
        if (visiting.size() >= MAX_RECURSION_DEPTH) {
            return Integer.MAX_VALUE;
        }

        // should not happen per Kinesis API; this means the lease table data was invalid
        if (!visiting.add(lease.leaseKey())) {
            throw new IllegalStateException("Circular dependency detected at shard " + lease.leaseKey());
        }

        // A merge child has two parents and a split child one. The lease table stores them as one unordered string
        // set (parentShardId), so which one Kinesis called the parent vs the adjacent parent isn't kept; take them in
        // sorted order so the result doesn't depend on set iteration order.
        final Iterator<String> parentShardIds =
                lease.parentShardIds().stream().sorted().iterator();
        final String parentKey = parentShardIds.hasNext() ? leaseKeyFor(lease, parentShardIds.next()) : null;
        final String adjacentParentKey = parentShardIds.hasNext() ? leaseKeyFor(lease, parentShardIds.next()) : null;

        // check both paths to find how buried the shard is; a parent with no lease (never recorded, or already
        // deleted) returns NO_LEASE. Parents at SHARD_END still have leases, so they still count.
        final Lease parent = parentKey == null ? null : leaseByLeaseKey.get(parentKey);
        final Lease adjacentParent = adjacentParentKey == null ? null : leaseByLeaseKey.get(adjacentParentKey);
        final int parentDepth = parent == null ? NO_LEASE : resolveNode(parent);
        final int adjacentParentDepth = adjacentParent == null ? NO_LEASE : resolveNode(adjacentParent);

        // ready to process: every parent that still has a lease has reached SHARD_END. Only a merge child can be
        // not ready, since a split child's lease is created when its one parent reaches SHARD_END.
        final boolean ready = isDoneOrGone(parent) && isDoneOrGone(adjacentParent);

        // shard depth is one more than its deepest parent (roots: -1 + 1 = 0);
        // saturate at MAX_VALUE rather than overflowing to a negative depth
        final int deepestParent = Math.max(parentDepth, adjacentParentDepth);
        final int maxDepth = deepestParent == Integer.MAX_VALUE ? Integer.MAX_VALUE : deepestParent + 1;

        // the LAM's last-renewal time; unassigned leases have 0, and a lease it hasn't set yet counts as 0 too
        final long lastCounterIncrementNanos =
                lease.lastCounterIncrementNanos() == null ? 0L : lease.lastCounterIncrementNanos();

        final SortingValue value = buildSortingValue(
                maxDepth,
                ready,
                lastCounterIncrementNanos,
                parentKey,
                parentDepth != NO_LEASE,
                adjacentParentDepth != NO_LEASE);

        // add sorting value result, unwind/pop visited set, and bubble up depth result
        nodes.put(lease.leaseKey(), value);
        visiting.remove(lease.leaseKey());
        return maxDepth;
    }

    /** True if the parent lease is gone (or there's no such parent) or has reached SHARD_END. */
    private static boolean isDoneOrGone(final Lease parent) {
        return parent == null || SHARD_END.equals(parent.checkpoint());
    }

    /**
     * Classifies the lease by which parent leases still exist. Split siblings share a parent,
     * so they get the same grouping key and sort next to each other; otherwise the
     * next unused key is assigned.
     */
    private SortingValue buildSortingValue(
            final int maxDepth,
            final boolean ready,
            final long lastCounterIncrementNanos,
            final String parentKey,
            final boolean hasParent,
            final boolean hasAdjacentParent) {
        if (!hasParent) {
            return new SortingValue(maxDepth, ready, 0, NodeType.PARENT, lastCounterIncrementNanos);
        }
        if (hasAdjacentParent) {
            return new SortingValue(maxDepth, ready, 0, NodeType.MERGE_CHILD, lastCounterIncrementNanos);
        }
        final int groupingKey = groupingKeys.computeIfAbsent(parentKey, k -> groupingKeys.size() + 1);
        return new SortingValue(maxDepth, ready, groupingKey, NodeType.SPLIT_CHILD, lastCounterIncrementNanos);
    }

    @Getter
    @RequiredArgsConstructor
    private static class SortingValue implements Comparable<SortingValue> {
        private final int maxDepth;
        /** Every parent lease is at SHARD_END or gone, so the shard can start processing as soon as it's owned. */
        private final boolean ready;

        private final int groupingKey;
        private final NodeType type;
        /**
         * The LAM's lastCounterIncrementNanos: 0 for unassigned leases, otherwise when the lease was last renewed.
         * Lowest priority, so when the shard tree doesn't decide (e.g. every lease is at depth 0, as when no
         * reshard is in progress), the order is the LAM's original one: unassigned leases first, then expired
         * leases, least recently renewed first.
         */
        private final long lastCounterIncrementNanos;
        /**
         * The sibling group's lastCounterIncrementNanos: for split children the smallest among the siblings, for
         * every other lease its own. Set in a second pass, once every lease has been resolved.
         */
        private long groupLastCounterIncrementNanos;

        // Sort keys, most important first. Each key only matters when every key before it is tied:
        //
        //  #  Key                             What it decides
        //  -  ------------------------------  -------------------------------------------------------------------
        //  1  ready                           Ready leases (every parent lease at SHARD_END or gone) before
        //                                     not-ready ones, at any depth. Only a merge child can be not ready.
        //  2  maxDepth                        Top of the tree first. Only parents with a lease count, so a lease
        //                                     whose parents are all gone is at depth 0.
        //  3  type                            At the same depth: PARENT (no parent lease), then MERGE_CHILD
        //                                     (two), then SPLIT_CHILD (one).
        //  4  groupLastCounterIncrementNanos  Merge children among themselves, and split sibling groups among
        //                                     themselves (a group's value is its smallest member's; any other
        //                                     lease uses its own). Unassigned (0) first, then the least recently
        //                                     renewed, as the lease assignment deciders sorted before.
        //  5  groupingKey                     Keeps two sibling groups whose values tie on key 4 from
        //                                     interleaving (common: right after a reshard, every group's value
        //                                     is 0).
        //  6  lastCounterIncrementNanos       The two siblings within one group: unassigned before expired.
        //
        // Split siblings share their one parent, so they tie on keys 1 to 3, and they share keys 4 and 5, so nothing
        // from another group sorts between them. Ready first also keeps parents before children: a ready lease's
        // parents are at SHARD_END, so they're ready too and at a smaller depth; a not-ready lease's parents are
        // either ready, or not ready at a smaller depth.
        @Override
        public int compareTo(final SortingValue other) {
            if (ready != other.ready) {
                return ready ? -1 : 1;
            }
            if (maxDepth != other.maxDepth) {
                return Integer.compare(maxDepth, other.maxDepth);
            }
            if (type != other.type) {
                return Integer.compare(type.ordinal(), other.type.ordinal());
            }
            if (groupLastCounterIncrementNanos != other.groupLastCounterIncrementNanos) {
                return Long.compare(groupLastCounterIncrementNanos, other.groupLastCounterIncrementNanos);
            }
            if (groupingKey != other.groupingKey) {
                return Integer.compare(groupingKey, other.groupingKey);
            }
            return Long.compare(lastCounterIncrementNanos, other.lastCounterIncrementNanos);
        }
    }

    private enum NodeType {
        PARENT,
        MERGE_CHILD,
        SPLIT_CHILD
    }
}
