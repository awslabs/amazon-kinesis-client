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

package software.amazon.kinesis.leases;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.leases.exceptions.LeasePendingDeletion;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LeaseCleanupManagerTest {

    private static final ShardInfo SHARD_INFO =
            new ShardInfo("shardId", "concurrencyToken", Collections.emptySet(), ExtendedSequenceNumber.LATEST);

    private static final StreamIdentifier STREAM_IDENTIFIER = StreamIdentifier.singleStreamInstance("streamName");

    private final long leaseCleanupIntervalMillis = Duration.ofSeconds(1).toMillis();
    private final long completedLeaseCleanupIntervalMillis =
            Duration.ofSeconds(0).toMillis();
    private final long garbageLeaseCleanupIntervalMillis = Duration.ofSeconds(0).toMillis();
    private boolean cleanupLeasesOfCompletedShards = true;
    private LeaseCleanupManager leaseCleanupManager;
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    @Mock
    private LeaseRefresher leaseRefresher;

    @Mock
    private LeaseCoordinator leaseCoordinator;

    @Mock
    private ShardDetector shardDetector;

    @Mock
    private ScheduledExecutorService deletionThreadPool;

    @Before
    public void setUp() throws Exception {
        leaseCleanupManager = new LeaseCleanupManager(
                leaseCoordinator,
                NULL_METRICS_FACTORY,
                deletionThreadPool,
                cleanupLeasesOfCompletedShards,
                leaseCleanupIntervalMillis,
                completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis);

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
    }

    /**
     * Tests subsequent calls to start {@link LeaseCleanupManager}.
     */
    @Test
    public final void testSubsequentStarts() {
        leaseCleanupManager.start();
        Assert.assertTrue(leaseCleanupManager.isRunning());
        leaseCleanupManager.start();
    }

    /**
     * Tests subsequent calls to shutdown {@link LeaseCleanupManager}.
     */
    @Test
    public final void testSubsequentShutdowns() {
        leaseCleanupManager.start();
        Assert.assertTrue(leaseCleanupManager.isRunning());
        leaseCleanupManager.shutdown();
        Assert.assertFalse(leaseCleanupManager.isRunning());
        leaseCleanupManager.shutdown();
    }

    /**
     * Tests that when both child shard leases are present, we are able to delete the parent shard for the completed
     * shard case.
     */
    @Test
    public final void testParentShardLeaseDeletedSplitCase() throws Exception {
        verifyExpectedDeletedLeasesCompletedShardCase(
                SHARD_INFO, childShardsForSplit(), ExtendedSequenceNumber.LATEST, 1);
    }

    /**
     * Tests that when both child shard leases are present, we are able to delete the parent shard for the completed
     * shard case.
     */
    @Test
    public final void testParentShardLeaseDeletedMergeCase() throws Exception {
        verifyExpectedDeletedLeasesCompletedShardCase(
                SHARD_INFO, childShardsForMerge(), ExtendedSequenceNumber.LATEST, 1);
    }

    /**
     * Tests that if cleanupLeasesOfCompletedShards is not enabled by the customer, then no leases are cleaned up for
     * the completed shard case.
     */
    @Test
    public final void testNoLeasesDeletedWhenNotEnabled() throws Exception {
        cleanupLeasesOfCompletedShards = false;

        leaseCleanupManager = new LeaseCleanupManager(
                leaseCoordinator,
                NULL_METRICS_FACTORY,
                deletionThreadPool,
                cleanupLeasesOfCompletedShards,
                leaseCleanupIntervalMillis,
                completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis);

        verifyExpectedDeletedLeasesCompletedShardCase(
                SHARD_INFO, childShardsForSplit(), ExtendedSequenceNumber.LATEST, 0);
    }

    /**
     * Tests that if some of the child shard leases are missing, we fail fast and don't delete the parent shard lease
     * for the completed shard case.
     */
    @Test
    public final void testNoCleanupWhenSomeChildShardLeasesAreNotPresent() throws Exception {
        List<ChildShard> childShards = childShardsForSplit();

        verifyExpectedDeletedLeasesCompletedShardCase(SHARD_INFO, childShards, ExtendedSequenceNumber.LATEST, false, 0);
    }

    /**
     * Tests that if some child shard leases haven't begun processing (at least one lease w/ checkpoint TRIM_HORIZON),
     * we don't delete them for the completed shard case.
     */
    @Test
    public final void testParentShardLeaseNotDeletedWhenChildIsAtTrim() throws Exception {
        testParentShardLeaseNotDeletedWhenChildIsAtPosition(ExtendedSequenceNumber.TRIM_HORIZON);
    }

    /**
     * Tests that if some child shard leases haven't begun processing (at least one lease w/ checkpoint AT_TIMESTAMP),
     * we don't delete them for the completed shard case.
     */
    @Test
    public final void testParentShardLeaseNotDeletedWhenChildIsAtTimestamp() throws Exception {
        testParentShardLeaseNotDeletedWhenChildIsAtPosition(ExtendedSequenceNumber.AT_TIMESTAMP);
    }

    private void testParentShardLeaseNotDeletedWhenChildIsAtPosition(ExtendedSequenceNumber extendedSequenceNumber)
            throws Exception {
        verifyExpectedDeletedLeasesCompletedShardCase(SHARD_INFO, childShardsForMerge(), extendedSequenceNumber, 0);
    }

    /**
     * Tests that if a lease's parents are still present, we do not delete the lease.
     */
    @Test
    public final void testLeaseNotDeletedWhenParentsStillPresent() throws Exception {
        final ShardInfo shardInfo = new ShardInfo(
                "shardId-0", "concurrencyToken", Collections.singleton("parent"), ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(
                shardInfo, childShardsForMerge(), ExtendedSequenceNumber.LATEST, 0);
    }

    /**
     * Verify {@link NullPointerException} is not thrown when a null lease is enqueued.
     */
    @Test
    public void testEnqueueNullLease() {
        leaseCleanupManager.enqueueForDeletion(createLeasePendingDeletion(null, SHARD_INFO));
    }

    /**
     * Tests ResourceNotFound case for if a shard expires, that we delete the lease when shardExpired is found.
     */
    @Test
    public final void testLeaseDeletedWhenShardDoesNotExist() throws Exception {
        final Lease heldLease =
                LeaseHelper.createLease(SHARD_INFO.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        testLeaseDeletedWhenShardDoesNotExist(heldLease);
    }

    /**
     * Tests ResourceNotFound case when completed lease cleanup is disabled.
     */
    @Test
    public final void testLeaseDeletedWhenShardDoesNotExistAndCleanupCompletedLeaseDisabled() throws Exception {
        final Lease heldLease =
                LeaseHelper.createLease(SHARD_INFO.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        cleanupLeasesOfCompletedShards = false;

        leaseCleanupManager = new LeaseCleanupManager(
                leaseCoordinator,
                NULL_METRICS_FACTORY,
                deletionThreadPool,
                cleanupLeasesOfCompletedShards,
                leaseCleanupIntervalMillis,
                completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis);

        testLeaseDeletedWhenShardDoesNotExist(heldLease);
    }

    private void testLeaseDeletedWhenShardDoesNotExist(Lease heldLease) throws Exception {
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(shardDetector.getChildShards(any(String.class))).thenThrow(ResourceNotFoundException.class);
        when(leaseRefresher.getLease(heldLease.leaseKey())).thenReturn(heldLease);

        leaseCleanupManager.enqueueForDeletion(createLeasePendingDeletion(heldLease, SHARD_INFO));
        leaseCleanupManager.cleanupLeases();

        verify(shardDetector).getChildShards(SHARD_INFO.shardId());
        verify(leaseRefresher).deleteLease(heldLease);
    }

    // =====================================================================================================
    // A worker's queued SHARD_END lease after the leader has evicted or deleted it (DynamoDB Local)
    //
    // When a worker finishes a shard, its ShutdownTask queues the lease in this worker's LeaseCleanupManager. With
    // leader-side SHARD_END cleanup, the leader may evict that lease (clearing its owner) or delete it before this
    // worker's cleanup gets to it. The queued entry still holds the lease as this worker last saw it, owner
    // included. These tests run the worker's cleanup against a real lease table in that state, and check that it
    // neither fails nor writes the old owner back.
    //
    // The leader (LeaseAssignmentManager) isn't involved; only the worker's LeaseCleanupManager runs. The leader's
    // actions are simulated by what each test writes to DynamoDB Local before the cleanup round:
    //  - The SHARD_END lease:
    //     - Evicted: written as the leader leaves it, at SHARD_END with no leaseOwner (evictedShardEndLease()).
    //       Eviction clears the owner and increments the lease counter, so the table copy has counter 2.
    //     - Deleted: never written at all.
    //  - The child leases: always written, owned by another worker ("otherWorker"), at a checkpoint past their
    //    initial position (started) or at TRIM_HORIZON (not started) (createChildLeases()).
    // The worker's queued entry is never written to the lease table. It's built in memory as the worker last saw the
    // lease before the eviction: owned by this worker ("leaseOwner"), with counter 1 (queuedShardEndLease()). So the
    // only owner on the SHARD_END lease is in the queued entry, and the checks that the lease is still unowned show
    // the cleanup didn't write that stale owner back.
    //
    // Each test runs one round of the worker's cleanup by calling cleanupLeases() directly. In production, start()
    // schedules it on the cleanup thread pool every leaseCleanupIntervalMillis; here start() is never called (the
    // thread pool is a mock), so nothing runs on its own. Inside cleanupLeases(), completed and garbage leases are
    // only checked once completedLeaseCleanupIntervalMillis and garbageLeaseCleanupIntervalMillis have passed; this
    // class sets both to 0, so the checks run right away.
    // =====================================================================================================

    /**
     * The leader already deleted the lease: the worker's cleanup drops its queued entry without writing anything.
     *
     * <p>The leader's deletion is set up by never creating the lease in the lease table (only its child leases
     * exist). The worker's queued entry still holds the lease as the worker last saw it. One cleanup round then reads
     * the lease from the lease table, finds it missing, and counts it as cleaned up ("Lease not present in lease
     * table"), so the entry is taken off the queue and not put back. That happens inside cleanupLeases();
     * isEnqueuedForDeletion() only checks whether the entry is still queued.
     */
    @Test
    public final void testQueuedLeaseAlreadyDeletedByLeader_droppedFromQueue() throws Exception {
        final LeaseRefresher realLeaseRefresher = createDynamoDbLocalLeaseRefresher();
        final LeasePendingDeletion queued = queuedShardEndLease();
        // only the child leases exist: the leader already deleted the SHARD_END lease
        createChildLeases(realLeaseRefresher, new ExtendedSequenceNumber("123"));

        leaseCleanupManager.enqueueForDeletion(queued);
        // one round of the worker's cleanup; this is where the entry is dropped
        leaseCleanupManager.cleanupLeases();

        Assert.assertFalse(leaseCleanupManager.isEnqueuedForDeletion(queued));
        // nothing wrote the lease back
        Assert.assertNull(realLeaseRefresher.getLease(SHARD_INFO.shardId()));
    }

    /**
     * The leader evicted the lease but hasn't deleted it yet, and it's ready to delete (both children have started):
     * the worker's cleanup deletes it.
     *
     * <p>The lease table has the evicted lease (at SHARD_END, no owner, with childShardIds) and both child leases
     * past their initial position. The worker's queued entry still holds the lease as the worker last saw it, owned
     * by the worker. One cleanup round reads the lease from the lease table, finds it present with childShardIds, and
     * applies the deletion rule, reading the children and parents from the lease table too: both children have
     * started and there are no parents, so it deletes the lease. The delete is by lease key only, so the queued
     * entry's stale owner doesn't matter. The entry counts as cleaned up and isn't put back on the queue.
     *
     * <p>If the leader deletes the lease too (both see it ready), that's harmless: deleteLease has no condition.
     */
    @Test
    public final void testQueuedLeaseEvictedByLeaderAndReady_deletedByWorker() throws Exception {
        final LeaseRefresher realLeaseRefresher = createDynamoDbLocalLeaseRefresher();
        // the lease as the leader left it, and both children started: ready to delete
        realLeaseRefresher.createLeaseIfNotExists(evictedShardEndLease());
        createChildLeases(realLeaseRefresher, new ExtendedSequenceNumber("123"));
        final LeasePendingDeletion queued = queuedShardEndLease();

        leaseCleanupManager.enqueueForDeletion(queued);
        // one round of the worker's cleanup; this is where the lease is deleted and the entry dropped
        leaseCleanupManager.cleanupLeases();

        Assert.assertNull(realLeaseRefresher.getLease(SHARD_INFO.shardId()));
        Assert.assertFalse(leaseCleanupManager.isEnqueuedForDeletion(queued));
        // only the SHARD_END lease is deleted, not its children
        for (ChildShard child : childShardsForSplit()) {
            Assert.assertNotNull(realLeaseRefresher.getLease(child.shardId()));
        }
    }

    /**
     * The leader evicted the lease and it isn't ready to delete (the children haven't started): the worker's cleanup
     * leaves it in the table untouched and keeps the entry queued for a later round.
     *
     * <p>The lease table has the evicted lease (at SHARD_END, no owner, with childShardIds) and both child leases
     * still at TRIM_HORIZON. One cleanup round reads the lease, finds it present with childShardIds, and applies the
     * deletion rule: the children haven't started, so it doesn't delete. The entry isn't cleaned up, so it's put back
     * on the queue.
     *
     * <p>The real checks are that the lease wasn't deleted and the entry is still queued. The test also checks the
     * lease is still unowned, which guards against the cleanup ever writing the queued entry's stale owner back. The
     * current cleanup can't: its only writes are the delete and recording childShardIds (only when the lease table
     * has none), and neither sets the owner.
     */
    @Test
    public final void testQueuedLeaseEvictedByLeaderNotReady_leftUnownedAndQueued() throws Exception {
        final LeaseRefresher realLeaseRefresher = createDynamoDbLocalLeaseRefresher();
        // the lease as the leader left it, and the children haven't started: not ready to delete
        realLeaseRefresher.createLeaseIfNotExists(evictedShardEndLease());
        createChildLeases(realLeaseRefresher, ExtendedSequenceNumber.TRIM_HORIZON);
        final LeasePendingDeletion queued = queuedShardEndLease();

        leaseCleanupManager.enqueueForDeletion(queued);
        // one round of the worker's cleanup; the entry isn't cleaned up, so it's put back on the queue
        leaseCleanupManager.cleanupLeases();

        assertEvictedLeaseNotDeletedAndStillUnowned(realLeaseRefresher);
        Assert.assertTrue(leaseCleanupManager.isEnqueuedForDeletion(queued));
    }

    /**
     * With cleanupLeasesUponShardCompletion off, the worker's cleanup never deletes the evicted lease, even when it's
     * ready, and leaves it untouched. The entry stays queued, as it always did with cleanup off.
     *
     * <p>The lease table has the evicted lease and both child leases past their initial position, so the lease
     * would be ready. With the flag off, a cleanup round skips the completed-lease check entirely (it doesn't even
     * read the lease). The garbage-lease check still runs: it asks Kinesis for the shard's children (the mocked
     * ShardDetector returns none, without ResourceNotFound), so the shard isn't treated as gone and the lease isn't
     * deleted. The entry isn't cleaned up, so it's put back on the queue.
     *
     * <p>The real check is that the lease wasn't deleted even though it's ready, so the flag is respected (compare
     * the previous test, where the lease isn't deleted because it isn't ready). Like that test, it also checks the
     * lease is still unowned.
     */
    @Test
    public final void testQueuedLeaseEvictedByLeaderWithCleanupOff_leftUnownedAndQueued() throws Exception {
        // the same setup as the class's LeaseCleanupManager, but with cleanupLeasesUponShardCompletion off
        leaseCleanupManager = new LeaseCleanupManager(
                leaseCoordinator,
                NULL_METRICS_FACTORY,
                deletionThreadPool,
                false,
                leaseCleanupIntervalMillis,
                completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis);
        final LeaseRefresher realLeaseRefresher = createDynamoDbLocalLeaseRefresher();
        // the lease as the leader left it, and both children started: it would be ready with cleanup on
        realLeaseRefresher.createLeaseIfNotExists(evictedShardEndLease());
        createChildLeases(realLeaseRefresher, new ExtendedSequenceNumber("123"));
        final LeasePendingDeletion queued = queuedShardEndLease();

        leaseCleanupManager.enqueueForDeletion(queued);
        // one round of the worker's cleanup; the entry isn't cleaned up, so it's put back on the queue
        leaseCleanupManager.cleanupLeases();

        assertEvictedLeaseNotDeletedAndStillUnowned(realLeaseRefresher);
        Assert.assertTrue(leaseCleanupManager.isEnqueuedForDeletion(queued));
    }

    /** A lease refresher on a fresh DynamoDB Local table, used by this test's LeaseCoordinator. */
    private LeaseRefresher createDynamoDbLocalLeaseRefresher() throws Exception {
        final DynamoDbAsyncClient dynamoDbAsyncClient =
                DynamoDBEmbedded.create().dynamoDbAsyncClient();
        final LeaseRefresher realLeaseRefresher = new DynamoDBLeaseRefresher(
                "leaseTable",
                dynamoDbAsyncClient,
                new DynamoDBLeaseSerializer(),
                true,
                TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK,
                LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
                new DdbTableConfig(),
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
                DefaultSdkAutoConstructList.getInstance());
        realLeaseRefresher.createLeaseTableIfNotExists();
        when(leaseCoordinator.leaseRefresher()).thenReturn(realLeaseRefresher);
        return realLeaseRefresher;
    }

    /**
     * The SHARD_END lease as the leader leaves it after evicting it, which is what the evicted-lease tests write to
     * the lease table: at SHARD_END, with childShardIds, no leaseOwner, and counter 2 (eviction increments it).
     *
     * <p>Its childShardIds come from childShardsForSplit(), the same list {@link #createChildLeases} uses for the
     * child lease keys. They have to match: the deletion rule reads these childShardIds and looks up each child's
     * lease by that key, and a missing child lease makes it throw instead of deleting.
     */
    private Lease evictedShardEndLease() {
        final Lease lease = LeaseHelper.createLease(
                SHARD_INFO.shardId(),
                null,
                Collections.emptyList(),
                childShardsForSplit().stream().map(ChildShard::shardId).collect(Collectors.toSet()),
                ExtendedSequenceNumber.SHARD_END);
        lease.leaseCounter(2L);
        return lease;
    }

    /**
     * The worker's queued entry: the same lease as the worker last saw it, before the eviction, so owned by this
     * worker ("leaseOwner") with counter 1. Built in memory only; it's never written to the lease table.
     */
    private LeasePendingDeletion queuedShardEndLease() {
        final Lease lease = evictedShardEndLease();
        lease.leaseOwner("leaseOwner");
        lease.leaseCounter(1L);
        return createLeasePendingDeletion(lease, SHARD_INFO);
    }

    /**
     * Writes the two split children's leases to the lease table, owned by another worker ("otherWorker"), at
     * {@code checkpoint}: past their initial position means they've started processing, TRIM_HORIZON means they
     * haven't.
     *
     * <p>Their lease keys come from childShardsForSplit(), the same list {@link #evictedShardEndLease} uses for the
     * SHARD_END lease's childShardIds, so the deletion rule finds them. Each child also names the SHARD_END lease as
     * its parent, as a real child lease would: its parentShardIds is {@code SHARD_INFO.shardId()}, the same shard ID
     * {@link #evictedShardEndLease} uses as the SHARD_END lease's key. The cleanup doesn't read that, though: it goes
     * from the SHARD_END lease's childShardIds to the children, not the other way.
     */
    private void createChildLeases(final LeaseRefresher realLeaseRefresher, final ExtendedSequenceNumber checkpoint)
            throws Exception {
        for (ChildShard child : childShardsForSplit()) {
            realLeaseRefresher.createLeaseIfNotExists(LeaseHelper.createLease(
                    child.shardId(),
                    "otherWorker",
                    Collections.singleton(SHARD_INFO.shardId()),
                    Collections.emptyList(),
                    checkpoint));
        }
    }

    /**
     * The evicted lease is still in the lease table (not deleted), and still unowned (the queued entry's stale owner
     * wasn't written back).
     */
    private void assertEvictedLeaseNotDeletedAndStillUnowned(final LeaseRefresher realLeaseRefresher) throws Exception {
        final Lease lease = realLeaseRefresher.getLease(SHARD_INFO.shardId());
        Assert.assertNotNull("not deleted", lease);
        Assert.assertNull("still unowned", lease.leaseOwner());
    }

    private void verifyExpectedDeletedLeasesCompletedShardCase(
            ShardInfo shardInfo,
            List<ChildShard> childShards,
            ExtendedSequenceNumber extendedSequenceNumber,
            int expectedDeletedLeases)
            throws Exception {
        verifyExpectedDeletedLeasesCompletedShardCase(
                shardInfo, childShards, extendedSequenceNumber, true, expectedDeletedLeases);
    }

    private void verifyExpectedDeletedLeasesCompletedShardCase(
            ShardInfo shardInfo,
            List<ChildShard> childShards,
            ExtendedSequenceNumber extendedSequenceNumber,
            boolean childShardLeasesPresent,
            int expectedDeletedLeases)
            throws Exception {
        final Lease lease = LeaseHelper.createLease(
                shardInfo.shardId(),
                "leaseOwner",
                shardInfo.parentShardIds(),
                childShards.stream().map(ChildShard::shardId).collect(Collectors.toSet()));
        final List<Lease> childShardLeases = childShards.stream()
                .map(c -> LeaseHelper.createLease(
                        ShardInfo.getLeaseKey(shardInfo, c.shardId()),
                        "leaseOwner",
                        Collections.singleton(shardInfo.shardId()),
                        Collections.emptyList(),
                        extendedSequenceNumber))
                .collect(Collectors.toList());

        final List<Lease> parentShardLeases = lease.parentShardIds().stream()
                .map(p -> LeaseHelper.createLease(
                        ShardInfo.getLeaseKey(shardInfo, p),
                        "leaseOwner",
                        Collections.emptyList(),
                        Collections.singleton(shardInfo.shardId()),
                        extendedSequenceNumber))
                .collect(Collectors.toList());

        when(leaseRefresher.getLease(lease.leaseKey())).thenReturn(lease);
        for (Lease parentShardLease : parentShardLeases) {
            when(leaseRefresher.getLease(parentShardLease.leaseKey())).thenReturn(parentShardLease);
        }
        if (childShardLeasesPresent) {
            for (Lease childShardLease : childShardLeases) {
                when(leaseRefresher.getLease(childShardLease.leaseKey())).thenReturn(childShardLease);
            }
        }

        leaseCleanupManager.enqueueForDeletion(createLeasePendingDeletion(lease, shardInfo));
        leaseCleanupManager.cleanupLeases();

        verify(shardDetector).getChildShards(shardInfo.shardId());
        verify(leaseRefresher, times(expectedDeletedLeases)).deleteLease(any(Lease.class));
    }

    private List<ChildShard> childShardsForSplit() {
        final List<String> parentShards = Collections.singletonList("splitParent");

        ChildShard leftChild = ChildShard.builder()
                .shardId("leftChild")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"))
                .build();
        ChildShard rightChild = ChildShard.builder()
                .shardId("rightChild")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("50", "99"))
                .build();

        return Arrays.asList(leftChild, rightChild);
    }

    private List<ChildShard> childShardsForMerge() {
        final List<String> parentShards = Arrays.asList("mergeParent1", "mergeParent2");

        ChildShard child = ChildShard.builder()
                .shardId("onlyChild")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "99"))
                .build();

        return Collections.singletonList(child);
    }

    private LeasePendingDeletion createLeasePendingDeletion(final Lease lease, final ShardInfo shardInfo) {
        return new LeasePendingDeletion(STREAM_IDENTIFIER, lease, shardInfo, shardDetector);
    }
}
