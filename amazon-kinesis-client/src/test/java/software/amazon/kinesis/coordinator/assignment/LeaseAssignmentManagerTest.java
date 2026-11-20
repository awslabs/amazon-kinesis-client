package software.amazon.kinesis.coordinator.assignment;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.coordinator.delegate.LeaseTableCoordinatorStateDAODelegate;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatus;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatusProvider;
import software.amazon.kinesis.coordinator.streamInfo.StreamIdCacheManager;
import software.amazon.kinesis.leases.EntityDAO;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseAssignmentStrategy;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseTableDao;
import software.amazon.kinesis.leases.dynamodb.DynamoDBMultiStreamLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats.LeaseTableWorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;
import software.amazon.kinesis.worker.metricstats.delegate.LegacyTableWorkerMetricStatsDAODelegate;

import static java.util.Objects.nonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber.SHARD_END;
import static software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber.TRIM_HORIZON;

class LeaseAssignmentManagerTest {
    @Mock
    StreamIdCacheManager streamIdCacheManager;

    private static final String TEST_LEADER_WORKER_ID = "workerId";
    private static final String TEST_TAKE_WORKER_ID = "workerIdTake";
    private static final String TEST_YIELD_WORKER_ID = "workerIdYield";

    private static final String LEASE_TABLE_NAME = "leaseTable";
    private static final String WORKER_METRICS_TABLE_NAME = "workerMetrics";
    private final DynamoDbAsyncClient dynamoDbAsyncClient =
            DynamoDBEmbedded.create().dynamoDbAsyncClient();
    private LeaseManagementConfig.GracefulLeaseHandoffConfig gracefulLeaseHandoffConfig =
            LeaseManagementConfig.GracefulLeaseHandoffConfig.builder()
                    .isGracefulLeaseHandoffEnabled(false)
                    .build();
    // TODO : Use DynamoDBLockBasedLeaderDecider with LocalDDBClient.
    private LeaderDecider mockLeaderDecider;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<Void> scheduledFuture;
    private Runnable leaseAssignmentManagerRunnable;
    private final LeaseSerializer leaseSerializer = new DynamoDBLeaseSerializer();
    private final LeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
            LEASE_TABLE_NAME,
            dynamoDbAsyncClient,
            leaseSerializer,
            true,
            TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK,
            LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
            new DdbTableConfig(),
            LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
            LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
            DefaultSdkAutoConstructList.getInstance());
    private final EntityDAO entityDAO = new DynamoDBLeaseTableDao(
            dynamoDbAsyncClient,
            LEASE_TABLE_NAME,
            leaseSerializer,
            new LeaseTableCoordinatorStateDAODelegate(
                    dynamoDbAsyncClient, LEASE_TABLE_NAME, LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT),
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()),
            Runtime.getRuntime().availableProcessors());
    private WorkerMetricStatsDAO workerMetricsDAO;
    private TableMigrationStatusProvider mockProvider;
    /**
     * The LeaseCleanupManager given to every LeaseAssignmentManager the helpers create. A real one backed by this
     * test's lease table by default; tests that need a different one reassign it before creating the LAM.
     */
    private LeaseCleanupManager leaseCleanupManager;

    @BeforeEach
    void setup() throws ProvisionedThroughputException, DependencyException {
        leaseRefresher.createLeaseTableIfNotExists();
        leaseCleanupManager = createLeaseCleanupManager(true);

        final WorkerMetricsTableConfig config = new WorkerMetricsTableConfig("applicationName");
        config.tableName(WORKER_METRICS_TABLE_NAME);
        mockProvider = mock(TableMigrationStatusProvider.class);
        when(mockProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        workerMetricsDAO =
                new WorkerMetricStatsDAO(dynamoDbAsyncClient, config, LEASE_TABLE_NAME, 10000L, mockProvider);
        workerMetricsDAO.initialize();
        mockLeaderDecider = Mockito.mock(LeaderDecider.class);
        scheduledExecutorService = Mockito.mock(ScheduledExecutorService.class);
        scheduledFuture = Mockito.mock(ScheduledFuture.class);
        when(mockLeaderDecider.isLeader(any())).thenReturn(true);
        when(scheduledExecutorService.scheduleWithFixedDelay(
                        any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    Object[] args = invocation.getArguments();
                    this.leaseAssignmentManagerRunnable = (Runnable) args[0];
                    return scheduledFuture;
                });
        when(scheduledFuture.cancel(anyBoolean())).thenReturn(true);
    }

    @Test
    void performAssignment_yieldAndTakeWorker_validateNewLeaseAssignedToTakeWorker() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("something"));

        leaseAssignmentManagerRunnable.run();

        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.listLeases().get(0).leaseOwner());
    }

    @Test
    void performAssignment_workerWithFailingWorkerMetric_assertLeaseNotAssignedToWorkerWithFailingWorkerMetric()
            throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);
        final String workerWithFailingWorkerMetricId = "WorkerIdOfFailingWorkerMetric";

        workerMetricsDAO.updateMetrics(createWorkerWithFailingWorkerMetric(workerWithFailingWorkerMetricId));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("something1"));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("something2"));

        leaseAssignmentManagerRunnable.run();
        assertEquals(
                0,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> workerWithFailingWorkerMetricId.equals(lease.leaseOwner()))
                        .collect(Collectors.toSet())
                        .size());
    }

    @Test
    void performAssignment_workerWithFailingWorkerMetricInPast_assertLeaseAssignment() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);
        final String workerWithFailingWorkerMetricId = "WorkerIdOfFailingWorkerMetric";

        workerMetricsDAO.updateMetrics(createWorkerWithFailingWorkerMetricInPast(workerWithFailingWorkerMetricId));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("something1"));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("something2"));

        leaseAssignmentManagerRunnable.run();
        assertEquals(
                2,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> workerWithFailingWorkerMetricId.equals(lease.leaseOwner()))
                        .collect(Collectors.toSet())
                        .size());
    }

    @Test
    void performAssignment_noThroughputToWorker_assertOneLeaseTaken() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        // 10 leases are assigned to yield worker, all have zero throughput
        for (int i = 0; i < 10; ++i) {
            final Lease lease = createDummyLease("lease" + i, TEST_YIELD_WORKER_ID);
            lease.throughputKBps(0D);
            populateLeasesInLeaseTable(lease);
        }

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                9,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> TEST_YIELD_WORKER_ID.equals(lease.leaseOwner()))
                        .collect(Collectors.toSet())
                        .size());
        // Only 1 lease is expected to be taken as during zero throughput we fall back to taking 1 lease.
        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> TEST_TAKE_WORKER_ID.equals(lease.leaseOwner()))
                        .collect(Collectors.toSet())
                        .size());
    }

    @Test
    void performAssignment_moreLeasesThanMaxConfigured_assertSomeUnassignedLeases()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20);
        createLeaseAssignmentManager(config, 100L, System::nanoTime, 2);

        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("lease1"));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("lease2"));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("lease3"));

        leaseAssignmentManagerRunnable.run();
        assertEquals(
                2L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> nonNull(lease.leaseOwner()))
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());
    }

    /**
     * Shard tree: "parent" splits into "left" and "right", which merge back into "merged". The SHARD_END leases
     * must be deleted parents first (each deletion check needs the lease's children to still exist), and the
     * lease table scan order is arbitrary, so this checks the whole chain still clears in a single LAM run.
     */
    @Test
    void performAssignment_shardEndChainReadyForDeletion_assertWholeChainDeletedInOneRun() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20), 100L, System::nanoTime, 2);
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        populateLeasesInLeaseTable(
                createShardTreeLease("parent", SHARD_END, Collections.emptyList(), Arrays.asList("left", "right")),
                createShardTreeLease(
                        "left", SHARD_END, Collections.singletonList("parent"), Collections.singletonList("merged")),
                createShardTreeLease(
                        "right", SHARD_END, Collections.singletonList("parent"), Collections.singletonList("merged")),
                // the merge child has started processing, so every SHARD_END lease above it is ready
                createShardTreeLease(
                        "merged",
                        new ExtendedSequenceNumber("123"),
                        Arrays.asList("left", "right"),
                        Collections.emptyList()));

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                Collections.singletonList("merged"),
                leaseRefresher.listLeases().stream().map(Lease::leaseKey).collect(Collectors.toList()));
    }

    /**
     * Same shard tree, but the merge child hasn't been picked up yet. "parent" is deleted (both its children have
     * reached SHARD_END); "left" and "right" can't be deleted yet, so they're evicted: they stop holding a slot on
     * their worker and aren't reassigned, which leaves room under maxLeasesForWorker for "merged".
     */
    @Test
    void performAssignment_shardEndLeasesNotReadyForDeletion_assertEvictedAndNotCountedOrAssigned() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20), 100L, System::nanoTime, 1);
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final Lease merged = createDummyUnAssignedLease("merged");
        merged.parentShardIds(Arrays.asList("left", "right"));
        populateLeasesInLeaseTable(
                createShardTreeLease("parent", SHARD_END, Collections.emptyList(), Arrays.asList("left", "right")),
                createShardTreeLease(
                        "left", SHARD_END, Collections.singletonList("parent"), Collections.singletonList("merged")),
                createShardTreeLease(
                        "right", SHARD_END, Collections.singletonList("parent"), Collections.singletonList("merged")),
                merged);

        leaseAssignmentManagerRunnable.run();

        assertNull(leaseRefresher.getLease("parent"));
        assertNull(leaseRefresher.getLease("left").leaseOwner());
        assertNull(leaseRefresher.getLease("right").leaseOwner());
        // maxLeasesForWorker is 1, and the SHARD_END leases the worker held don't count toward it
        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.getLease("merged").leaseOwner());
    }

    /**
     * Evicted SHARD_END leases are never offered for assignment again, even with capacity to spare: they stay
     * unowned across runs until they're ready to delete.
     */
    @Test
    void performAssignment_evictedShardEndLeases_assertNotReassigned() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        // "child" hasn't started, so "ended" isn't ready to delete
        final Lease child = createDummyUnAssignedLease("child");
        child.parentShardIds(Collections.singletonList("ended"));
        populateLeasesInLeaseTable(
                createShardTreeLease("ended", SHARD_END, Collections.emptyList(), Collections.singletonList("child")),
                child);

        leaseAssignmentManagerRunnable.run();
        leaseAssignmentManagerRunnable.run();

        assertNull(leaseRefresher.getLease("ended").leaseOwner());
        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.getLease("child").leaseOwner());
    }

    /**
     * A SHARD_END lease without childShardIds (e.g. the stream was deleted, so the shard ended with
     * ResourceNotFound and no children were recorded) is left to the owning worker's LeaseCleanupManager: it stays
     * owned and still counts toward maxLeasesForWorker, so the worker has no room for another lease.
     */
    @Test
    void performAssignment_shardEndLeaseWithoutChildShardIds_assertLeftOwnedAndCounted() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20), 100L, System::nanoTime, 1);
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        populateLeasesInLeaseTable(
                createShardTreeLease("ended", SHARD_END, Collections.emptyList(), Collections.emptyList()),
                createDummyUnAssignedLease("waiting"));

        leaseAssignmentManagerRunnable.run();

        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.getLease("ended").leaseOwner());
        assertNull(leaseRefresher.getLease("waiting").leaseOwner());
    }

    /**
     * With cleanupLeasesUponShardCompletion off, SHARD_END leases are never deleted, even when ready, but they're
     * still evicted and not counted toward maxLeasesForWorker.
     */
    @Test
    void performAssignment_cleanupLeasesUponShardCompletionOff_assertShardEndLeasesEvictedNotDeleted()
            throws Exception {
        leaseCleanupManager = createLeaseCleanupManager(false);
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20), 100L, System::nanoTime, 2);
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        populateLeasesInLeaseTable(
                createShardTreeLease("parent", SHARD_END, Collections.emptyList(), Arrays.asList("left", "right")),
                createShardTreeLease(
                        "left", SHARD_END, Collections.singletonList("parent"), Collections.singletonList("merged")),
                createShardTreeLease(
                        "right", SHARD_END, Collections.singletonList("parent"), Collections.singletonList("merged")),
                createShardTreeLease(
                        "merged",
                        new ExtendedSequenceNumber("123"),
                        Arrays.asList("left", "right"),
                        Collections.emptyList()),
                createDummyUnAssignedLease("waiting"));

        leaseAssignmentManagerRunnable.run();

        for (String shardEndLeaseKey : Arrays.asList("parent", "left", "right")) {
            assertNull(leaseRefresher.getLease(shardEndLeaseKey).leaseOwner(), shardEndLeaseKey + " evicted");
        }
        // maxLeasesForWorker is 2: "merged" and "waiting", with the SHARD_END leases not counted
        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.getLease("waiting").leaseOwner());
    }

    /**
     * At most MAX_SHARD_END_LEASE_DELETIONS_PER_RUN leases are deleted per run; a ready lease past the limit is
     * evicted and deleted on a later run.
     */
    @Test
    void performAssignment_moreReadyShardEndLeasesThanPerRunLimit_assertLimitDeletedAndRestOnNextRun()
            throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);
        final int readyLeases = LeaseAssignmentManager.MAX_SHARD_END_LEASE_DELETIONS_PER_RUN + 1;
        for (int i = 0; i < readyLeases; i++) {
            populateLeasesInLeaseTable(
                    createShardTreeLease(
                            "ended" + i, SHARD_END, Collections.emptyList(), Collections.singletonList("child" + i)),
                    createShardTreeLease(
                            "child" + i,
                            new ExtendedSequenceNumber("123"),
                            Collections.singletonList("ended" + i),
                            Collections.emptyList()));
        }

        leaseAssignmentManagerRunnable.run();

        final List<Lease> shardEndLeasesLeft = listShardEndLeases();
        assertEquals(1, shardEndLeasesLeft.size());
        assertNull(shardEndLeasesLeft.get(0).leaseOwner(), "the lease past the limit is evicted");

        leaseAssignmentManagerRunnable.run();

        assertTrue(listShardEndLeases().isEmpty());
    }

    /**
     * SHARD_END leases past the per-run deletion limit are never assigned, whether they're unassigned or expired:
     * they're handled (here, deleted) on the next run straight from the lease table, without a worker having to take
     * them first.
     *
     * <p>Every SHARD_END lease here is ready to delete. The unassigned ones sort first (lastCounterIncrementNanos 0),
     * so the limit is used up on them, and two unassigned leases plus "expiredEnded" are left over. "expiredEnded" is
     * owned by a worker with no worker metrics, and its first eviction fails, so it's still owned after run 1 and
     * expired by run 2 (its lease counter doesn't change while the clock moves past the lease duration). "waiting" is
     * an ordinary unassigned lease, there to show the take worker is accepting assignments in both runs.
     */
    @Test
    void performAssignment_shardEndLeasesPastPerRunLimit_assertUnassignedAndExpiredNotAssigned() throws Exception {
        final AtomicLong nanoTime = new AtomicLong(Duration.ofSeconds(1).toNanos());
        final LeaseRefresher evictionFailingOnceLeaseRefresher = Mockito.spy(leaseRefresher);
        doThrow(new DependencyException(new RuntimeException("test")))
                .doCallRealMethod()
                .when(evictionFailingOnceLeaseRefresher)
                .evictLease(any());
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                nanoTime::get,
                Integer.MAX_VALUE,
                evictionFailingOnceLeaseRefresher,
                workerMetricsDAO);
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final int unassignedReadyLeases = LeaseAssignmentManager.MAX_SHARD_END_LEASE_DELETIONS_PER_RUN + 2;
        for (int i = 0; i < unassignedReadyLeases; i++) {
            final Lease ended = createShardTreeLease(
                    "unassignedEnded" + i, SHARD_END, Collections.emptyList(), Collections.singletonList("child" + i));
            ended.leaseOwner(null);
            populateLeasesInLeaseTable(ended, startedChild("child" + i, "unassignedEnded" + i));
        }
        final Lease expiredEnded = createShardTreeLease(
                "expiredEnded", SHARD_END, Collections.emptyList(), Collections.singletonList("expiredChild"));
        expiredEnded.leaseOwner("deadWorker");
        populateLeasesInLeaseTable(
                expiredEnded, startedChild("expiredChild", "expiredEnded"), createDummyUnAssignedLease("waiting"));

        leaseAssignmentManagerRunnable.run();

        final List<Lease> shardEndLeasesLeft = listShardEndLeases();
        assertEquals(3, shardEndLeasesLeft.size(), "two unassigned leases and expiredEnded are past the limit");
        assertEquals("deadWorker", leaseRefresher.getLease("expiredEnded").leaseOwner(), "eviction failed");
        assertEquals(
                2,
                shardEndLeasesLeft.stream()
                        .filter(lease -> lease.leaseOwner() == null)
                        .count(),
                "the unassigned leases past the limit are still unassigned");
        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.getLease("waiting").leaseOwner());

        nanoTime.addAndGet(Duration.ofSeconds(1).toNanos());
        leaseAssignmentManagerRunnable.run();

        assertTrue(listShardEndLeases().isEmpty(), "deleted on the next run without being assigned first");
    }

    /** A child lease owned by the take worker that has started processing, so its parent is ready to delete. */
    private Lease startedChild(final String leaseKey, final String parentLeaseKey) {
        return createShardTreeLease(
                leaseKey,
                new ExtendedSequenceNumber("123"),
                Collections.singletonList(parentLeaseKey),
                Collections.emptyList());
    }
    /**
     * A SHARD_END lease whose child lease is missing from the lease table isn't deleted (the deletion check needs
     * the child lease), but it's still evicted.
     */
    @Test
    void performAssignment_shardEndLeaseWithMissingChildLease_assertNotDeletedButEvicted() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);

        populateLeasesInLeaseTable(createShardTreeLease(
                "ended", SHARD_END, Collections.emptyList(), Collections.singletonList("missingChild")));

        leaseAssignmentManagerRunnable.run();

        assertNull(leaseRefresher.getLease("ended").leaseOwner());
    }

    /** If deleting a SHARD_END lease fails, it's evicted instead, and the rest of the run (assignment) still happens. */
    @Test
    void performAssignment_shardEndLeaseDeletionFails_assertEvictedAndOtherLeasesAssigned() throws Exception {
        leaseCleanupManager = mock(LeaseCleanupManager.class);
        when(leaseCleanupManager.deleteCompletedShardLeaseIfReady(any(), any(), any(), any()))
                .thenThrow(new DependencyException(new RuntimeException("test")));
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20), 100L, System::nanoTime, 2);
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        populateLeasesInLeaseTable(
                createShardTreeLease("ended", SHARD_END, Collections.emptyList(), Collections.singletonList("child")),
                createShardTreeLease(
                        "child",
                        new ExtendedSequenceNumber("123"),
                        Collections.singletonList("ended"),
                        Collections.emptyList()),
                createDummyUnAssignedLease("waiting"));

        leaseAssignmentManagerRunnable.run();

        assertNull(leaseRefresher.getLease("ended").leaseOwner());
        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.getLease("waiting").leaseOwner());
    }

    /**
     * If evicting a SHARD_END lease fails (throws, or returns false because the lease changed since the scan), the
     * run continues, and the lease still doesn't count toward maxLeasesForWorker.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void performAssignment_shardEndLeaseEvictionFails_assertNotCountedAndChildAssigned(final boolean evictionThrows)
            throws Exception {
        final LeaseRefresher evictionFailingLeaseRefresher = Mockito.spy(leaseRefresher);
        if (evictionThrows) {
            doThrow(new DependencyException(new RuntimeException("test")))
                    .when(evictionFailingLeaseRefresher)
                    .evictLease(any());
        } else {
            doReturn(false).when(evictionFailingLeaseRefresher).evictLease(any());
        }
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                1,
                evictionFailingLeaseRefresher,
                workerMetricsDAO);
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        // "child" hasn't started, so "ended" isn't ready to delete and the leader tries to evict it
        final Lease child = createDummyUnAssignedLease("child");
        child.parentShardIds(Collections.singletonList("ended"));
        populateLeasesInLeaseTable(
                createShardTreeLease("ended", SHARD_END, Collections.emptyList(), Collections.singletonList("child")),
                child);

        leaseAssignmentManagerRunnable.run();

        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.getLease("ended").leaseOwner(), "eviction failed");
        // maxLeasesForWorker is 1, and the worker still owns "ended", which doesn't count
        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.getLease("child").leaseOwner());
    }

    /**
     * In multi-stream mode, a SHARD_END lease whose child has started is deleted by the leader, through the same
     * code path a multi-stream application uses.
     *
     * <p>In multi-stream mode, a lease key is {@code <streamIdentifier>:<shardId>}, but a lease's
     * {@code childShardIds} and {@code parentShardIds} hold bare shard IDs with no stream. To find a child's lease,
     * the deletion check builds the key with {@code ShardInfo.getLeaseKey(shardInfo, childShardId)}, which adds the
     * stream prefix only if the ShardInfo carries a stream identifier. So the leader has to pass the deletion check
     * a ShardInfo that carries the lease's stream, and a lease lookup keyed by the full multi-stream lease key.
     *
     * <p>Two streams with the same shard IDs, so the test catches either mistake:
     * <ul>
     *   <li>If only one side drops the stream (the ShardInfo, or the lookup's keys), the bare and qualified keys
     *       never match, the check finds no child lease, and the SHARD_END lease is evicted instead of deleted.
     *       Stream A's parent would then still be in the table.</li>
     *   <li>If both sides drop the stream, the keys match again, but the two streams' leases share the same bare
     *       keys, so one stream's parent is checked against the other stream's child. Stream A's child has started
     *       and stream B's hasn't, so either A's parent isn't deleted or B's parent is deleted too early.</li>
     * </ul>
     *
     * <p>The rest of this class reads and writes the lease table with {@link DynamoDBLeaseSerializer}, which
     * doesn't store a lease's stream identifier. This test uses a second lease refresher and DAO on the same table
     * with {@link DynamoDBMultiStreamLeaseSerializer}, which is what the Scheduler picks in multi-stream mode. So the
     * LAM's lease table scan returns MultiStreamLease objects, and the real LeaseCleanupManager deletes through the
     * same refresher.
     *
     * <p>Setup: in each of streams A and B, "shardId-000000000000" is at SHARD_END and its child is
     * "shardId-000000000001". In stream A the child has started processing (checkpoint "123"), so A's parent is ready
     * to delete. In stream B the child is still at TRIM_HORIZON, so B's parent isn't. After one LAM run, A's parent
     * lease is gone, and B's parent lease and both child leases are still there.
     */
    @Test
    void performAssignment_multiStreamShardEndLease_assertDeletedUsingStreamQualifiedLeaseKeys() throws Exception {
        final LeaseSerializer multiStreamLeaseSerializer = new DynamoDBMultiStreamLeaseSerializer();
        final LeaseRefresher multiStreamLeaseRefresher = new DynamoDBLeaseRefresher(
                LEASE_TABLE_NAME,
                dynamoDbAsyncClient,
                multiStreamLeaseSerializer,
                true,
                TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK,
                LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
                new DdbTableConfig(),
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
                DefaultSdkAutoConstructList.getInstance());
        final EntityDAO multiStreamEntityDAO = new DynamoDBLeaseTableDao(
                dynamoDbAsyncClient,
                LEASE_TABLE_NAME,
                multiStreamLeaseSerializer,
                new LeaseTableCoordinatorStateDAODelegate(
                        dynamoDbAsyncClient, LEASE_TABLE_NAME, LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT),
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()),
                Runtime.getRuntime().availableProcessors());
        leaseCleanupManager = createLeaseCleanupManager(multiStreamLeaseRefresher, true);
        createLeaseAssignmentManagerWithStrategy(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE,
                multiStreamLeaseRefresher,
                multiStreamEntityDAO,
                workerMetricsDAO,
                LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE);

        final String streamA = "123456789012:multiStreamTestA:1";
        final String streamB = "123456789012:multiStreamTestB:1";
        final MultiStreamLease parentA = multiStreamParent(streamA);
        final MultiStreamLease childA = multiStreamChild(streamA, new ExtendedSequenceNumber("123"));
        final MultiStreamLease parentB = multiStreamParent(streamB);
        final MultiStreamLease childB = multiStreamChild(streamB, TRIM_HORIZON);
        for (MultiStreamLease lease : Arrays.asList(parentA, childA, parentB, childB)) {
            multiStreamLeaseRefresher.createLeaseIfNotExists(lease);
        }

        leaseAssignmentManagerRunnable.run();

        assertNull(multiStreamLeaseRefresher.getLease(parentA.leaseKey()), "stream A's parent is deleted");
        for (MultiStreamLease lease : Arrays.asList(childA, parentB, childB)) {
            assertEquals(
                    lease.leaseKey(),
                    Optional.ofNullable(multiStreamLeaseRefresher.getLease(lease.leaseKey()))
                            .map(Lease::leaseKey)
                            .orElse(null),
                    lease.leaseKey() + " is still in the lease table");
        }
    }

    /** "shardId-000000000000" at SHARD_END in the given stream, with child "shardId-000000000001". */
    private static MultiStreamLease multiStreamParent(final String streamIdentifier) {
        return createMultiStreamShardTreeLease(
                streamIdentifier,
                "shardId-000000000000",
                SHARD_END,
                Collections.emptyList(),
                Collections.singletonList("shardId-000000000001"));
    }

    /** "shardId-000000000001" in the given stream, with parent "shardId-000000000000". */
    private static MultiStreamLease multiStreamChild(
            final String streamIdentifier, final ExtendedSequenceNumber checkpoint) {
        return createMultiStreamShardTreeLease(
                streamIdentifier,
                "shardId-000000000001",
                checkpoint,
                Collections.singletonList("shardId-000000000000"),
                Collections.emptyList());
    }

    @Test
    void performAssignment_unAssignedAndExpiredLeasesBothAvailable_validateUnAssignedLeaseAssignedFirst()
            throws Exception {

        final Supplier<Long> mockNanoTimeProvider = Mockito.mock(Supplier.class);
        when(mockNanoTimeProvider.get())
                .thenReturn(Duration.ofMillis(100).toNanos())
                .thenReturn(Duration.ofMillis(110).toNanos());
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(9D, 20), 1L, mockNanoTimeProvider, Integer.MAX_VALUE);

        leaseRefresher.createLeaseIfNotExists(createDummyLease("expiredLease", "random-owner"));

        // No assignment will happen as no workers not existing, but will start tracking lease for expiry
        leaseAssignmentManagerRunnable.run();

        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("unAssignedLease"));

        leaseAssignmentManagerRunnable.run();

        assertNotEquals(
                TEST_TAKE_WORKER_ID, leaseRefresher.getLease("expiredLease").leaseOwner());
        assertEquals(
                TEST_TAKE_WORKER_ID, leaseRefresher.getLease("unAssignedLease").leaseOwner());
    }

    @Test
    void performAssignment_workerNotAboveReBalanceThresholdButAboveOperatingRange_asserReBalance() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("Worker1", 41, 50));
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("Worker2", 59, 50));

        final Lease lease1 = createDummyLease("lease1", "Worker2");
        lease1.throughputKBps(1000);
        final Lease lease2 = createDummyLease("lease2", "Worker2");
        lease2.throughputKBps(1);
        populateLeasesInLeaseTable(lease1, lease2);

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals("Worker1"))
                        .count());
    }

    @Test
    void performAssignment_inActiveWorkerWithLowUtilizationAvailable_verifyLeaseNotAssigned()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final String inActiveWorkerId = "InActiveWorker";
        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        // workerMetricsDAO has validation to allow expired WorkerMetricStats writes, so write directly to table
        // for test
        writeToWorkerMetricsTables(createInActiveWorkerWithNoUtilization(inActiveWorkerId));

        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(9D, 20), 1L, System::nanoTime, Integer.MAX_VALUE);

        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("leaseKey1"));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("leaseKey2"));

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                2L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_YIELD_WORKER_ID))
                        .count());
        assertEquals(
                0L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(inActiveWorkerId))
                        .count());
    }

    @Test
    void performAssignment_takeAndYieldWorkers_verifyThroughoutTaken() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10),
                Duration.ofHours(1).toMillis(),
                System::nanoTime,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final Lease lease1 = createDummyLease("lease1", TEST_YIELD_WORKER_ID);
        lease1.throughputKBps(100D);
        final Lease lease2 = createDummyLease("lease2", TEST_YIELD_WORKER_ID);
        lease2.throughputKBps(200D);
        final Lease lease3 = createDummyLease("lease3", TEST_YIELD_WORKER_ID);
        lease3.throughputKBps(300D);
        final Lease lease4 = createDummyLease("lease4", TEST_YIELD_WORKER_ID);
        lease4.throughputKBps(400D);

        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4);

        leaseAssignmentManagerRunnable.run();

        // Yield worker has total of 1000KBps throughput assigned, based on the average will take around 17% of
        // throughput
        assertTrue(leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .mapToDouble(Lease::throughputKBps)
                        .sum()
                >= 100D);
    }

    @Test
    void performAssignment_takeOvershootingLease_verifySmallestLeaseTaken() throws Exception {
        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        // 3 leases. the yield worker has 90% CPU util so each lease is contributing around 30% of the total cpu.
        // The taker worker is sitting at 50%. So any one of the leases will cause throughput overshoot
        final Lease lease1 = createDummyLease("lease1", TEST_YIELD_WORKER_ID);
        lease1.throughputKBps(300D);
        final Lease lease2 = createDummyLease("lease2", TEST_YIELD_WORKER_ID);
        lease2.throughputKBps(299D);
        final Lease lease3 = createDummyLease("lease3", TEST_YIELD_WORKER_ID);
        lease3.throughputKBps(301D);
        final Lease lease4 = createDummyLease("lease4", TEST_TAKE_WORKER_ID);
        lease4.throughputKBps(3000D);
        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4);

        // 1. test with the config set to false. No lease should be picked
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10);
        config.allowThroughputOvershoot(false);
        createLeaseAssignmentManager(config, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);

        leaseAssignmentManagerRunnable.run();
        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());

        // 2. test with config set to true. Take one lease
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10),
                Duration.ofHours(1).toMillis(),
                System::nanoTime,
                Integer.MAX_VALUE);
        leaseAssignmentManagerRunnable.run();

        final List<String> leaseKeysAssignedToTakeWorker = leaseRefresher.listLeases().stream()
                .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                .map(Lease::leaseKey)
                .collect(Collectors.toList());
        assertEquals(2, leaseKeysAssignedToTakeWorker.size());
        assertTrue(leaseKeysAssignedToTakeWorker.contains("lease4"));
        assertTrue(leaseKeysAssignedToTakeWorker.contains("lease2"));
    }

    @Test
    void performAssignment_takeAndYieldWorkerWithSeveralLeases_verifyBalancingBetweenLeases() throws Exception {

        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10),
                Duration.ofHours(1).toMillis(),
                System::nanoTime,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID + "1"));
        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID + "2"));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final Lease lease1 = createDummyLease("lease1", TEST_YIELD_WORKER_ID + "1");
        final Lease lease2 = createDummyLease("lease2", TEST_YIELD_WORKER_ID + "1");
        lease2.throughputKBps(1000);

        final Lease lease3 = createDummyLease("lease3", TEST_YIELD_WORKER_ID + "2");
        final Lease lease4 = createDummyLease("lease4", TEST_YIELD_WORKER_ID + "2");
        lease4.throughputKBps(1000);

        final Lease lease5 = createDummyLease("lease5", TEST_TAKE_WORKER_ID);

        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4, lease5);

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                3L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());
        assertEquals(
                2L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_YIELD_WORKER_ID + "1")
                                || lease.leaseOwner().equals(TEST_YIELD_WORKER_ID + "2"))
                        .count());
        assertTrue(leaseRefresher.listLeases().stream()
                .anyMatch(lease -> lease.leaseOwner().equals(TEST_YIELD_WORKER_ID + "1")));
        assertTrue(leaseRefresher.listLeases().stream()
                .anyMatch(lease -> lease.leaseOwner().equals(TEST_YIELD_WORKER_ID + "2")));
    }

    @Test
    void performAssignment_varianceBalanceFreq3_asserLoadBalancingEvery3Iteration() throws Exception {
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10);
        config.varianceBalancingFrequency(3);
        createLeaseAssignmentManager(config, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);

        setupConditionForVarianceBalancing();
        // 1sh Run, expect re-balance
        leaseAssignmentManagerRunnable.run();
        assertEquals(
                3L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());

        setupConditionForVarianceBalancing();
        // 2nd Run, expect no re-balance
        leaseAssignmentManagerRunnable.run();
        assertEquals(
                1L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());

        setupConditionForVarianceBalancing();
        // 3nd Run, expect no re-balance
        leaseAssignmentManagerRunnable.run();
        assertEquals(
                1L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());

        setupConditionForVarianceBalancing();
        // 4th Run, expect re-balance
        leaseAssignmentManagerRunnable.run();
        assertEquals(
                3L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());

        setupConditionForVarianceBalancing();
        // 5th Run, expect no re-balance
        leaseAssignmentManagerRunnable.run();
        assertEquals(
                1L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());
    }

    private void setupConditionForVarianceBalancing() throws Exception {

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID + "1"));
        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID + "2"));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final Lease lease1 = createDummyLease("lease1", TEST_YIELD_WORKER_ID + "1");
        final Lease lease2 = createDummyLease("lease2", TEST_YIELD_WORKER_ID + "1");
        lease2.throughputKBps(1000);

        final Lease lease3 = createDummyLease("lease3", TEST_YIELD_WORKER_ID + "2");
        final Lease lease4 = createDummyLease("lease4", TEST_YIELD_WORKER_ID + "2");
        lease4.throughputKBps(1000);

        final Lease lease5 = createDummyLease("lease5", TEST_TAKE_WORKER_ID);

        leaseRefresher.deleteLease(lease1);
        leaseRefresher.deleteLease(lease2);
        leaseRefresher.deleteLease(lease3);
        leaseRefresher.deleteLease(lease4);
        leaseRefresher.deleteLease(lease5);
        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4, lease5);
    }

    @Test
    void performAssignment_withLeaderSwitchOver_assertAssignmentOnlyAfterBeingLeader() throws Exception {
        when(mockLeaderDecider.isLeader(anyString())).thenReturn(false).thenReturn(true);

        final Supplier<Long> mockNanoTimeProvider = Mockito.mock(Supplier.class);
        when(mockNanoTimeProvider.get())
                .thenReturn(Duration.ofMillis(100).toNanos())
                .thenReturn(Duration.ofMillis(110).toNanos())
                .thenReturn(Duration.ofMillis(120).toNanos());
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                1L,
                mockNanoTimeProvider,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("unAssignedLease"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease-1", "someOwner"));

        // First time call is made, the worker is not leader, no assignment
        leaseAssignmentManagerRunnable.run();

        assertFalse(leaseRefresher.listLeases().stream()
                .anyMatch(lease ->
                        lease.leaseOwner() != null && lease.leaseOwner().equals(TEST_TAKE_WORKER_ID)));

        // Second time call is made, the worker is leader, for unAssignedLease assignment is done
        leaseAssignmentManagerRunnable.run();

        assertEquals(
                TEST_TAKE_WORKER_ID, leaseRefresher.getLease("unAssignedLease").leaseOwner());
        assertEquals("someOwner", leaseRefresher.getLease("lease-1").leaseOwner());

        // Third time call is made, the worker is leader, for expiredLease assignment is done
        leaseAssignmentManagerRunnable.run();

        assertEquals(
                TEST_TAKE_WORKER_ID, leaseRefresher.getLease("unAssignedLease").leaseOwner());
        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.getLease("lease-1").leaseOwner());
    }

    @Test
    void performAssignment_underUtilizedWorker_assertBalancingAndUnassignedLeaseAssignmentToSameWorker()
            throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10),
                Duration.ofHours(1).toMillis(),
                System::nanoTime,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final Lease leaseY1 = createDummyLease("leaseY1", TEST_YIELD_WORKER_ID);
        leaseY1.throughputKBps(1000);
        final Lease leaseT2 = createDummyUnAssignedLease("leaseT1");

        populateLeasesInLeaseTable(leaseY1, leaseT2);

        populateLeasesInLeaseTable(createDummyLease("leaseY2", TEST_YIELD_WORKER_ID));
        populateLeasesInLeaseTable(createDummyLease("leaseY3", TEST_YIELD_WORKER_ID));
        populateLeasesInLeaseTable(createDummyLease("leaseY5", TEST_YIELD_WORKER_ID));
        populateLeasesInLeaseTable(createDummyLease("leaseY6", TEST_YIELD_WORKER_ID));
        populateLeasesInLeaseTable(createDummyLease("leaseY7", TEST_YIELD_WORKER_ID));
        populateLeasesInLeaseTable(createDummyLease("leaseY8", TEST_YIELD_WORKER_ID));
        populateLeasesInLeaseTable(createDummyLease("leaseY9", TEST_YIELD_WORKER_ID));

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                4L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());
    }

    @Test
    void performAssignment_workerWithHotWorkerMetricButNotAboveAverage_validateRebalance()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final String randomWorkerId = "randomWorkerId";
        // Setting reBalance threshold as INT_MAX which means no reBalance due to variance in utilization ratio
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                Duration.ofHours(1).toMillis(),
                System::nanoTime,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createWorkerWithHotWorkerMetricStats(randomWorkerId));
        final WorkerMetricStats takeWorkerStats = createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID);
        takeWorkerStats.setMetricStats(ImmutableMap.of("C", ImmutableList.of(40D, 40D)));
        workerMetricsDAO.updateMetrics(takeWorkerStats);

        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey1", randomWorkerId));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey2", randomWorkerId));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey3", randomWorkerId));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey4", randomWorkerId));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey5", randomWorkerId));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey6", randomWorkerId));

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                5,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(randomWorkerId))
                        .count());
        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());
    }

    @Test
    void performAssignment_yieldWorkerWithSingleLease_assertReBalance() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                Duration.ofHours(1).toMillis(),
                System::nanoTime,
                Integer.MAX_VALUE);
        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final Lease lease1 = createDummyLease("lease1", TEST_YIELD_WORKER_ID);
        lease1.throughputKBps(5);
        final Lease lease2 = createDummyLease("lease2", TEST_TAKE_WORKER_ID);
        lease2.throughputKBps(30);
        populateLeasesInLeaseTable(lease1, lease2);

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                2L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                        .count());

        assertEquals(
                0L,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(TEST_YIELD_WORKER_ID))
                        .count());
    }

    @Test
    void performAssignment_continuousFailure_assertLeadershipRelease() throws Exception {
        final Supplier<Long> mockFailingNanoTimeProvider = Mockito.mock(Supplier.class);
        when(mockFailingNanoTimeProvider.get()).thenThrow(new RuntimeException("IAmAlwaysFailing"));

        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                Duration.ofHours(1).toMillis(),
                mockFailingNanoTimeProvider,
                Integer.MAX_VALUE);

        leaseAssignmentManagerRunnable.run();
        verify(mockLeaderDecider, times(0)).releaseLeadershipIfHeld();
        leaseAssignmentManagerRunnable.run();
        verify(mockLeaderDecider, times(0)).releaseLeadershipIfHeld();
        leaseAssignmentManagerRunnable.run();
        // After 3 failures, leadership is expected to be released.
        verify(mockLeaderDecider, times(1)).releaseLeadershipIfHeld();
    }

    private void populateLeasesInLeaseTable(Lease... leases) throws Exception {
        for (Lease lease : leases) {
            leaseRefresher.createLeaseIfNotExists(lease);
        }
    }

    @Test
    void performAssignment_onError_assertLeadershipRelease() throws Exception {
        final Supplier<Long> mockFailingNanoTimeProvider = Mockito.mock(Supplier.class);
        when(mockFailingNanoTimeProvider.get()).thenThrow(new OutOfMemoryError("IAmAlwaysFailing"));

        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                Duration.ofHours(1).toMillis(),
                mockFailingNanoTimeProvider,
                Integer.MAX_VALUE);

        leaseAssignmentManagerRunnable.run();
        // After 1 error, leadership is expected to be released.
        verify(mockLeaderDecider, times(1)).releaseLeadershipIfHeld();
    }

    @Test
    void startStopValidation_sanity()
            throws InterruptedException, ProvisionedThroughputException, InvalidStateException, DependencyException {
        final LeaseAssignmentManager leaseAssignmentManager = createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                Duration.ofHours(1).toMillis(),
                System::nanoTime,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("something"));

        leaseAssignmentManagerRunnable.run();

        when(scheduledFuture.isDone()).thenReturn(true);
        leaseAssignmentManager.stop();

        verify(scheduledFuture).cancel(anyBoolean());
        // Validate the assignment did happen
        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.listLeases().get(0).leaseOwner());
    }

    @Test
    void performAssignment_staleWorkerMetricsEntries_assertCleaning() throws Exception {
        LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20);

        config.staleWorkerMetricsEntryCleanupDuration(Duration.ofHours(60));
        createLeaseAssignmentManager(config, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);
        // Non expired workerMetrics
        writeToWorkerMetricsTables(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final WorkerMetricStats.LeaseTableWorkerMetricStats expiredWorkerStats =
                createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID);
        expiredWorkerStats.setLastUpdateTime(
                Instant.now().minus(100, ChronoUnit.HOURS).getEpochSecond());
        // expired workerMetrics
        writeToWorkerMetricsTables(expiredWorkerStats);

        leaseAssignmentManagerRunnable.run();

        assertEquals(1, workerMetricsDAO.getAllWorkerMetricStats().size());
        assertEquals(
                TEST_TAKE_WORKER_ID,
                workerMetricsDAO.getAllWorkerMetricStats().get(0).getWorkerId());
    }

    @Test
    void performAssignment_testRetryBehaviorForLeaseRefresher()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final EntityDAO mockedentiEntityDAO = Mockito.mock(EntityDAO.class);

        when(mockedentiEntityDAO.scanEntities(any())).thenThrow(new RuntimeException());

        createLeaseAssignmentManagerWithStrategy(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE,
                leaseRefresher,
                mockedentiEntityDAO,
                workerMetricsDAO,
                LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE);

        leaseAssignmentManagerRunnable.run();

        verify(mockedentiEntityDAO, times(2)).scanEntities(any());
    }

    @Test
    void performAssignment_testRetryBehaviorForWorkerMetricsDAO()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        when(mockProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);
        final WorkerMetricStatsDAO mockedWorkerMetricsDAO = Mockito.mock(WorkerMetricStatsDAO.class);

        final LegacyTableWorkerMetricStatsDAODelegate mockDelegate =
                Mockito.mock(LegacyTableWorkerMetricStatsDAODelegate.class);
        when(mockedWorkerMetricsDAO.getLegacyTableDaoDelegate()).thenReturn(mockDelegate);

        when(mockDelegate.getAllWorkerMetricStats()).thenThrow(new RuntimeException());

        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE,
                leaseRefresher,
                mockedWorkerMetricsDAO);

        leaseAssignmentManagerRunnable.run();

        verify(mockDelegate, times(2)).getAllWorkerMetricStats();
    }

    @Test
    void performAssignment_invalidLeaseInTable_validateAssignmentDoesNotFail() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("validLeaseKey"));

        // This lease fails to deserialize as it does not have required parameters still the assignment
        // does not fail and gracefully handle this.
        createAndPutBadLeaseEntryInTable("badLeaseKey");

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                TEST_TAKE_WORKER_ID, leaseRefresher.getLease("validLeaseKey").leaseOwner());
    }

    @Test
    void performAssignment_invalidWorkerMetricsEntry_validateAssignmentDoesNotFail() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("leaseKey"));

        // This lease fails to deserialize as it does not have required parameters still the assignment
        // does not fail and gracefully handle this.
        createAndPutBadWorkerMetricsEntryInTable("badWorkerId");

        leaseAssignmentManagerRunnable.run();

        assertEquals(TEST_TAKE_WORKER_ID, leaseRefresher.getLease("leaseKey").leaseOwner());
    }

    @Test
    void performAssignment_testAssignmentHandlingForDifferentPendingCheckpointStatesLeases() throws Exception {
        final long leaseDurationMillis = 100;
        final long currentTimeMillis = 10000;

        final Supplier<Long> mockNanoTimeProvider = Mockito.mock(Supplier.class);
        when(mockNanoTimeProvider.get())
                .thenReturn(Duration.ofMillis(currentTimeMillis).toNanos());

        gracefulLeaseHandoffConfig =
                LeaseManagementConfig.GracefulLeaseHandoffConfig.builder().build();
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                leaseDurationMillis,
                mockNanoTimeProvider,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final Lease lease1 = createDummyLease("lease1", TEST_YIELD_WORKER_ID);
        lease1.throughputKBps(5D);
        final Lease lease2 = createDummyLease("lease2", TEST_YIELD_WORKER_ID);
        lease2.throughputKBps(30D);
        populateLeasesInLeaseTable(lease1, lease2);

        // 1: Check one lease is marked for pending shutdown
        leaseAssignmentManagerRunnable.run();
        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(l -> TEST_YIELD_WORKER_ID.equals(l.checkpointOwner()))
                        .count());
        final Lease l1 = leaseRefresher.listLeases().stream()
                .filter(l -> TEST_YIELD_WORKER_ID.equals(l.checkpointOwner()))
                .findFirst()
                .get();

        // 2. This is a no-op because pending checkpoint is not expired
        leaseAssignmentManagerRunnable.run();
        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(l -> TEST_YIELD_WORKER_ID.equals(l.checkpointOwner()))
                        .count());
        final Lease l2 = leaseRefresher.listLeases().stream()
                .filter(l -> TEST_YIELD_WORKER_ID.equals(l.checkpointOwner()))
                .findFirst()
                .get();
        assertEquals(l1.leaseKey(), l2.leaseKey());

        // 3. Fast-forward the time to expire the lease. There should be no lease in pending checkpoint state
        final long newTimeMillis = leaseDurationMillis
                + gracefulLeaseHandoffConfig.gracefulLeaseHandoffTimeoutMillis()
                + TimeUnit.NANOSECONDS.toMillis(currentTimeMillis)
                + 100000;
        when(mockNanoTimeProvider.get())
                .thenReturn(Duration.ofMillis(newTimeMillis).toNanos());
        // Renew the lease2 as time is fast forwarded so the lease2 which is assigned to yield worker does not get
        // assigned to takeWorker. Otherwise the lease1 assigned to takeWorker is assigned back to yield worker as
        // after assignment of expired lease lease takeWorker is more on CPU.
        leaseRefresher.renewLease(lease2);
        leaseAssignmentManagerRunnable.run();
        assertEquals(
                0,
                leaseRefresher.listLeases().stream()
                        .filter(l -> l.checkpointOwner() != null)
                        .count());
    }

    @Test
    void performAssignment_expiredLeasesButPendingCheckpointNotExpiredLease_validateItIsAssigned() throws Exception {
        final Supplier<Long> mockNanoTimeProvider = Mockito.mock(Supplier.class);
        when(mockNanoTimeProvider.get())
                .thenReturn(Duration.ofMillis(100).toNanos())
                .thenReturn(Duration.ofMillis(110).toNanos());
        gracefulLeaseHandoffConfig = LeaseManagementConfig.GracefulLeaseHandoffConfig.builder()
                .gracefulLeaseHandoffTimeoutMillis(30000)
                .build();
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(9D, 20), 1L, mockNanoTimeProvider, Integer.MAX_VALUE);

        final Lease expireLease = createDummyLease("expiredLease", "random-owner");
        // LAM will add a timeout one pending checkpoint when it sees this attribute.
        expireLease.checkpointOwner("another-random-owner");
        leaseRefresher.createLeaseIfNotExists(expireLease);

        // run one so lease is tracked but since there is no worker participating in lease assignment yet we don't
        // assign it.
        leaseAssignmentManagerRunnable.run();
        assertNotEquals(
                TEST_TAKE_WORKER_ID, leaseRefresher.getLease("expiredLease").leaseOwner());

        // add a host now
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));
        leaseAssignmentManagerRunnable.run();
        assertEquals(
                TEST_TAKE_WORKER_ID, leaseRefresher.getLease("expiredLease").leaseOwner());
    }

    @Test
    void loadInMemoryStorageView_testDefaultWorkerMetricTakeLeasesUtilRatioCalculation() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);

        final WorkerMetricStats workerMetrics1 = createDummyDefaultWorkerMetrics("worker1");
        final WorkerMetricStats workerMetrics2 = createDummyTakeWorkerMetrics("worker2");

        workerMetricsDAO.updateMetrics(workerMetrics1);
        workerMetricsDAO.updateMetrics(workerMetrics2);

        populateLeasesInLeaseTable(
                createDummyUnAssignedLease("lease1"),
                createDummyUnAssignedLease("lease2"),
                createDummyUnAssignedLease("lease3"),
                createDummyLease("lease6", workerMetrics2.getWorkerId()));

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                3,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics1.getWorkerId()))
                        .count());

        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics2.getWorkerId()))
                        .count());
    }

    @Test
    void loadInMemoryStorageView_assertNoLeasesTakenFromOptimallyUtilizedDefaultWorkerMetricWorker() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);

        final WorkerMetricStats workerMetrics1 = createDummyDefaultWorkerMetrics("worker1");
        final WorkerMetricStats workerMetrics2 = createDummyTakeWorkerMetrics("worker2");

        workerMetricsDAO.updateMetrics(workerMetrics1);
        workerMetricsDAO.updateMetrics(workerMetrics2);

        final Lease lease1 = createDummyLease("lease1", workerMetrics1.getWorkerId());
        lease1.throughputKBps(100D);
        final Lease lease2 = createDummyLease("lease2", workerMetrics1.getWorkerId());
        lease2.throughputKBps(200D);
        final Lease lease3 = createDummyLease("lease3", workerMetrics2.getWorkerId());
        lease3.throughputKBps(300D);
        final Lease lease4 = createDummyLease("lease4", workerMetrics2.getWorkerId());
        lease4.throughputKBps(400D);

        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4);

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                2,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics1.getWorkerId()))
                        .count());

        assertEquals(
                2,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics2.getWorkerId()))
                        .count());
    }

    @Test
    void loadInMemoryStorageView_assertNoLeasesTakenWhenDefaultWorkerMetricAndCPUWorkerMetricWorkersAreOverloaded()
            throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);

        final WorkerMetricStats workerMetrics1 = createDummyDefaultWorkerMetrics("worker1");
        final WorkerMetricStats workerMetrics2 = createDummyYieldWorkerMetrics("worker2");

        workerMetricsDAO.updateMetrics(workerMetrics1);
        workerMetricsDAO.updateMetrics(workerMetrics2);

        final Lease lease1 = createDummyLease("lease1", workerMetrics1.getWorkerId());
        final Lease lease2 = createDummyLease("lease2", workerMetrics2.getWorkerId());

        populateLeasesInLeaseTable(lease1, lease2);

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics1.getWorkerId()))
                        .count());

        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics2.getWorkerId()))
                        .count());
    }

    //    @Test
    void loadInMemoryStorageView_assertLeasesAreTakenWhenDefaultWorkerMetricWorkerIsOverloaded() throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);

        final WorkerMetricStats workerMetrics1 = createDummyDefaultWorkerMetrics("worker1");
        final WorkerMetricStats workerMetrics2 = createDummyTakeWorkerMetrics("worker2");

        workerMetricsDAO.updateMetrics(workerMetrics1);
        workerMetricsDAO.updateMetrics(workerMetrics2);

        final Lease lease1 = createDummyLease("lease1", workerMetrics1.getWorkerId());
        lease1.throughputKBps(1000D);
        final Lease lease2 = createDummyLease("lease2", workerMetrics1.getWorkerId());
        lease2.throughputKBps(1000D);
        final Lease lease3 = createDummyLease("lease3", workerMetrics2.getWorkerId());
        lease3.throughputKBps(1D);
        final Lease lease4 = createDummyLease("lease4", workerMetrics2.getWorkerId());
        lease4.throughputKBps(1D);

        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4);
        leaseAssignmentManagerRunnable.run();

        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics1.getWorkerId()))
                        .count());

        assertEquals(
                3,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics2.getWorkerId()))
                        .count());

        assertEquals(
                1000D,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics1.getWorkerId()))
                        .mapToDouble(Lease::throughputKBps)
                        .sum());

        assertEquals(
                1002D,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics2.getWorkerId()))
                        .mapToDouble(Lease::throughputKBps)
                        .sum());
    }

    //    @Test
    void loadInMemoryStorageView_assertLeasesAreBalancedWhenDefaultWorkerMetricWorkerIsOverloadedWithMultipleRuns()
            throws Exception {
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);

        final WorkerMetricStats workerMetrics1 = createDummyDefaultWorkerMetrics("worker1");
        final WorkerMetricStats workerMetrics2 = createDummyTakeWorkerMetrics("worker2");

        workerMetricsDAO.updateMetrics(workerMetrics1);
        workerMetricsDAO.updateMetrics(workerMetrics2);

        final Lease lease1 = createDummyLease("lease1", workerMetrics1.getWorkerId());
        lease1.throughputKBps(1000D);
        final Lease lease2 = createDummyLease("lease2", workerMetrics1.getWorkerId());
        lease2.throughputKBps(1000D);
        final Lease lease3 = createDummyLease("lease3", workerMetrics2.getWorkerId());
        lease3.throughputKBps(1D);
        final Lease lease4 = createDummyLease("lease4", workerMetrics2.getWorkerId());
        lease4.throughputKBps(1D);

        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4);
        leaseAssignmentManagerRunnable.run();
        leaseAssignmentManagerRunnable.run();
        leaseAssignmentManagerRunnable.run();

        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics1.getWorkerId()))
                        .count());

        assertEquals(
                3,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics2.getWorkerId()))
                        .count());

        assertEquals(
                1000D,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics1.getWorkerId()))
                        .mapToDouble(Lease::throughputKBps)
                        .sum());

        assertEquals(
                1002D,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals(workerMetrics2.getWorkerId()))
                        .mapToDouble(Lease::throughputKBps)
                        .sum());
    }

    private void createAndPutBadWorkerMetricsEntryInTable(final String workerId) {
        final PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(WORKER_METRICS_TABLE_NAME)
                .item(ImmutableMap.of(
                        "leaseKey", AttributeValue.builder().s(workerId).build()))
                .build();

        dynamoDbAsyncClient.putItem(putItemRequest);
    }

    private void createAndPutBadLeaseEntryInTable(final String leaseKey) {
        final PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(LEASE_TABLE_NAME)
                .item(ImmutableMap.of(
                        "leaseKey", AttributeValue.builder().s(leaseKey).build()))
                .build();

        dynamoDbAsyncClient.putItem(putItemRequest);
    }

    @Test
    void testLeaseAssignmentSchedulingWithDefaultInterval() {
        long failoverTimeMillis = 1000L;
        ScheduledExecutorService mockExecutor = Mockito.mock(ScheduledExecutorService.class);

        LeaseAssignmentManager leaseAssignmentManager = new LeaseAssignmentManager(
                leaseRefresher,
                mockLeaderDecider,
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                TEST_LEADER_WORKER_ID,
                failoverTimeMillis,
                new NullMetricsFactory(),
                mockExecutor,
                System::nanoTime,
                Integer.MAX_VALUE,
                gracefulLeaseHandoffConfig,
                LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE,
                2 * failoverTimeMillis,
                streamIdCacheManager,
                Mockito.mock(LAMDataManager.class),
                leaseCleanupManager);

        leaseAssignmentManager.start();

        verify(mockExecutor)
                .scheduleWithFixedDelay(
                        any(Runnable.class), eq(0L), eq(2 * failoverTimeMillis), eq(TimeUnit.MILLISECONDS));
    }

    @ParameterizedTest
    @CsvSource({
        "1000, 500", // leaseAssignmentInterval smaller than failover
        "1000, 1000", // leaseAssignmentInterval equal to failover
        "1000, 2000", // leaseAssignmentInterval larger than failover
    })
    void testLeaseAssignmentWithDifferentIntervals(long failoverTimeMillis, long leaseAssignmentIntervalMillis) {
        ScheduledExecutorService mockExecutor = Mockito.mock(ScheduledExecutorService.class);

        LeaseAssignmentManager leaseAssignmentManager = new LeaseAssignmentManager(
                leaseRefresher,
                mockLeaderDecider,
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                TEST_LEADER_WORKER_ID,
                failoverTimeMillis,
                new NullMetricsFactory(),
                mockExecutor,
                System::nanoTime,
                Integer.MAX_VALUE,
                gracefulLeaseHandoffConfig,
                LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE,
                leaseAssignmentIntervalMillis,
                streamIdCacheManager,
                Mockito.mock(LAMDataManager.class),
                leaseCleanupManager);

        leaseAssignmentManager.start();

        verify(mockExecutor)
                .scheduleWithFixedDelay(
                        any(Runnable.class), eq(0L), eq(leaseAssignmentIntervalMillis), eq(TimeUnit.MILLISECONDS));
    }

    private LeaseAssignmentManager createLeaseAssignmentManager(
            final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config,
            final Long leaseDurationMillis,
            final Supplier<Long> nanoTimeProvider,
            final int maxLeasesPerWorker) {

        return createLeaseAssignmentManager(
                config, leaseDurationMillis, nanoTimeProvider, maxLeasesPerWorker, leaseRefresher, workerMetricsDAO);
    }

    private LeaseAssignmentManager createLeaseAssignmentManager(
            final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config,
            final Long leaseDurationMillis,
            final Supplier<Long> nanoTimeProvider,
            final int maxLeasesPerWorker,
            LeaseRefresher leaseRefresher,
            WorkerMetricStatsDAO workerMetricStatsDao) {

        return createLeaseAssignmentManagerWithStrategy(
                config,
                leaseDurationMillis,
                nanoTimeProvider,
                maxLeasesPerWorker,
                leaseRefresher,
                entityDAO,
                workerMetricStatsDao,
                LeaseAssignmentStrategy.WORKER_UTILIZATION_AWARE);
    }

    private LeaseAssignmentManager createLeaseAssignmentManagerWithStrategy(
            final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config,
            final Long leaseDurationMillis,
            final Supplier<Long> nanoTimeProvider,
            final int maxLeasesPerWorker,
            final LeaseAssignmentStrategy strategy) {

        return createLeaseAssignmentManagerWithStrategy(
                config,
                leaseDurationMillis,
                nanoTimeProvider,
                maxLeasesPerWorker,
                leaseRefresher,
                entityDAO,
                workerMetricsDAO,
                strategy);
    }

    private LeaseAssignmentManager createLeaseAssignmentManagerWithStrategy(
            final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config,
            final Long leaseDurationMillis,
            final Supplier<Long> nanoTimeProvider,
            final int maxLeasesPerWorker,
            LeaseRefresher leaseRefresher,
            EntityDAO entityDAO,
            WorkerMetricStatsDAO workerMetricStatsDao,
            final LeaseAssignmentStrategy strategy) {

        final LAMDataManager lamDataManager = new MigrationAwareLAMDataManager(
                entityDAO,
                workerMetricStatsDao,
                mockProvider,
                mock(Consumer.class),
                config,
                MoreExecutors.newDirectExecutorService(),
                new NullMetricsFactory());

        final LeaseAssignmentManager leaseAssignmentManager = new LeaseAssignmentManager(
                leaseRefresher,
                mockLeaderDecider,
                config,
                TEST_LEADER_WORKER_ID,
                leaseDurationMillis,
                new NullMetricsFactory(),
                scheduledExecutorService,
                nanoTimeProvider,
                maxLeasesPerWorker,
                gracefulLeaseHandoffConfig,
                strategy,
                2 * leaseDurationMillis,
                streamIdCacheManager,
                lamDataManager,
                leaseCleanupManager);
        leaseAssignmentManager.start();
        return leaseAssignmentManager;
    }

    @NotNull
    private static LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig
            getWorkerUtilizationAwareAssignmentConfig(final double maxThroughput, final int reBalanceThreshold) {
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                new LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig();
        config.maxThroughputPerHostKBps(maxThroughput);
        config.workerMetricsReporterFreqInMillis(Duration.ofHours(1).toMillis());
        config.reBalanceThresholdPercentage(reBalanceThreshold);
        config.dampeningPercentage(80);
        config.staleWorkerMetricsEntryCleanupDuration(Duration.ofDays(1000));
        config.varianceBalancingFrequency(1);
        return config;
    }

    /** A LeaseCleanupManager that reads and deletes through this test's lease table. */
    private LeaseCleanupManager createLeaseCleanupManager(final boolean cleanupLeasesUponShardCompletion) {
        return createLeaseCleanupManager(leaseRefresher, cleanupLeasesUponShardCompletion);
    }

    /** A LeaseCleanupManager that reads and deletes through the given lease refresher. */
    private LeaseCleanupManager createLeaseCleanupManager(
            final LeaseRefresher leaseRefresher, final boolean cleanupLeasesUponShardCompletion) {
        final LeaseCoordinator leaseCoordinator = mock(LeaseCoordinator.class);
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        return new LeaseCleanupManager(
                leaseCoordinator,
                new NullMetricsFactory(),
                mock(ScheduledExecutorService.class),
                cleanupLeasesUponShardCompletion,
                Duration.ofMinutes(1).toMillis(),
                Duration.ofMinutes(5).toMillis(),
                Duration.ofMinutes(30).toMillis());
    }

    private Lease createDummyUnAssignedLease(final String leaseKey) {
        final Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.checkpoint(TRIM_HORIZON);
        return lease;
    }

    private Lease createDummyLease(final String leaseKey, final String leaseOwner) {
        final Lease lease = createDummyUnAssignedLease(leaseKey);
        lease.leaseOwner(leaseOwner);
        lease.leaseCounter(123L);
        lease.throughputKBps(10D);
        return lease;
    }

    /** A lease owned by the take worker, with the given checkpoint and shard tree links. */
    private Lease createShardTreeLease(
            final String leaseKey,
            final ExtendedSequenceNumber checkpoint,
            final List<String> parentShardIds,
            final List<String> childShardIds) {
        final Lease lease = createDummyLease(leaseKey, TEST_TAKE_WORKER_ID);
        lease.checkpoint(checkpoint);
        lease.parentShardIds(parentShardIds);
        lease.childShardIds(childShardIds);
        return lease;
    }

    /** A multi-stream lease owned by another worker, with the given checkpoint and shard tree links. */
    private static MultiStreamLease createMultiStreamShardTreeLease(
            final String streamIdentifier,
            final String shardId,
            final ExtendedSequenceNumber checkpoint,
            final List<String> parentShardIds,
            final List<String> childShardIds) {
        final MultiStreamLease lease = new MultiStreamLease();
        lease.streamIdentifier(streamIdentifier);
        lease.shardId(shardId);
        lease.leaseKey(MultiStreamLease.getLeaseKey(streamIdentifier, shardId));
        lease.leaseOwner(TEST_YIELD_WORKER_ID);
        lease.leaseCounter(1L);
        lease.checkpoint(checkpoint);
        lease.parentShardIds(parentShardIds);
        lease.childShardIds(childShardIds);
        return lease;
    }

    private List<Lease> listShardEndLeases() throws Exception {
        return leaseRefresher.listLeases().stream()
                .filter(lease -> SHARD_END.equals(lease.checkpoint()))
                .collect(Collectors.toList());
    }

    private LeaseTableWorkerMetricStats createDummyDefaultWorkerMetrics(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.LeaseTableWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of())
                .build();
    }

    private LeaseTableWorkerMetricStats createDummyYieldWorkerMetrics(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.LeaseTableWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(90D, 90D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private LeaseTableWorkerMetricStats createDummyTakeWorkerMetrics(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.LeaseTableWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(50D, 50D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private LeaseTableWorkerMetricStats createDummyWorkerMetrics(
            final String workerId, final double value, final long operatingRangeMax) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.LeaseTableWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(value, value)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(operatingRangeMax)))
                .build();
    }

    private LeaseTableWorkerMetricStats createWorkerWithFailingWorkerMetric(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.LeaseTableWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(50D, -1D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private LeaseTableWorkerMetricStats createWorkerWithFailingWorkerMetricInPast(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.LeaseTableWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(-1D, 50D, 50D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private LeaseTableWorkerMetricStats createWorkerWithHotWorkerMetricStats(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.LeaseTableWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(90D, 90D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(50L)))
                .build();
    }

    private LeaseTableWorkerMetricStats createInActiveWorkerWithNoUtilization(final String workerId) {
        // Setting 0 as update time means worker is always expired.
        return WorkerMetricStats.LeaseTableWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(0L)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(5D, 5D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private void writeToWorkerMetricsTables(final LeaseTableWorkerMetricStats workerMetrics) {
        dynamoDbAsyncClient
                .putItem(PutItemRequest.builder()
                        .tableName(LEASE_TABLE_NAME)
                        .item(TableSchema.fromBean(LeaseTableWorkerMetricStats.class)
                                .itemToMap(workerMetrics, false))
                        .build())
                .join();
    }

    @Test
    void performAssignment_withWorkerUtilizationStrategy_usesVarianceDecider() throws Exception {
        // Variance decider assigns based on utilization, not lease count.
        // Yield worker (90% CPU) has 0 leases, Take worker (50% CPU) has 1 lease.
        // Variance decider should assign to Take worker (lower utilization) despite having more leases.
        // If lease-count decider were used instead, it would assign to Yield worker (fewer leases).
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("existingLease", TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("newLease"));

        leaseAssignmentManagerRunnable.run();

        final Map<String, String> leaseAssignments =
                leaseRefresher.listLeases().stream().collect(Collectors.toMap(Lease::leaseKey, Lease::leaseOwner));
        // Variance decider picks Take worker (lower CPU) even though it already has a lease
        assertEquals(TEST_TAKE_WORKER_ID, leaseAssignments.get("newLease"));
    }

    @Test
    void performAssignment_withLeaseCountStrategy_usesLeaseCountDecider() throws Exception {
        // Lease-count decider assigns based on lease count, ignoring utilization.
        // Yield worker (90% CPU) has 0 leases, Take worker (50% CPU) has 1 lease.
        // Lease-count decider should assign to Yield worker (fewer leases) despite higher utilization.
        // If variance decider were used instead, it would assign to Take worker (lower utilization).
        createLeaseAssignmentManagerWithStrategy(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE,
                LeaseAssignmentStrategy.LEASE_COUNT_BASED);

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("existingLease", TEST_TAKE_WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("newLease"));

        leaseAssignmentManagerRunnable.run();

        final Map<String, String> leaseAssignments =
                leaseRefresher.listLeases().stream().collect(Collectors.toMap(Lease::leaseKey, Lease::leaseOwner));
        // Lease-count decider picks Yield worker (fewer leases) despite higher CPU
        assertEquals(TEST_YIELD_WORKER_ID, leaseAssignments.get("newLease"));
    }

    // --- Migration: LAM balances to 3.4 workers (legacy-only metrics without entityType) ---

    @Test
    void performAssignment_duringMigration_assignsLeasesToBoth34And35Workers() throws Exception {
        // Create legacy worker metrics table (PK: wid) — simulates a pre-existing 3.4 deployment
        dynamoDbAsyncClient
                .createTable(b -> b.tableName(WORKER_METRICS_TABLE_NAME)
                        .keySchema(KeySchemaElement.builder()
                                .attributeName("wid")
                                .keyType(KeyType.HASH)
                                .build())
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("wid")
                                .attributeType(ScalarAttributeType.S)
                                .build())
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(5L)
                                .writeCapacityUnits(5L)
                                .build()))
                .join();

        // Simulate DEPLOYED state: 3.5 workers write to lease table, 3.4 workers write to legacy table
        final TableMigrationStatusProvider deployedProvider = mock(TableMigrationStatusProvider.class);
        when(deployedProvider.getTableMigrationStatus())
                .thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);
        when(deployedProvider.dynamicModeChangeSupportNeeded()).thenReturn(true);
        final WorkerMetricsTableConfig config = new WorkerMetricsTableConfig("applicationName");
        config.tableName(WORKER_METRICS_TABLE_NAME);
        WorkerMetricStatsDAO migrationDao =
                new WorkerMetricStatsDAO(dynamoDbAsyncClient, config, LEASE_TABLE_NAME, 10000L, deployedProvider);
        migrationDao.initialize();

        // 3.5 worker: writes metrics to lease table (has entityType)
        final String worker35 = "worker-3-5";
        migrationDao.updateMetrics(createDummyTakeWorkerMetrics(worker35));

        // 3.4 worker: writes metrics to legacy table WITHOUT entityType (simulates old KCL version)
        final String worker34 = "worker-3-4";
        final long currentTime = Instant.now().getEpochSecond();
        Map<String, AttributeValue> legacyItem = new HashMap<>();
        legacyItem.put("wid", AttributeValue.fromS(worker34));
        legacyItem.put("lut", AttributeValue.fromN(String.valueOf(currentTime)));
        legacyItem.put(
                "sts",
                AttributeValue.fromM(ImmutableMap.of(
                        "C",
                        AttributeValue.builder()
                                .l(AttributeValue.fromN("50.0"), AttributeValue.fromN("50.0"))
                                .build())));
        legacyItem.put(
                "opr",
                AttributeValue.fromM(ImmutableMap.of(
                        "C",
                        AttributeValue.builder().l(AttributeValue.fromN("80")).build())));
        legacyItem.put("p", AttributeValue.fromM(ImmutableMap.of("versionHash", AttributeValue.fromS("0"))));
        // Deliberately NO entityType attribute — simulates 3.4 worker
        dynamoDbAsyncClient
                .putItem(b -> b.tableName(WORKER_METRICS_TABLE_NAME).item(legacyItem))
                .join();

        // Create unassigned leases
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("lease1"));
        leaseRefresher.createLeaseIfNotExists(createDummyUnAssignedLease("lease2"));

        when(mockProvider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_DEPLOYED);
        createLeaseAssignmentManager(
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                100L,
                System::nanoTime,
                Integer.MAX_VALUE,
                leaseRefresher,
                migrationDao);

        leaseAssignmentManagerRunnable.run();

        final Map<String, String> assignments =
                leaseRefresher.listLeases().stream().collect(Collectors.toMap(Lease::leaseKey, Lease::leaseOwner));

        // Both workers should have leases assigned — LAM must see 3.4 worker from legacy table
        assertTrue(assignments.containsValue(worker35), "3.5 worker should have leases assigned");
        assertTrue(
                assignments.containsValue(worker34), "3.4 worker (legacy, no entityType) should have leases assigned");
    }

    // ===== Absolute threshold tests =====

    @Test
    void performAssignment_absoluteThreshold_zeroThroughputLowCpu_noRebalance() throws Exception {
        // Two workers with zero-throughput (silent) shards at low CPU.
        // Fleet avg 0.3%. With absolute threshold of 10, rebalancing only triggers when a worker
        // is more than 5 percentage points from average. No worker exceeds that.
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10);
        config.useAbsoluteReBalanceThreshold(true);
        createLeaseAssignmentManager(config, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker1", 0.5, 80));
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker2", 0.1, 80));

        final Lease lease1 = createDummyLease("lease1", "worker1");
        lease1.throughputKBps(0D);
        final Lease lease2 = createDummyLease("lease2", "worker1");
        lease2.throughputKBps(0D);
        final Lease lease3 = createDummyLease("lease3", "worker1");
        lease3.throughputKBps(0D);
        final Lease lease4 = createDummyLease("lease4", "worker1");
        lease4.throughputKBps(0D);
        final Lease lease5 = createDummyLease("lease5", "worker1");
        lease5.throughputKBps(0D);
        final Lease lease6 = createDummyLease("lease6", "worker2");
        lease6.throughputKBps(0D);
        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4, lease5, lease6);

        leaseAssignmentManagerRunnable.run();

        // With absolute threshold, all workers within ±5 points of avg → no rebalancing
        assertEquals(
                5,
                leaseRefresher.listLeases().stream()
                        .filter(l -> l.leaseOwner().equals("worker1"))
                        .count(),
                "Worker1 should keep all 5 leases — absolute threshold prevents triggering");
    }

    @Test
    void performAssignment_absoluteThreshold_nonZeroThroughputLowCpu_noRebalance() throws Exception {
        // Two workers at 12% and 8% CPU with non-zero throughput leases.
        // Fleet avg 10%. With absolute threshold of 10, rebalancing only triggers when a worker
        // is more than 5 percentage points from average. Neither worker exceeds that.
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10);
        config.useAbsoluteReBalanceThreshold(true);
        createLeaseAssignmentManager(config, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker1", 12.0, 80));
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker2", 8.0, 80));

        final Lease lease1 = createDummyLease("lease1", "worker1");
        lease1.throughputKBps(100D);
        final Lease lease2 = createDummyLease("lease2", "worker1");
        lease2.throughputKBps(200D);
        final Lease lease3 = createDummyLease("lease3", "worker1");
        lease3.throughputKBps(300D);
        final Lease lease4 = createDummyLease("lease4", "worker1");
        lease4.throughputKBps(400D);
        final Lease lease5 = createDummyLease("lease5", "worker2");
        lease5.throughputKBps(500D);
        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4, lease5);

        leaseAssignmentManagerRunnable.run();

        // With absolute threshold, both workers within ±5 points of avg → no rebalancing
        assertEquals(
                4,
                leaseRefresher.listLeases().stream()
                        .filter(l -> l.leaseOwner().equals("worker1"))
                        .count(),
                "Worker1 should keep all 4 leases — absolute threshold prevents triggering");
    }

    @Test
    void performAssignment_absoluteThreshold_highCpu_rebalanceTriggered() throws Exception {
        // Yield worker at 90% CPU, take worker at 50% CPU. Fleet avg 70%.
        // With absolute threshold of 10, yield worker at 90% is more than 5 points above avg.
        // Rebalancing should trigger and leases should move.
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10);
        config.useAbsoluteReBalanceThreshold(true);
        createLeaseAssignmentManager(config, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final Lease lease1 = createDummyLease("lease1", TEST_YIELD_WORKER_ID);
        lease1.throughputKBps(100D);
        final Lease lease2 = createDummyLease("lease2", TEST_YIELD_WORKER_ID);
        lease2.throughputKBps(200D);
        final Lease lease3 = createDummyLease("lease3", TEST_YIELD_WORKER_ID);
        lease3.throughputKBps(300D);
        final Lease lease4 = createDummyLease("lease4", TEST_YIELD_WORKER_ID);
        lease4.throughputKBps(400D);
        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4);

        leaseAssignmentManagerRunnable.run();

        // Yield worker at 90% is outside absolute threshold → rebalancing triggers
        assertTrue(
                leaseRefresher.listLeases().stream()
                                .filter(l -> l.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                                .mapToDouble(Lease::throughputKBps)
                                .sum()
                        >= 100D,
                "Take worker should have received at least one lease from yield worker");
    }

    @Test
    void performAssignment_absoluteThreshold_withinBandButAboveOperatingRange_rebalanceTriggered() throws Exception {
        // Worker2 at 59% is within absolute band [40%, 60%] but above operating range (50).
        // Rebalancing should still trigger via operating range check.
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20);
        config.useAbsoluteReBalanceThreshold(true);
        createLeaseAssignmentManager(config, 100L, System::nanoTime, Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("Worker1", 41, 50));
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("Worker2", 59, 50));

        final Lease lease1 = createDummyLease("lease1", "Worker2");
        lease1.throughputKBps(1000);
        final Lease lease2 = createDummyLease("lease2", "Worker2");
        lease2.throughputKBps(1);
        populateLeasesInLeaseTable(lease1, lease2);

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                1,
                leaseRefresher.listLeases().stream()
                        .filter(lease -> lease.leaseOwner().equals("Worker1"))
                        .count(),
                "Worker1 should receive a lease — Worker2 is above operating range despite being within absolute band");
    }

    @Test
    void performAssignment_absoluteThreshold_customThresholdValue_rebalanceTriggered() throws Exception {
        // Yield worker at 90%, take worker at 50%. Fleet avg 70%.
        // With a smaller absolute threshold (4), band is avg ±2 = [68%, 72%].
        // Yield worker at 90% is outside → triggers rebalancing.
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 4);
        config.useAbsoluteReBalanceThreshold(true);
        createLeaseAssignmentManager(config, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID));
        workerMetricsDAO.updateMetrics(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final Lease lease1 = createDummyLease("lease1", TEST_YIELD_WORKER_ID);
        lease1.throughputKBps(100D);
        final Lease lease2 = createDummyLease("lease2", TEST_YIELD_WORKER_ID);
        lease2.throughputKBps(200D);
        final Lease lease3 = createDummyLease("lease3", TEST_YIELD_WORKER_ID);
        lease3.throughputKBps(300D);
        final Lease lease4 = createDummyLease("lease4", TEST_YIELD_WORKER_ID);
        lease4.throughputKBps(400D);
        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4);

        leaseAssignmentManagerRunnable.run();

        assertTrue(
                leaseRefresher.listLeases().stream()
                                .filter(l -> l.leaseOwner().equals(TEST_TAKE_WORKER_ID))
                                .mapToDouble(Lease::throughputKBps)
                                .sum()
                        >= 100D,
                "Take worker should have received at least one lease with tighter absolute threshold");
    }

    @Test
    void performAssignment_absoluteThreshold_highThresholdValue_noRebalance() throws Exception {
        // Workers at 90% and 10% CPU with a high threshold (90). Fleet avg 50%.
        // Band is avg ±45 = [5%, 95%]. Both workers inside despite large CPU spread.
        // Operating range set to 100 so it does not interfere.
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 90);
        config.useAbsoluteReBalanceThreshold(true);
        createLeaseAssignmentManager(config, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker1", 90, 100));
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker2", 10, 100));

        final Lease lease1 = createDummyLease("lease1", "worker1");
        lease1.throughputKBps(100D);
        final Lease lease2 = createDummyLease("lease2", "worker1");
        lease2.throughputKBps(200D);
        final Lease lease3 = createDummyLease("lease3", "worker2");
        lease3.throughputKBps(100D);
        populateLeasesInLeaseTable(lease1, lease2, lease3);

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                2,
                leaseRefresher.listLeases().stream()
                        .filter(l -> l.leaseOwner().equals("worker1"))
                        .count(),
                "Worker1 should keep both leases — high threshold band covers entire CPU range");
    }

    @Test
    void performAssignment_absoluteThreshold_workerBelowLowerLimit_rebalanceTriggered() throws Exception {
        // Three workers at 63%, 63%, 50%. Fleet avg ~58.7%.
        // With absolute threshold of 10, band is [53.7%, 63.7%].
        // Worker3 at 50% is below lower limit 53.7% → triggers rebalancing.
        // Workers 1 and 2 at 63% are inside band (below 63.7%).
        // No worker is above upper limit — this isolates the below-lower trigger path.
        // Fallback logic picks the most loaded worker (63%) to yield leases.
        // Worker3 at 50% is below fleet avg → can receive leases.
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10);
        config.useAbsoluteReBalanceThreshold(true);
        createLeaseAssignmentManager(config, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker1", 63, 100));
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker2", 63, 100));
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker3", 50, 100));

        // Many small leases to keep avg throughput per worker high,
        // so receiver-side extrapolation stays below fleet average.
        for (int i = 1; i <= 10; i++) {
            final Lease lease = createDummyLease("lease-w1-" + i, "worker1");
            lease.throughputKBps(100D);
            populateLeasesInLeaseTable(lease);
        }
        for (int i = 1; i <= 10; i++) {
            final Lease lease = createDummyLease("lease-w2-" + i, "worker2");
            lease.throughputKBps(100D);
            populateLeasesInLeaseTable(lease);
        }
        for (int i = 1; i <= 2; i++) {
            final Lease lease = createDummyLease("lease-w3-" + i, "worker3");
            lease.throughputKBps(100D);
            populateLeasesInLeaseTable(lease);
        }

        leaseAssignmentManagerRunnable.run();

        // Worker3 at 50% is below lower limit 53.7% → rebalancing triggers.
        // No worker above upper limit — only below-lower path fires.
        assertTrue(
                leaseRefresher.listLeases().stream()
                                .filter(l -> l.leaseOwner().equals("worker3"))
                                .count()
                        > 2,
                "Worker3 should receive leases — rebalancing triggered solely by worker below lower limit");
    }

    @Test
    void performAssignment_sameData_absoluteSkipsButRelativeTriggers() throws Exception {
        // Same workers and leases tested with both modes. Workers at 0.5% and 0.1% CPU.
        // Fleet avg 0.3%, threshold=10.
        // Absolute band: [-4.7%, 5.3%] → both inside → no trigger.
        // Relative band: [0.27%, 0.33%] → worker1 at 0.5% above 0.33% → triggers.

        // --- Part 1: Absolute mode — no trigger, leases unchanged ---
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig absConfig =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10);
        absConfig.useAbsoluteReBalanceThreshold(true);
        createLeaseAssignmentManager(absConfig, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);

        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker1", 0.5, 80));
        workerMetricsDAO.updateMetrics(createDummyWorkerMetrics("worker2", 0.1, 80));

        final Lease lease1 = createDummyLease("lease1", "worker1");
        lease1.throughputKBps(0D);
        final Lease lease2 = createDummyLease("lease2", "worker1");
        lease2.throughputKBps(0D);
        final Lease lease3 = createDummyLease("lease3", "worker1");
        lease3.throughputKBps(0D);
        final Lease lease4 = createDummyLease("lease4", "worker1");
        lease4.throughputKBps(0D);
        final Lease lease5 = createDummyLease("lease5", "worker1");
        lease5.throughputKBps(0D);
        final Lease lease6 = createDummyLease("lease6", "worker2");
        lease6.throughputKBps(0D);
        populateLeasesInLeaseTable(lease1, lease2, lease3, lease4, lease5, lease6);

        leaseAssignmentManagerRunnable.run();

        assertEquals(
                5,
                leaseRefresher.listLeases().stream()
                        .filter(l -> l.leaseOwner().equals("worker1"))
                        .count(),
                "Absolute mode: Worker1 should keep all 5 leases — all workers within absolute threshold range");

        // --- Part 2: Relative mode — same leases still in table, triggers ---
        final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig relConfig =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10);
        // useAbsoluteReBalanceThreshold defaults to false — relative mode
        createLeaseAssignmentManager(relConfig, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);

        leaseAssignmentManagerRunnable.run();

        assertTrue(
                leaseRefresher.listLeases().stream()
                                .filter(l -> l.leaseOwner().equals("worker1"))
                                .count()
                        < 5,
                "Relative mode: Worker1 should lose leases — worker1 at 0.5% is outside relative threshold range [0.27%, 0.33%]");
    }
}
