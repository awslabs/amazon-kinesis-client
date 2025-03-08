package software.amazon.kinesis.coordinator.assignment;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.var;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;

import static java.util.Objects.nonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber.TRIM_HORIZON;

class LeaseAssignmentManagerTest {

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
    private final LeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
            LEASE_TABLE_NAME,
            dynamoDbAsyncClient,
            new DynamoDBLeaseSerializer(),
            true,
            TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK,
            LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
            new DdbTableConfig(),
            LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
            LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
            DefaultSdkAutoConstructList.getInstance());
    private WorkerMetricStatsDAO workerMetricsDAO;

    @BeforeEach
    void setup() throws ProvisionedThroughputException, DependencyException {
        final WorkerMetricsTableConfig config = new WorkerMetricsTableConfig("applicationName");
        config.tableName(WORKER_METRICS_TABLE_NAME);
        workerMetricsDAO = new WorkerMetricStatsDAO(dynamoDbAsyncClient, config, 10000L);
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
        leaseRefresher.createLeaseTableIfNotExists();
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
        final var config = getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 10);
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
    void performAssignment_staleWorkerMetricsEntries_assertCleaning() {
        LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config =
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20);

        config.staleWorkerMetricsEntryCleanupDuration(Duration.ofHours(60));
        createLeaseAssignmentManager(config, Duration.ofHours(1).toMillis(), System::nanoTime, Integer.MAX_VALUE);
        // Non expired workerMetrics
        writeToWorkerMetricsTables(createDummyTakeWorkerMetrics(TEST_TAKE_WORKER_ID));

        final WorkerMetricStats expiredWorkerStats = createDummyYieldWorkerMetrics(TEST_YIELD_WORKER_ID);
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
    void performAssignment_testRetryBehavior()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {

        final WorkerMetricStatsDAO mockedWorkerMetricsDAO = Mockito.mock(WorkerMetricStatsDAO.class);
        final LeaseRefresher mockedLeaseRefresher = Mockito.mock(LeaseRefresher.class);

        when(mockedLeaseRefresher.listLeasesParallely(any(), anyInt())).thenThrow(new RuntimeException());
        when(mockedWorkerMetricsDAO.getAllWorkerMetricStats()).thenThrow(new RuntimeException());

        final LeaseAssignmentManager leaseAssignmentManager = new LeaseAssignmentManager(
                mockedLeaseRefresher,
                mockedWorkerMetricsDAO,
                mockLeaderDecider,
                getWorkerUtilizationAwareAssignmentConfig(Double.MAX_VALUE, 20),
                TEST_LEADER_WORKER_ID,
                100L,
                new NullMetricsFactory(),
                scheduledExecutorService,
                System::nanoTime,
                Integer.MAX_VALUE,
                LeaseManagementConfig.GracefulLeaseHandoffConfig.builder()
                        .isGracefulLeaseHandoffEnabled(false)
                        .build());

        leaseAssignmentManager.start();

        leaseAssignmentManagerRunnable.run();

        verify(mockedLeaseRefresher, times(2)).listLeasesParallely(any(), anyInt());
        verify(mockedWorkerMetricsDAO, times(2)).getAllWorkerMetricStats();
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
                        "wid", AttributeValue.builder().s(workerId).build()))
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

    private LeaseAssignmentManager createLeaseAssignmentManager(
            final LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig config,
            final Long leaseDurationMillis,
            final Supplier<Long> nanoTimeProvider,
            final int maxLeasesPerWorker) {

        final LeaseAssignmentManager leaseAssignmentManager = new LeaseAssignmentManager(
                leaseRefresher,
                workerMetricsDAO,
                mockLeaderDecider,
                config,
                TEST_LEADER_WORKER_ID,
                leaseDurationMillis,
                new NullMetricsFactory(),
                scheduledExecutorService,
                nanoTimeProvider,
                maxLeasesPerWorker,
                gracefulLeaseHandoffConfig);
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

    private WorkerMetricStats createDummyDefaultWorkerMetrics(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of())
                .build();
    }

    private WorkerMetricStats createDummyYieldWorkerMetrics(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(90D, 90D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private WorkerMetricStats createDummyTakeWorkerMetrics(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(50D, 50D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private WorkerMetricStats createDummyWorkerMetrics(
            final String workerId, final double value, final long operatingRangeMax) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(value, value)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(operatingRangeMax)))
                .build();
    }

    private WorkerMetricStats createWorkerWithFailingWorkerMetric(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(50D, -1D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private WorkerMetricStats createWorkerWithFailingWorkerMetricInPast(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(-1D, 50D, 50D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private WorkerMetricStats createWorkerWithHotWorkerMetricStats(final String workerId) {
        final long currentTime = Instant.now().getEpochSecond();
        return WorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(currentTime)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(90D, 90D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(50L)))
                .build();
    }

    private WorkerMetricStats createInActiveWorkerWithNoUtilization(final String workerId) {
        // Setting 0 as update time means worker is always expired.
        return WorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(0L)
                .metricStats(ImmutableMap.of("C", ImmutableList.of(5D, 5D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    private void writeToWorkerMetricsTables(final WorkerMetricStats workerMetrics) {
        dynamoDbAsyncClient
                .putItem(PutItemRequest.builder()
                        .tableName(WORKER_METRICS_TABLE_NAME)
                        .item(TableSchema.fromBean(WorkerMetricStats.class).itemToMap(workerMetrics, false))
                        .build())
                .join();
    }
}
