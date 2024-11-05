/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.coordinator.migration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStatsDAO;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class MigrationReadyMonitorTest {
    private static final String WORKER_ID = "MigrationReadyMonitorTestWorker0";
    private static final String DUMMY_STREAM_NAME = "DummyStreamName";
    private static final long WORKER_METRICS_EXPIRY_SECONDS = 60L;

    private MigrationReadyMonitor monitorUnderTest;

    private final MetricsFactory nullMetricsFactory = new NullMetricsFactory();
    private final Callable<Long> mockTimeProvider = mock(Callable.class, Mockito.RETURNS_MOCKS);

    private final LeaderDecider mockLeaderDecider = mock(LeaderDecider.class, Mockito.RETURNS_MOCKS);

    private final WorkerMetricStatsDAO mockWorkerMetricsDao = mock(WorkerMetricStatsDAO.class, Mockito.RETURNS_MOCKS);

    private final LeaseRefresher mockLeaseRefresher = mock(LeaseRefresher.class, Mockito.RETURNS_MOCKS);

    private final ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class, Mockito.RETURNS_MOCKS);

    private final Runnable mockRunnableCallback = mock(Runnable.class, Mockito.RETURNS_MOCKS);

    @BeforeEach
    public void setup() {
        monitorUnderTest = new MigrationReadyMonitor(
                nullMetricsFactory,
                mockTimeProvider,
                mockLeaderDecider,
                WORKER_ID,
                mockWorkerMetricsDao,
                WORKER_METRICS_EXPIRY_SECONDS,
                mockLeaseRefresher,
                mockScheduler,
                mockRunnableCallback,
                0 /* stabilization duration - 0, to let the callback happen right away */);
    }

    @ParameterizedTest
    @ValueSource(strings = {"WORKER_READY_CONDITION_MET", "WORKER_READY_CONDITION_MET_MULTISTREAM_MODE_SANITY"})
    public void verifyNonLeaderDoesNotPerformMigrationChecks(final TestDataType testDataType) throws Exception {
        final TestData data = TEST_DATA_MAP.get(testDataType);
        when(mockTimeProvider.call()).thenReturn(1000L);
        when(mockLeaseRefresher.isLeaseOwnerToLeaseKeyIndexActive()).thenReturn(true);
        when(mockLeaderDecider.isLeader(eq(WORKER_ID))).thenReturn(false);
        when(mockWorkerMetricsDao.getAllWorkerMetricStats()).thenReturn(data.workerMetrics);
        when(mockLeaseRefresher.listLeases()).thenReturn(data.leaseList);

        monitorUnderTest.startMonitor();
        final ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduler)
                .scheduleWithFixedDelay(runnableArgumentCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        runnableArgumentCaptor.getValue().run();

        verify(mockRunnableCallback, never()).run();
    }

    @ParameterizedTest
    @ValueSource(strings = {"WORKER_READY_CONDITION_MET", "WORKER_READY_CONDITION_MET_MULTISTREAM_MODE_SANITY"})
    public void verifyLeaderPerformMigrationChecks(final TestDataType testDataType) throws Exception {
        final TestData data = TEST_DATA_MAP.get(testDataType);
        when(mockTimeProvider.call()).thenReturn(1000L);
        when(mockLeaseRefresher.isLeaseOwnerToLeaseKeyIndexActive()).thenReturn(true);
        when(mockLeaderDecider.isLeader(eq(WORKER_ID))).thenReturn(true);
        when(mockWorkerMetricsDao.getAllWorkerMetricStats()).thenReturn(data.workerMetrics);
        when(mockLeaseRefresher.listLeases()).thenReturn(data.leaseList);

        monitorUnderTest.startMonitor();
        final ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduler)
                .scheduleWithFixedDelay(runnableArgumentCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        runnableArgumentCaptor.getValue().run();

        verify(mockRunnableCallback, times(1)).run();
    }

    @ParameterizedTest
    @CsvSource({
        "false, WORKER_READY_CONDITION_MET",
        "false, WORKER_READY_CONDITION_MET_MULTISTREAM_MODE_SANITY",
        "true, WORKER_READY_CONDITION_NOT_MET_WITH_ZERO_WORKER_STATS",
        "true, WORKER_READY_CONDITION_NOT_MET_WITH_PARTIAL_WORKER_STATS",
        "true, WORKER_READY_CONDITION_NOT_MET_WITH_ALL_INACTIVE_WORKER_STATS",
        "true, WORKER_READY_CONDITION_NOT_MET_WITH_PARTIAL_INACTIVE_WORKER_STATS",
        "true, WORKER_READY_CONDITION_NOT_MET_WITH_EXPIRED_LEASES_AND_NO_WORKER_STATS",
        "true, WORKER_READY_CONDITION_NOT_MET_WITH_EXPIRED_LEASES_AND_INACTIVE_WORKER_STATS",
        "true, WORKER_READY_CONDITION_NOT_MET_MULTISTREAM_MODE_SANITY"
    })
    public void testReadyConditionNotMetDoesNotInvokeCallback(final boolean gsiReady, final TestDataType testDataType)
            throws Exception {
        final TestData data = TEST_DATA_MAP.get(testDataType);
        when(mockTimeProvider.call()).thenReturn(80 * 1000L);
        when(mockLeaseRefresher.isLeaseOwnerToLeaseKeyIndexActive()).thenReturn(gsiReady);
        when(mockLeaderDecider.isLeader(eq(WORKER_ID))).thenReturn(true);
        when(mockWorkerMetricsDao.getAllWorkerMetricStats()).thenReturn(data.workerMetrics);
        when(mockLeaseRefresher.listLeases()).thenReturn(data.leaseList);

        monitorUnderTest.startMonitor();
        final ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduler)
                .scheduleWithFixedDelay(runnableArgumentCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        runnableArgumentCaptor.getValue().run();

        verify(mockRunnableCallback, never()).run();
    }

    @Test
    public void testExpiredLeaseOwner() throws Exception {
        final TestData data1 = TEST_DATA_MAP.get(
                TestDataType.WORKER_READY_CONDITION_NOT_MET_WITH_EXPIRED_LEASES_AND_INACTIVE_WORKER_STATS);
        final TestData data2 =
                TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET_AFTER_EXPIRED_LEASES_ARE_REASSIGNED);

        when(mockTimeProvider.call()).thenReturn(80 * 1000L);
        when(mockLeaseRefresher.isLeaseOwnerToLeaseKeyIndexActive()).thenReturn(true);
        when(mockLeaderDecider.isLeader(eq(WORKER_ID))).thenReturn(true);
        when(mockWorkerMetricsDao.getAllWorkerMetricStats())
                .thenReturn(data1.workerMetrics)
                .thenReturn(data2.workerMetrics);
        when(mockLeaseRefresher.listLeases()).thenReturn(data1.leaseList).thenReturn(data2.leaseList);

        monitorUnderTest.startMonitor();
        final ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduler)
                .scheduleWithFixedDelay(runnableArgumentCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        runnableArgumentCaptor.getValue().run();

        verify(mockRunnableCallback, times(0)).run();

        runnableArgumentCaptor.getValue().run();
        verify(mockRunnableCallback, times(1)).run();
    }

    @Test
    public void testInactiveToActiveWorkerMetricsCausesMonitorToSucceed() throws Exception {
        final TestData data1 =
                TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_NOT_MET_WITH_ALL_INACTIVE_WORKER_STATS);
        final TestData data2 = TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET);

        when(mockTimeProvider.call()).thenReturn(80 * 1000L);
        when(mockLeaseRefresher.isLeaseOwnerToLeaseKeyIndexActive()).thenReturn(true);
        when(mockLeaderDecider.isLeader(eq(WORKER_ID))).thenReturn(true);
        when(mockWorkerMetricsDao.getAllWorkerMetricStats())
                .thenReturn(data1.workerMetrics)
                .thenReturn(data2.workerMetrics);
        when(mockLeaseRefresher.listLeases()).thenReturn(data1.leaseList).thenReturn(data2.leaseList);

        monitorUnderTest.startMonitor();
        final ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduler)
                .scheduleWithFixedDelay(runnableArgumentCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        runnableArgumentCaptor.getValue().run();

        verify(mockRunnableCallback, times(0)).run();

        runnableArgumentCaptor.getValue().run();
        verify(mockRunnableCallback, times(1)).run();
    }

    @ParameterizedTest
    @ValueSource(longs = {12, 30, 60, 180})
    public void testTriggerStability(final long stabilityDurationInSeconds) throws Exception {
        monitorUnderTest = new MigrationReadyMonitor(
                nullMetricsFactory,
                mockTimeProvider,
                mockLeaderDecider,
                WORKER_ID,
                mockWorkerMetricsDao,
                WORKER_METRICS_EXPIRY_SECONDS,
                mockLeaseRefresher,
                mockScheduler,
                mockRunnableCallback,
                stabilityDurationInSeconds);

        final TestData data = TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET);

        when(mockLeaseRefresher.isLeaseOwnerToLeaseKeyIndexActive()).thenReturn(true);
        when(mockLeaderDecider.isLeader(eq(WORKER_ID))).thenReturn(true);
        when(mockWorkerMetricsDao.getAllWorkerMetricStats()).thenReturn(data.workerMetrics);
        when(mockLeaseRefresher.listLeases()).thenReturn(data.leaseList);

        monitorUnderTest.startMonitor();
        final ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduler)
                .scheduleWithFixedDelay(runnableArgumentCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        // Test 2: callback is only invoked after trigger being true consecutively for the configured
        // time
        long testTime =
                Duration.ofSeconds(ACTIVE_WORKER_STATS_LAST_UPDATE_TIME - 200).toMillis();
        for (int i = 0; i <= stabilityDurationInSeconds; i++) {
            verify(mockRunnableCallback, times(0)).run();
            when(mockTimeProvider.call()).thenReturn(testTime + i * 1000L);
            runnableArgumentCaptor.getValue().run();
        }
        verify(mockRunnableCallback, times(1)).run();
        reset(mockRunnableCallback);

        // Test 2: If leader changes the timer starts over
        testTime =
                Duration.ofSeconds(ACTIVE_WORKER_STATS_LAST_UPDATE_TIME - 600).toMillis();
        for (int i = 0; i < stabilityDurationInSeconds / 2; i++) {
            verify(mockRunnableCallback, times(0)).run();
            testTime = testTime + 1000L;
            when(mockTimeProvider.call()).thenReturn(testTime);
            runnableArgumentCaptor.getValue().run();

            if (i == stabilityDurationInSeconds / 3) {
                reset(mockLeaderDecider);
                when(mockLeaderDecider.isLeader(eq(WORKER_ID))).thenReturn(false);
            }
        }
        verify(mockRunnableCallback, times(0)).run();
        when(mockLeaderDecider.isLeader(eq(WORKER_ID))).thenReturn(true);
        for (int j = (int) stabilityDurationInSeconds / 2; j <= 3 * stabilityDurationInSeconds / 2; j++) {
            verify(mockRunnableCallback, times(0)).run();
            testTime = testTime + 1000L;
            when(mockTimeProvider.call()).thenReturn(testTime);
            runnableArgumentCaptor.getValue().run();
        }

        verify(mockRunnableCallback, times(1)).run();
        reset(mockRunnableCallback);

        // reset flag by making worker stats expire
        when(mockWorkerMetricsDao.getAllWorkerMetricStats())
                .thenReturn(TEST_DATA_MAP.get(
                                TestDataType.WORKER_READY_CONDITION_NOT_MET_WITH_PARTIAL_INACTIVE_WORKER_STATS)
                        .workerMetrics);
        when(mockLeaseRefresher.listLeases())
                .thenReturn(TEST_DATA_MAP.get(
                                TestDataType.WORKER_READY_CONDITION_NOT_MET_WITH_PARTIAL_INACTIVE_WORKER_STATS)
                        .leaseList);
        testTime = testTime + 1000L;
        when(mockTimeProvider.call()).thenReturn(testTime);
        runnableArgumentCaptor.getValue().run();
        verify(mockRunnableCallback, times(0)).run();

        // Use active worker stats for rest of the test
        when(mockWorkerMetricsDao.getAllWorkerMetricStats()).thenReturn(data.workerMetrics);
        when(mockLeaseRefresher.listLeases()).thenReturn(data.leaseList);

        // Test 2: If trigger toggles back and forth
        // Trigger is true 5 times in a row
        for (int i = 0; i < 5; i++) {
            verify(mockRunnableCallback, times(0)).run();
            testTime = testTime + 1000L;
            when(mockTimeProvider.call()).thenReturn(testTime);
            runnableArgumentCaptor.getValue().run();
        }
        // and then false thrice
        when(mockLeaseRefresher.isLeaseOwnerToLeaseKeyIndexActive()).thenReturn(false);
        for (int i = 0; i < 3; i++) {
            verify(mockRunnableCallback, times(0)).run();
            testTime = testTime + 1000L;
            when(mockTimeProvider.call()).thenReturn(testTime);
            runnableArgumentCaptor.getValue().run();
        }

        // and then true until stabilityDurationInSeconds, but callback should not be invoked
        // until after 8 more invocations.
        when(mockLeaseRefresher.isLeaseOwnerToLeaseKeyIndexActive()).thenReturn(true);
        for (int i = 8; i <= (stabilityDurationInSeconds + 8); i++) {
            verify(mockRunnableCallback, times(0)).run();
            testTime = testTime + 1000L;
            when(mockTimeProvider.call()).thenReturn(testTime);
            runnableArgumentCaptor.getValue().run();
        }
        verify(mockRunnableCallback, times(1)).run();
    }

    /**
     * Test that when workers stats are just expired, its valid for 60 seconds, at 61 seconds
     * it should be considered expired.
     */
    @Test
    public void testWorkerMetricsExpiryBoundaryConditions() throws Exception {
        final TestData data = TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET);
        // Each run of monitor calls timeProvider twice
        when(mockTimeProvider.call())
                .thenReturn(
                        Duration.ofSeconds(ACTIVE_WORKER_STATS_LAST_UPDATE_TIME).toMillis())
                .thenReturn(
                        Duration.ofSeconds(ACTIVE_WORKER_STATS_LAST_UPDATE_TIME).toMillis())
                .thenReturn(Duration.ofSeconds(ACTIVE_WORKER_STATS_LAST_UPDATE_TIME + 59)
                        .toMillis())
                .thenReturn(Duration.ofSeconds(ACTIVE_WORKER_STATS_LAST_UPDATE_TIME + 59)
                        .toMillis())
                .thenReturn(Duration.ofSeconds(ACTIVE_WORKER_STATS_LAST_UPDATE_TIME + 60)
                        .toMillis())
                .thenReturn(Duration.ofSeconds(ACTIVE_WORKER_STATS_LAST_UPDATE_TIME + 60)
                        .toMillis())
                .thenReturn(Duration.ofSeconds(ACTIVE_WORKER_STATS_LAST_UPDATE_TIME + 61)
                        .toMillis());
        when(mockLeaseRefresher.isLeaseOwnerToLeaseKeyIndexActive()).thenReturn(true);
        when(mockLeaderDecider.isLeader(eq(WORKER_ID))).thenReturn(true);
        when(mockWorkerMetricsDao.getAllWorkerMetricStats()).thenReturn(data.workerMetrics);
        when(mockLeaseRefresher.listLeases()).thenReturn(data.leaseList);

        monitorUnderTest.startMonitor();
        final ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduler)
                .scheduleWithFixedDelay(runnableArgumentCaptor.capture(), anyLong(), anyLong(), any(TimeUnit.class));

        // At 0 seconds, WorkerMetricStats are valid
        reset(mockRunnableCallback);
        runnableArgumentCaptor.getValue().run();
        verify(mockRunnableCallback, times(1)).run();

        // At 59 seconds, WorkerMetricStats are valid
        reset(mockRunnableCallback);
        runnableArgumentCaptor.getValue().run();
        verify(mockRunnableCallback, times(1)).run();

        // At 60 seconds, WorkerMetricStats have expired
        reset(mockRunnableCallback);
        runnableArgumentCaptor.getValue().run();
        verify(mockRunnableCallback, times(0)).run();

        // At 61 seconds, WorkerMetricStats have expired
        reset(mockRunnableCallback);
        runnableArgumentCaptor.getValue().run();
        verify(mockRunnableCallback, times(0)).run();
    }

    @RequiredArgsConstructor
    @Getter
    public static class TestData {
        private final List<Lease> leaseList;
        private final List<WorkerMetricStats> workerMetrics;
    }

    private static final long EXPIRED_WORKER_STATS_LAST_UPDATE_TIME = 10L;
    private static final long ACTIVE_WORKER_STATS_LAST_UPDATE_TIME = 10000L;

    public enum TestDataType {
        WORKER_READY_CONDITION_MET,
        WORKER_READY_CONDITION_NOT_MET_WITH_ZERO_WORKER_STATS,
        WORKER_READY_CONDITION_NOT_MET_WITH_PARTIAL_WORKER_STATS,
        WORKER_READY_CONDITION_NOT_MET_WITH_ALL_INACTIVE_WORKER_STATS,
        WORKER_READY_CONDITION_NOT_MET_WITH_PARTIAL_INACTIVE_WORKER_STATS,
        WORKER_READY_CONDITION_NOT_MET_WITH_EXPIRED_LEASES_AND_NO_WORKER_STATS,
        WORKER_READY_CONDITION_NOT_MET_WITH_EXPIRED_LEASES_AND_INACTIVE_WORKER_STATS,
        WORKER_READY_CONDITION_MET_AFTER_EXPIRED_LEASES_ARE_REASSIGNED,
        WORKER_READY_CONDITION_MET_MULTISTREAM_MODE_SANITY,
        WORKER_READY_CONDITION_NOT_MET_MULTISTREAM_MODE_SANITY,
    }

    public static final HashMap<TestDataType, TestData> TEST_DATA_MAP = new HashMap<>();

    @BeforeAll
    public static void populateTestDataMap() {
        final int numWorkers = 10;
        final Random random = new Random();

        TEST_DATA_MAP.put(
                TestDataType.WORKER_READY_CONDITION_MET,
                new TestData(
                        IntStream.range(0, 100)
                                .mapToObj(i -> new Lease(
                                        "shardId-000000000" + i,
                                        "MigrationReadyMonitorTestWorker" + random.nextInt(numWorkers),
                                        random.nextLong(),
                                        UUID.randomUUID(),
                                        System.nanoTime(),
                                        ExtendedSequenceNumber.TRIM_HORIZON,
                                        null,
                                        random.nextLong(),
                                        null,
                                        null,
                                        null,
                                        null))
                                .collect(Collectors.toList()),
                        IntStream.range(0, 10)
                                .mapToObj(i -> WorkerMetricStats.builder()
                                        .workerId("MigrationReadyMonitorTestWorker" + i)
                                        .lastUpdateTime(ACTIVE_WORKER_STATS_LAST_UPDATE_TIME)
                                        .build())
                                .collect(Collectors.toList())));

        TEST_DATA_MAP.put(
                TestDataType.WORKER_READY_CONDITION_NOT_MET_WITH_ZERO_WORKER_STATS,
                new TestData(TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET).leaseList, new ArrayList<>()));

        TEST_DATA_MAP.put(
                TestDataType.WORKER_READY_CONDITION_NOT_MET_WITH_PARTIAL_WORKER_STATS,
                new TestData(
                        TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET).leaseList,
                        IntStream.range(0, 5)
                                .mapToObj(i -> TEST_DATA_MAP
                                        .get(TestDataType.WORKER_READY_CONDITION_MET)
                                        .workerMetrics
                                        .get(random.nextInt(10)))
                                .collect(Collectors.toList())));

        TEST_DATA_MAP.put(
                TestDataType.WORKER_READY_CONDITION_NOT_MET_WITH_ALL_INACTIVE_WORKER_STATS,
                new TestData(
                        TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET).leaseList,
                        IntStream.range(0, 10)
                                .mapToObj(i -> WorkerMetricStats.builder()
                                        .workerId("MigrationReadyMonitorTestWorker" + i)
                                        .lastUpdateTime(EXPIRED_WORKER_STATS_LAST_UPDATE_TIME)
                                        .build())
                                .collect(Collectors.toList())));

        TEST_DATA_MAP.put(
                TestDataType.WORKER_READY_CONDITION_NOT_MET_WITH_PARTIAL_INACTIVE_WORKER_STATS,
                new TestData(
                        TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET).leaseList,
                        IntStream.range(0, 10)
                                .mapToObj(i -> WorkerMetricStats.builder()
                                        .workerId("MigrationReadyMonitorTestWorker" + i)
                                        .lastUpdateTime(
                                                random.nextDouble() > 0.5
                                                        ? EXPIRED_WORKER_STATS_LAST_UPDATE_TIME
                                                        : ACTIVE_WORKER_STATS_LAST_UPDATE_TIME) // Some are active, some
                                        // inactive
                                        .build())
                                .collect(Collectors.toList())));

        final ArrayList<Lease> newLeaseList =
                new ArrayList<>(TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET).leaseList);
        // add some leases for another worker
        IntStream.range(0, 5)
                .mapToObj(i -> new Lease(
                        "shardId-100000000" + i,
                        "ExpiredLeaseWorker",
                        100L,
                        UUID.randomUUID(),
                        100L,
                        ExtendedSequenceNumber.TRIM_HORIZON,
                        null,
                        5L,
                        null,
                        null,
                        null,
                        null))
                .forEach(newLeaseList::add);

        TEST_DATA_MAP.put(
                TestDataType.WORKER_READY_CONDITION_NOT_MET_WITH_EXPIRED_LEASES_AND_NO_WORKER_STATS,
                new TestData(newLeaseList, TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET).workerMetrics));

        final ArrayList<WorkerMetricStats> newWorkerMetrics =
                new ArrayList<>(TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET).workerMetrics);
        newWorkerMetrics.add(WorkerMetricStats.builder()
                .workerId("ExpiredLeaseWorker")
                .lastUpdateTime(EXPIRED_WORKER_STATS_LAST_UPDATE_TIME)
                .build());
        TEST_DATA_MAP.put(
                TestDataType.WORKER_READY_CONDITION_NOT_MET_WITH_EXPIRED_LEASES_AND_INACTIVE_WORKER_STATS,
                new TestData(
                        TEST_DATA_MAP.get(
                                        TestDataType
                                                .WORKER_READY_CONDITION_NOT_MET_WITH_EXPIRED_LEASES_AND_NO_WORKER_STATS)
                                .leaseList,
                        newWorkerMetrics));

        TEST_DATA_MAP.put(
                TestDataType.WORKER_READY_CONDITION_MET_AFTER_EXPIRED_LEASES_ARE_REASSIGNED,
                new TestData(
                        TEST_DATA_MAP
                                .get(
                                        TestDataType
                                                .WORKER_READY_CONDITION_NOT_MET_WITH_EXPIRED_LEASES_AND_NO_WORKER_STATS)
                                .leaseList
                                .stream()
                                .map(lease -> {
                                    final Lease newLease;
                                    if (lease.leaseOwner().equals("ExpiredLeaseWorker")) {
                                        newLease = new Lease(
                                                lease.leaseKey(),
                                                "MigrationReadyMonitorTestWorker" + random.nextInt(numWorkers),
                                                lease.leaseCounter(),
                                                lease.concurrencyToken(),
                                                lease.lastCounterIncrementNanos(),
                                                lease.checkpoint(),
                                                lease.pendingCheckpoint(),
                                                lease.ownerSwitchesSinceCheckpoint(),
                                                lease.parentShardIds(),
                                                lease.childShardIds(),
                                                lease.pendingCheckpointState(),
                                                lease.hashKeyRangeForLease());
                                    } else {
                                        newLease = new Lease(
                                                lease.leaseKey(),
                                                lease.leaseOwner(),
                                                lease.leaseCounter(),
                                                lease.concurrencyToken(),
                                                lease.lastCounterIncrementNanos(),
                                                lease.checkpoint(),
                                                lease.pendingCheckpoint(),
                                                lease.ownerSwitchesSinceCheckpoint(),
                                                lease.parentShardIds(),
                                                lease.childShardIds(),
                                                lease.pendingCheckpointState(),
                                                lease.hashKeyRangeForLease());
                                    }
                                    return newLease;
                                })
                                .collect(Collectors.toList()),
                        TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET).workerMetrics));

        final int numStreams = 3;

        TEST_DATA_MAP.put(
                TestDataType.WORKER_READY_CONDITION_MET_MULTISTREAM_MODE_SANITY,
                new TestData(
                        IntStream.range(0, 100)
                                .mapToObj(i -> {
                                    final int streamId = random.nextInt(numStreams);
                                    final int workerId = random.nextInt(numWorkers);
                                    final MultiStreamLease m = new MultiStreamLease();
                                    m.leaseKey(DUMMY_STREAM_NAME + streamId + ":shardId-00000000" + i);
                                    m.streamIdentifier(DUMMY_STREAM_NAME + streamId);
                                    m.shardId("shardId-00000000" + i);
                                    m.leaseOwner("MigrationReadyMonitorTestWorker" + workerId);
                                    m.leaseCounter(random.nextLong());
                                    m.concurrencyToken(UUID.randomUUID());
                                    m.lastCounterIncrementNanos(System.nanoTime());
                                    m.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                                    m.ownerSwitchesSinceCheckpoint(random.nextLong());
                                    return m;
                                })
                                .collect(Collectors.toList()),
                        TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET).workerMetrics));

        TEST_DATA_MAP.put(
                TestDataType.WORKER_READY_CONDITION_NOT_MET_MULTISTREAM_MODE_SANITY,
                new TestData(
                        TEST_DATA_MAP.get(TestDataType.WORKER_READY_CONDITION_MET_MULTISTREAM_MODE_SANITY).leaseList,
                        IntStream.range(0, 5)
                                .mapToObj(i -> TEST_DATA_MAP
                                        .get(TestDataType.WORKER_READY_CONDITION_MET_MULTISTREAM_MODE_SANITY)
                                        .workerMetrics
                                        .get(random.nextInt(10)))
                                .collect(Collectors.toList())));
    }
}
