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
package software.amazon.kinesis.leases.dynamodb;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider;
import software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider.LeaseAssignmentMode;
import software.amazon.kinesis.coordinator.streamInfo.StreamIdCacheManager;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.ShardConsumer;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

class DynamoDBLeaseCoordinatorTest {

    private static final String WORKER_ID = "testWorker";
    private static final long LEASE_DURATION_MILLIS = 10000L;
    private static final long EPSILON_MILLIS = 25L;
    private static final int MAX_LEASES_FOR_WORKER = Integer.MAX_VALUE;
    private static final int MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1;
    private static final int MAX_LEASE_RENEWER_THREAD_COUNT = 20;
    private static final long INITIAL_LEASE_TABLE_READ_CAPACITY = 10L;
    private static final long INITIAL_LEASE_TABLE_WRITE_CAPACITY = 10L;

    @Mock
    private LeaseRefresher mockLeaseRefresher;

    @Mock
    private StreamIdCacheManager mockStreamIdCacheManager;

    @Mock
    private MigrationAdaptiveLeaseAssignmentModeProvider mockModeProvider;

    private DynamoDBLeaseCoordinator coordinator;

    @BeforeEach
    void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(mockLeaseRefresher.listLeases()).thenReturn(Collections.emptyList());

        coordinator = new DynamoDBLeaseCoordinator(
                mockLeaseRefresher,
                WORKER_ID,
                LEASE_DURATION_MILLIS,
                LeaseManagementConfig.DEFAULT_ENABLE_PRIORITY_LEASE_ASSIGNMENT,
                EPSILON_MILLIS,
                MAX_LEASES_FOR_WORKER,
                MAX_LEASES_TO_STEAL_AT_ONE_TIME,
                MAX_LEASE_RENEWER_THREAD_COUNT,
                INITIAL_LEASE_TABLE_READ_CAPACITY,
                INITIAL_LEASE_TABLE_WRITE_CAPACITY,
                new NullMetricsFactory(),
                new LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig(),
                LeaseManagementConfig.GracefulLeaseHandoffConfig.builder().build(),
                new ConcurrentHashMap<ShardInfo, ShardConsumer>(),
                2 * LEASE_DURATION_MILLIS,
                mockStreamIdCacheManager,
                0);
    }

    @AfterEach
    void tearDown() {
        coordinator.stop();
    }

    /**
     * Test that lease taker thread is started when dynamicModeChangeSupportNeeded is true
     * (migration/dual mode scenario).
     */
    @Test
    void start_withDynamicModeChangeSupport_startsTakerThread() throws Exception {
        when(mockModeProvider.dynamicModeChangeSupportNeeded()).thenReturn(true);
        when(mockModeProvider.getLeaseAssignmentMode())
                .thenReturn(LeaseAssignmentMode.DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT);

        coordinator.start(mockModeProvider);

        assertNotNull(getTakerFuture(), "Taker future should be set when dynamicModeChangeSupportNeeded is true");
    }

    /**
     * Test that lease taker thread is started when dynamicModeChangeSupportNeeded is false
     * but mode is DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT (Phase 1 scenario).
     */
    @Test
    void start_withPhase1DefaultLeaseCountMode_startsTakerThread() throws Exception {
        when(mockModeProvider.dynamicModeChangeSupportNeeded()).thenReturn(false);
        when(mockModeProvider.getLeaseAssignmentMode())
                .thenReturn(LeaseAssignmentMode.DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT);

        coordinator.start(mockModeProvider);

        assertNotNull(
                getTakerFuture(),
                "Taker future should be set when mode is DEFAULT_LEASE_COUNT_BASED_ASSIGNMENT even without dynamic support");
    }

    /**
     * Test that lease taker thread is NOT started when dynamicModeChangeSupportNeeded is false
     * and mode is WORKER_UTILIZATION_AWARE_ASSIGNMENT (fully migrated 3.x scenario).
     */
    @Test
    void start_withWorkerUtilAwareAndNoDynamicSupport_doesNotStartTakerThread() throws Exception {
        when(mockModeProvider.dynamicModeChangeSupportNeeded()).thenReturn(false);
        when(mockModeProvider.getLeaseAssignmentMode())
                .thenReturn(LeaseAssignmentMode.WORKER_UTILIZATION_AWARE_ASSIGNMENT);

        coordinator.start(mockModeProvider);

        assertNull(
                getTakerFuture(),
                "Taker future should NOT be set when mode is WORKER_UTILIZATION_AWARE_ASSIGNMENT without dynamic support");
    }

    /**
     * Test that lease taker thread is started when dynamicModeChangeSupportNeeded is true
     * and mode is WORKER_UTILIZATION_AWARE_ASSIGNMENT (dual mode with 3.x assignment active).
     */
    @Test
    void start_withDynamicSupportAndWorkerUtilAwareMode_startsTakerThread() throws Exception {
        when(mockModeProvider.dynamicModeChangeSupportNeeded()).thenReturn(true);
        when(mockModeProvider.getLeaseAssignmentMode())
                .thenReturn(LeaseAssignmentMode.WORKER_UTILIZATION_AWARE_ASSIGNMENT);

        coordinator.start(mockModeProvider);

        assertNotNull(
                getTakerFuture(),
                "Taker future should be set when dynamicModeChangeSupportNeeded is true regardless of current mode");
    }

    private ScheduledFuture<?> getTakerFuture() throws Exception {
        Field field = DynamoDBLeaseCoordinator.class.getDeclaredField("takerFuture");
        field.setAccessible(true);
        return (ScheduledFuture<?>) field.get(coordinator);
    }
}
