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

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.migration.ClientVersionChangeMonitor.ClientVersionChangeCallback;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.coordinator.migration.MigrationState.MIGRATION_HASH_KEY;

public class ClientVersionChangeMonitorTest {
    private ClientVersionChangeMonitor monitorUnderTest;

    private final MetricsFactory nullMetricsFactory = new NullMetricsFactory();
    private final CoordinatorStateDAO mockCoordinatorStateDAO =
            Mockito.mock(CoordinatorStateDAO.class, Mockito.RETURNS_MOCKS);
    private final ScheduledExecutorService mockScheduler =
            Mockito.mock(ScheduledExecutorService.class, Mockito.RETURNS_MOCKS);
    private final ClientVersionChangeCallback mockCallback =
            Mockito.mock(ClientVersionChangeCallback.class, Mockito.RETURNS_MOCKS);
    private final Random mockRandom = Mockito.mock(Random.class, Mockito.RETURNS_MOCKS);

    @BeforeEach
    public void setup() {
        when(mockRandom.nextDouble()).thenReturn(0.0);
    }

    @ParameterizedTest
    @CsvSource({
        "CLIENT_VERSION_2X, CLIENT_VERSION_UPGRADE_FROM_2X",
        "CLIENT_VERSION_3X_WITH_ROLLBACK, CLIENT_VERSION_2X",
        "CLIENT_VERSION_UPGRADE_FROM_2X, CLIENT_VERSION_3X_WITH_ROLLBACK",
        "CLIENT_VERSION_3X_WITH_ROLLBACK, CLIENT_VERSION_3X"
    })
    public void testMonitor(final ClientVersion currentClientVersion, final ClientVersion changedClientVersion)
            throws Exception {
        monitorUnderTest = new ClientVersionChangeMonitor(
                nullMetricsFactory,
                mockCoordinatorStateDAO,
                mockScheduler,
                mockCallback,
                currentClientVersion,
                mockRandom);

        monitorUnderTest.startMonitor();
        final ArgumentCaptor<Runnable> argumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduler).scheduleWithFixedDelay(argumentCaptor.capture(), anyLong(), anyLong(), anyObject());

        final MigrationState initialState =
                new MigrationState(MIGRATION_HASH_KEY, "DUMMY_WORKER").update(currentClientVersion, "DUMMY_WORKER");
        final MigrationState changedState = initialState.copy().update(changedClientVersion, "DUMMY_WORKER2");
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY))
                .thenReturn(initialState)
                .thenReturn(changedState);
        argumentCaptor.getValue().run();

        verify(mockCallback, never()).accept(anyObject());

        argumentCaptor.getValue().run();

        final ArgumentCaptor<MigrationState> stateCaptor = ArgumentCaptor.forClass(MigrationState.class);
        verify(mockCallback, times(1)).accept(stateCaptor.capture());

        Assertions.assertEquals(changedClientVersion, stateCaptor.getValue().getClientVersion());
    }

    @Test
    public void testCallIsInvokedOnlyOnceIfSuccessful() throws Exception {
        monitorUnderTest = new ClientVersionChangeMonitor(
                nullMetricsFactory,
                mockCoordinatorStateDAO,
                mockScheduler,
                mockCallback,
                ClientVersion.CLIENT_VERSION_2X,
                mockRandom);

        monitorUnderTest.startMonitor();
        final ArgumentCaptor<Runnable> argumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduler).scheduleWithFixedDelay(argumentCaptor.capture(), anyLong(), anyLong(), anyObject());

        final MigrationState state = new MigrationState(MIGRATION_HASH_KEY, "DUMMY_WORKER")
                .update(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, "DUMMY_WORKER");
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(state);

        argumentCaptor.getValue().run();
        verify(mockCallback, times(1)).accept(anyObject());
        reset(mockCallback);

        argumentCaptor.getValue().run();
        verify(mockCallback, never()).accept(anyObject());
    }

    @Test
    public void testCallIsInvokedAgainIfFailed() throws Exception {
        monitorUnderTest = new ClientVersionChangeMonitor(
                nullMetricsFactory,
                mockCoordinatorStateDAO,
                mockScheduler,
                mockCallback,
                ClientVersion.CLIENT_VERSION_2X,
                mockRandom);

        monitorUnderTest.startMonitor();
        final ArgumentCaptor<Runnable> argumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockScheduler).scheduleWithFixedDelay(argumentCaptor.capture(), anyLong(), anyLong(), anyObject());

        final MigrationState state = new MigrationState(MIGRATION_HASH_KEY, "DUMMY_WORKER")
                .update(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, "DUMMY_WORKER");
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(state);

        doThrow(new InvalidStateException("test exception")).when(mockCallback).accept(any());

        argumentCaptor.getValue().run();
        verify(mockCallback, times(1)).accept(anyObject());
        reset(mockCallback);

        argumentCaptor.getValue().run();
        verify(mockCallback, times(1)).accept(anyObject());
        reset(mockCallback);

        argumentCaptor.getValue().run();
        verify(mockCallback, times(0)).accept(anyObject());
    }
}
