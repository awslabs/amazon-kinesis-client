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

import java.util.AbstractMap.SimpleEntry;
import java.util.Random;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.kinesis.coordinator.CoordinatorConfig.ClientVersionConfig;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.leases.exceptions.DependencyException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.coordinator.migration.MigrationState.MIGRATION_HASH_KEY;

/**
 * Tests for {@link MigrationClientVersionStateInitializer}
 */
public class MigrationClientVersionStateInitializerTest {

    private static final String WORKER_ID = "testWorker";

    private final Callable<Long> mockTimeProvider = mock(Callable.class);
    private final CoordinatorStateDAO mockCoordinatorStateDAO = mock(CoordinatorStateDAO.class, Mockito.RETURNS_MOCKS);
    private final Random mockRandom = mock(Random.class);

    @BeforeEach
    public void setup() throws Exception {
        when(mockTimeProvider.call()).thenReturn(1000L);
        when(mockRandom.nextDouble()).thenReturn(0.5);
    }

    private MigrationClientVersionStateInitializer createInitializer(final ClientVersionConfig config) {
        return new MigrationClientVersionStateInitializer(mockCoordinatorStateDAO, config, mockRandom, WORKER_ID);
    }

    // ========================
    // Phase 1 (COMPATIBLE_WITH_2X_PHASE1) tests
    // ========================

    @Test
    public void testPhase1Config_noStateInDDB_returnsClientVersionInit() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(null);

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1);

        final SimpleEntry<ClientVersion, MigrationState> result = initializer.getInitialState();

        assertEquals(ClientVersion.CLIENT_VERSION_INIT, result.getKey());
        // No DDB write should occur for Phase 1
        verify(mockCoordinatorStateDAO, never()).createCoordinatorStateIfNotExists(any());
        verify(mockCoordinatorStateDAO, never()).updateCoordinatorStateWithExpectation(any(), any());
    }

    @Test
    public void testPhase1Config_withExisting3xWithRollbackState_returns3xWithRollback() throws Exception {
        final MigrationState existingState =
                new MigrationState(WORKER_ID).update(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(existingState);

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1);

        final SimpleEntry<ClientVersion, MigrationState> result = initializer.getInitialState();

        assertEquals(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK, result.getKey());
    }

    @Test
    public void testPhase1Config_withExisting2xState_returns2x() throws Exception {
        final MigrationState existingState =
                new MigrationState(WORKER_ID).update(ClientVersion.CLIENT_VERSION_2X, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(existingState);

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1);

        final SimpleEntry<ClientVersion, MigrationState> result = initializer.getInitialState();

        assertEquals(ClientVersion.CLIENT_VERSION_2X, result.getKey());
    }

    @Test
    public void testPhase1Config_withExistingUpgradeFrom2xState_returnsUpgradeFrom2x() throws Exception {
        final MigrationState existingState =
                new MigrationState(WORKER_ID).update(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(existingState);

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1);

        final SimpleEntry<ClientVersion, MigrationState> result = initializer.getInitialState();

        assertEquals(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, result.getKey());
    }

    @Test
    public void testPhase1Config_withExisting3xState_returns3x() throws Exception {
        final MigrationState existingState =
                new MigrationState(WORKER_ID).update(ClientVersion.CLIENT_VERSION_3X, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(existingState);

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1);

        final SimpleEntry<ClientVersion, MigrationState> result = initializer.getInitialState();

        assertEquals(ClientVersion.CLIENT_VERSION_3X, result.getKey());
    }

    // ========================
    // Phase 2 (COMPATIBLE_WITH_2X) tests
    // ========================

    @Test
    public void testPhase2Config_noStateInDDB_returnsUpgradeFrom2x() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(null);

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);

        final SimpleEntry<ClientVersion, MigrationState> result = initializer.getInitialState();

        assertEquals(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, result.getKey());
    }

    @Test
    public void testPhase2Config_with3xWithRollbackState_returns3xWithRollback() throws Exception {
        final MigrationState existingState =
                new MigrationState(WORKER_ID).update(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(existingState);

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);

        final SimpleEntry<ClientVersion, MigrationState> result = initializer.getInitialState();

        assertEquals(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK, result.getKey());
    }

    // ========================
    // Phase 3 (3X) tests
    // ========================

    @Test
    public void testPhase3Config_noStateInDDB_returns3x() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(null);

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_3X);

        final SimpleEntry<ClientVersion, MigrationState> result = initializer.getInitialState();

        assertEquals(ClientVersion.CLIENT_VERSION_3X, result.getKey());
    }

    @Test
    public void testPhase3Config_with3xWithRollbackState_returns3x() throws Exception {
        final MigrationState existingState =
                new MigrationState(WORKER_ID).update(ClientVersion.CLIENT_VERSION_3X_WITH_ROLLBACK, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(existingState);

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_3X);

        final SimpleEntry<ClientVersion, MigrationState> result = initializer.getInitialState();

        assertEquals(ClientVersion.CLIENT_VERSION_3X, result.getKey());
    }

    @Test
    public void testPhase3Config_with2xState_returns2x() throws Exception {
        final MigrationState existingState =
                new MigrationState(WORKER_ID).update(ClientVersion.CLIENT_VERSION_2X, WORKER_ID);
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY)).thenReturn(existingState);

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_3X);

        final SimpleEntry<ClientVersion, MigrationState> result = initializer.getInitialState();

        assertEquals(ClientVersion.CLIENT_VERSION_2X, result.getKey());
    }

    // ========================
    // Error handling tests
    // ========================

    @Test
    public void testGetInitialState_throwsWhenDDBReadFailsRepeatedly() throws Exception {
        when(mockCoordinatorStateDAO.getCoordinatorState(MIGRATION_HASH_KEY))
                .thenThrow(new DependencyException(new RuntimeException("DDB unavailable")));

        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1);

        // The retry loop will exhaust 10 retries and throw RuntimeException
        assertThrows(RuntimeException.class, initializer::getInitialState);
    }

    // ========================
    // getClientVersionForInitialization unit tests
    // ========================

    @Test
    public void testGetClientVersionForInitialization_initState_phase1Config_returnsInit() {
        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X_PHASE1);
        final MigrationState initState = new MigrationState(WORKER_ID);

        final ClientVersion result = initializer.getClientVersionForInitialization(initState);

        assertEquals(ClientVersion.CLIENT_VERSION_INIT, result);
    }

    @Test
    public void testGetClientVersionForInitialization_initState_phase2Config_returnsUpgradeFrom2x() {
        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);
        final MigrationState initState = new MigrationState(WORKER_ID);

        final ClientVersion result = initializer.getClientVersionForInitialization(initState);

        assertEquals(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X, result);
    }

    @Test
    public void testGetClientVersionForInitialization_initState_phase3Config_returns3x() {
        final MigrationClientVersionStateInitializer initializer =
                createInitializer(ClientVersionConfig.CLIENT_VERSION_CONFIG_3X);
        final MigrationState initState = new MigrationState(WORKER_ID);

        final ClientVersion result = initializer.getClientVersionForInitialization(initState);

        assertEquals(ClientVersion.CLIENT_VERSION_3X, result);
    }
}
