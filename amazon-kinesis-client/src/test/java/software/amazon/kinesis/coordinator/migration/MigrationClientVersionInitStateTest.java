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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.coordinator.DynamicMigrationComponentsInitializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link MigrationClientVersionInitState}
 */
public class MigrationClientVersionInitStateTest {

    private DynamicMigrationComponentsInitializer mockInitializer;
    private MigrationClientVersionInitState state;

    @BeforeEach
    public void setup() {
        mockInitializer = mock(DynamicMigrationComponentsInitializer.class);
        state = new MigrationClientVersionInitState(mockInitializer);
    }

    @Test
    public void testClientVersion_returnsInit() {
        assertEquals(ClientVersion.CLIENT_VERSION_INIT, state.clientVersion());
    }

    @Test
    public void testEnter_callsInitializeClientVersionForPhase1() throws DependencyException {
        state.enter(ClientVersion.CLIENT_VERSION_INIT);

        verify(mockInitializer, times(1)).initializeClientVersionForPhase1();
    }

    @Test
    public void testEnter_idempotent_doesNotReinitialize() throws DependencyException {
        state.enter(ClientVersion.CLIENT_VERSION_INIT);
        state.enter(ClientVersion.CLIENT_VERSION_INIT);

        // Should only be called once due to the entered flag
        verify(mockInitializer, times(1)).initializeClientVersionForPhase1();
    }

    @Test
    public void testEnter_fromAnyClientVersion_callsPhase1Init() throws DependencyException {
        state.enter(ClientVersion.CLIENT_VERSION_UPGRADE_FROM_2X);

        verify(mockInitializer, times(1)).initializeClientVersionForPhase1();
    }

    @Test
    public void testLeave_doesNotThrow() throws DependencyException {
        state.enter(ClientVersion.CLIENT_VERSION_INIT);
        // leave() should not throw
        state.leave();
    }

    @Test
    public void testLeave_withoutEnter_doesNotThrow() {
        // leave() should be safe to call even without enter
        state.leave();
    }

    @Test
    public void testEnter_thenLeave_thenEnter_doesNotReinitialize() throws DependencyException {
        state.enter(ClientVersion.CLIENT_VERSION_INIT);
        state.leave();
        // Even after leave, re-entering should not reinitialize (entered flag is permanent)
        state.enter(ClientVersion.CLIENT_VERSION_INIT);

        verify(mockInitializer, times(1)).initializeClientVersionForPhase1();
    }
}
