package software.amazon.kinesis.leader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.coordinator.MigrationAdaptiveLeaseAssignmentModeProvider;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MigrationAdaptiveLeaderDeciderTest {

    private static final String TEST_RANDOM_WORKER_ID = "IAmRandomWorkerId";

    @Mock
    private LeaderDecider kcl3xLeaderDecider;

    @Mock
    private LeaderDecider kcl2xLeaderDecider;

    private MigrationAdaptiveLeaderDecider migrationAdaptiveLeaderDecider;

    @BeforeEach
    void setup() {
        MockitoAnnotations.initMocks(this);
    }

    private void createLeaderDecider(final LeaderDecider currentLeaderDecider) {
        this.migrationAdaptiveLeaderDecider = new MigrationAdaptiveLeaderDecider(new NullMetricsFactory());
        this.migrationAdaptiveLeaderDecider.updateLeaderDecider(currentLeaderDecider);
    }

    @Test
    void isLeader_modeProviderReturnsKCL3_X_assertNewLeaderDecideInvoked() {
        when(kcl3xLeaderDecider.isLeader(anyString())).thenReturn(true);
        when(kcl2xLeaderDecider.isLeader(anyString())).thenReturn(false);

        createLeaderDecider(kcl3xLeaderDecider);
        final boolean response = migrationAdaptiveLeaderDecider.isLeader(TEST_RANDOM_WORKER_ID);

        assertTrue(response, "kcl3_xLeaderDecider didn't elected leader");
        verify(kcl3xLeaderDecider, times(1)).isLeader(anyString());
        verify(kcl2xLeaderDecider, times(0)).isLeader(anyString());
    }

    @Test
    void isLeader_modeProviderReturnsKCL2_X_assertNewLeaderDecideInvoked() {
        when(kcl3xLeaderDecider.isLeader(anyString())).thenReturn(false);
        when(kcl2xLeaderDecider.isLeader(anyString())).thenReturn(true);

        createLeaderDecider(kcl2xLeaderDecider);
        final boolean response = migrationAdaptiveLeaderDecider.isLeader(TEST_RANDOM_WORKER_ID);

        assertTrue(response, "kcl2_xLeaderDecider didn't elected leader");
        verify(kcl3xLeaderDecider, times(0)).isLeader(anyString());
        verify(kcl2xLeaderDecider, times(1)).isLeader(anyString());
    }

    @Test
    void isLeader_transitionFromKCL2_XTo3_X_assertSwitchInTransition() {
        // kcl3_xLeaderDecider returns true always in this mock
        when(kcl3xLeaderDecider.isLeader(anyString())).thenReturn(true);
        // kcl2_xLeaderDecider returns false always in this mock
        when(kcl2xLeaderDecider.isLeader(anyString())).thenReturn(false);
        final MigrationAdaptiveLeaseAssignmentModeProvider mockModeProvider =
                mock(MigrationAdaptiveLeaseAssignmentModeProvider.class);
        createLeaderDecider(kcl2xLeaderDecider);

        final boolean responseFirst = migrationAdaptiveLeaderDecider.isLeader(TEST_RANDOM_WORKER_ID);
        final boolean responseSecond = migrationAdaptiveLeaderDecider.isLeader(TEST_RANDOM_WORKER_ID);
        assertFalse(responseFirst);
        assertFalse(responseSecond);

        // validate 2 calls to kcl2_xLeaderDecider and 0 calls gone to kcl3_xLeaderDecider
        verify(kcl3xLeaderDecider, times(0)).isLeader(anyString());
        verify(kcl2xLeaderDecider, times(2)).isLeader(anyString());

        migrationAdaptiveLeaderDecider.updateLeaderDecider(kcl3xLeaderDecider);

        final boolean responseThird = migrationAdaptiveLeaderDecider.isLeader(TEST_RANDOM_WORKER_ID);
        final boolean responseForth = migrationAdaptiveLeaderDecider.isLeader(TEST_RANDOM_WORKER_ID);
        assertTrue(responseThird);
        assertTrue(responseForth);

        // validate no more call to kcl2_xLeaderDecider and 2 calls gone to kcl3_xLeaderDecider
        verify(kcl3xLeaderDecider, times(2)).isLeader(anyString());
        verify(kcl2xLeaderDecider, times(2)).isLeader(anyString());

        // Both LD as initialized once, kcl2_xLeaderDecider as initial mode and kcl3_xLeaderDecider after switch and
        // only once.
        verify(kcl2xLeaderDecider, times(1)).initialize();
        verify(kcl3xLeaderDecider, times(1)).initialize();

        // As the mode has changed, validate shutdown is called for kcl2_xLeaderDecider
        verify(kcl2xLeaderDecider, times(1)).shutdown();
        verify(kcl3xLeaderDecider, times(0)).shutdown();
    }
}
