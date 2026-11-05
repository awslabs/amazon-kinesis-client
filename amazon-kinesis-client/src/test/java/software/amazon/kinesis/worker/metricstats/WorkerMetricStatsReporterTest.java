package software.amazon.kinesis.worker.metricstats;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.segmenting.FleetSegmentingHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerMetricStatsReporterTest {

    private static final String WORKER_ID = "worker1";

    private WorkerMetricStatsManager metricsManager;
    private WorkerMetricStatsDAO metricsDAO;
    private FleetSegmentingHandler segmentingHandler;
    private WorkerMetricStatsReporter reporter;

    @BeforeEach
    void setup() {
        metricsManager = mock(WorkerMetricStatsManager.class);
        metricsDAO = mock(WorkerMetricStatsDAO.class);
        segmentingHandler = mock(FleetSegmentingHandler.class);
        when(metricsManager.computeMetrics()).thenReturn(Collections.emptyMap());
        when(metricsManager.getOperatingRange()).thenReturn(Collections.emptyMap());
        reporter = new WorkerMetricStatsReporter(
                new NullMetricsFactory(), WORKER_ID, metricsManager, metricsDAO, segmentingHandler);
    }

    /** With segmenting off, the reporter writes the same entry as before segmenting. */
    @Test
    void run_segmentingDisabled_leavesPropertiesUnset() throws Exception {
        when(segmentingHandler.isEnabled()).thenReturn(false);

        reporter.run();

        assertNull(captureReported().getProperties());
        verify(segmentingHandler, never()).generateVersionHashWithLastUpdatedTimeMap();
    }

    @Test
    void run_segmentingEnabled_reportsVersionHash() throws Exception {
        final Map<String, String> props = Collections.singletonMap(FleetSegmentingHandler.VERSION_HASH_KEY, "v1");
        when(segmentingHandler.isEnabled()).thenReturn(true);
        when(segmentingHandler.generateVersionHashWithLastUpdatedTimeMap()).thenReturn(props);

        reporter.run();

        assertEquals(props, captureReported().getProperties());
    }

    private WorkerMetricStats captureReported() throws Exception {
        final ArgumentCaptor<WorkerMetricStats> captor = ArgumentCaptor.forClass(WorkerMetricStats.class);
        verify(metricsDAO).updateMetrics(captor.capture());
        return captor.getValue();
    }
}
