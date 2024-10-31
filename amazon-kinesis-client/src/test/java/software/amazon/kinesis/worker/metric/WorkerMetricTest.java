package software.amazon.kinesis.worker.metric;

import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;

class WorkerMetricTest {

    @Test
    void testWorkerMetricValueBuildValidCases() {
        // build method does not fail means test is valid
        WorkerMetric.WorkerMetricValue.builder().value(0D).build();
        WorkerMetric.WorkerMetricValue.builder().value(0.000001D).build();
        WorkerMetric.WorkerMetricValue.builder().value(50D).build();
        WorkerMetric.WorkerMetricValue.builder().value(100D).build();
        WorkerMetric.WorkerMetricValue.builder().value(99.00001D).build();

        assertFailure(() -> WorkerMetric.WorkerMetricValue.builder().value(-1D).build(), -1D);
        assertFailure(
                () -> WorkerMetric.WorkerMetricValue.builder().value(100.00001D).build(), 100.00001D);
    }

    private void assertFailure(final Supplier<WorkerMetric.WorkerMetricValue> supplier, final double value) {
        try {
            supplier.get();
            throw new RuntimeException("If call reached here that means its a fail");
        } catch (final Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }
}
