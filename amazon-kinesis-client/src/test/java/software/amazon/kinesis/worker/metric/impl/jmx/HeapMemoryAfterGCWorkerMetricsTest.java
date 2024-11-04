package software.amazon.kinesis.worker.metric.impl.jmx;

import org.junit.jupiter.api.Test;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetricType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class HeapMemoryAfterGCWorkerMetricsTest {

    @Test
    void capture_sanity() {
        final HeapMemoryAfterGCWorkerMetric heapMemoryAfterGCWorkerMetric = new HeapMemoryAfterGCWorkerMetric(
                OperatingRange.builder().maxUtilization(100).build());

        assertNotNull(heapMemoryAfterGCWorkerMetric.capture().getValue());

        assertEquals(WorkerMetricType.MEMORY, heapMemoryAfterGCWorkerMetric.getWorkerMetricType());
        assertEquals(WorkerMetricType.MEMORY.getShortName(), heapMemoryAfterGCWorkerMetric.getShortName());
    }
}
