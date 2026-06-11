package software.amazon.kinesis.coordinator.assignment;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LAMDataSnapshotTest {

    private Lease createLease(String leaseKey, String owner) {
        Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.leaseOwner(owner);
        lease.leaseCounter(1L);
        return lease;
    }

    private WorkerMetricStats createWorkerMetrics(String workerId) {
        return WorkerMetricStats.LegacyWorkerMetricStats.builder()
                .workerId(workerId)
                .lastUpdateTime(Instant.now().getEpochSecond())
                .metricStats(ImmutableMap.of("C", ImmutableList.of(50D, 50D)))
                .operatingRange(ImmutableMap.of("C", ImmutableList.of(80L)))
                .build();
    }

    @Test
    void builder_withAllFields_createsSnapshot() {
        List<Lease> leases = Arrays.asList(createLease("l1", "w1"), createLease("l2", "w2"));
        List<WorkerMetricStats> metrics = Arrays.asList(createWorkerMetrics("w1"), createWorkerMetrics("w2"));
        List<String> failures = Arrays.asList("badKey1", "badKey2");

        LAMDataSnapshot snapshot = LAMDataSnapshot.builder()
                .leases(leases)
                .workerMetricStats(metrics)
                .leaseDeserializationFailures(failures)
                .build();

        assertEquals(2, snapshot.getLeases().size());
        assertEquals(2, snapshot.getWorkerMetricStats().size());
        assertEquals(2, snapshot.getLeaseDeserializationFailures().size());
        assertEquals("l1", snapshot.getLeases().get(0).leaseKey());
        assertEquals("w1", snapshot.getWorkerMetricStats().get(0).getWorkerId());
        assertEquals("badKey1", snapshot.getLeaseDeserializationFailures().get(0));
    }

    @Test
    void builder_emptyLists_createsEmptySnapshot() {
        LAMDataSnapshot snapshot = LAMDataSnapshot.builder()
                .leases(Collections.emptyList())
                .workerMetricStats(Collections.emptyList())
                .leaseDeserializationFailures(Collections.emptyList())
                .build();

        assertNotNull(snapshot);
        assertTrue(snapshot.getLeases().isEmpty());
        assertTrue(snapshot.getWorkerMetricStats().isEmpty());
        assertTrue(snapshot.getLeaseDeserializationFailures().isEmpty());
    }

    @Test
    void builder_nullLists_allowsNull() {
        LAMDataSnapshot snapshot = LAMDataSnapshot.builder()
                .leases(null)
                .workerMetricStats(null)
                .leaseDeserializationFailures(null)
                .build();

        assertNotNull(snapshot);
        // null is allowed by lombok @Value @Builder
        assertEquals(null, snapshot.getLeases());
        assertEquals(null, snapshot.getWorkerMetricStats());
        assertEquals(null, snapshot.getLeaseDeserializationFailures());
    }

    @Test
    void builder_singleLease_snapshotAccessible() {
        Lease lease = createLease("singleLease", "owner1");

        LAMDataSnapshot snapshot = LAMDataSnapshot.builder()
                .leases(Collections.singletonList(lease))
                .workerMetricStats(Collections.emptyList())
                .leaseDeserializationFailures(Collections.emptyList())
                .build();

        assertEquals(1, snapshot.getLeases().size());
        assertEquals("singleLease", snapshot.getLeases().get(0).leaseKey());
        assertEquals("owner1", snapshot.getLeases().get(0).leaseOwner());
    }

    @Test
    void builder_duplicateWorkerMetrics_bothRetained() {
        // LAMDataSnapshot allows duplicate worker IDs (from different tables during migration)
        WorkerMetricStats wm1 = createWorkerMetrics("worker1");
        WorkerMetricStats wm2 = createWorkerMetrics("worker1");

        LAMDataSnapshot snapshot = LAMDataSnapshot.builder()
                .leases(Collections.emptyList())
                .workerMetricStats(Arrays.asList(wm1, wm2))
                .leaseDeserializationFailures(Collections.emptyList())
                .build();

        assertEquals(2, snapshot.getWorkerMetricStats().size());
        assertEquals("worker1", snapshot.getWorkerMetricStats().get(0).getWorkerId());
        assertEquals("worker1", snapshot.getWorkerMetricStats().get(1).getWorkerId());
    }

    @Test
    void builder_onlyDeserializationFailures_validSnapshot() {
        LAMDataSnapshot snapshot = LAMDataSnapshot.builder()
                .leases(Collections.emptyList())
                .workerMetricStats(Collections.emptyList())
                .leaseDeserializationFailures(Arrays.asList("key1", "key2", "key3"))
                .build();

        assertEquals(0, snapshot.getLeases().size());
        assertEquals(0, snapshot.getWorkerMetricStats().size());
        assertEquals(3, snapshot.getLeaseDeserializationFailures().size());
    }
}
