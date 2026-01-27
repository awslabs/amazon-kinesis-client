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

package software.amazon.kinesis.coordinator.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.kinesis.leases.Lease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LeaseCountBasedLeaseAssignmentDeciderTest {

    @Mock
    private LeaseAssignmentManager.InMemoryStorageView inMemoryStorageView;

    private LeaseCountBasedLeaseAssignmentDecider decider;
    private static final int MAX_LEASES_FOR_WORKER = 10;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        decider = new LeaseCountBasedLeaseAssignmentDecider(inMemoryStorageView, MAX_LEASES_FOR_WORKER);
    }

    @Test
    void assignExpiredOrUnassignedLeases_emptyList_noAssignments() {
        List<Lease> emptyList = new ArrayList<>();
        when(inMemoryStorageView.getWorkerToLeasesMap()).thenReturn(new HashMap<>());
        when(inMemoryStorageView.getActiveWorkerIdSet()).thenReturn(new HashSet<>());
        when(inMemoryStorageView.getLeaseList()).thenReturn(new ArrayList<>());

        decider.assignExpiredOrUnassignedLeases(emptyList);

        verify(inMemoryStorageView, never()).performLeaseAssignment(any(), any());
    }

    @Test
    void assignExpiredOrUnassignedLeases_singleWorker_assignsAllLeases() {
        // Setup
        Lease lease1 = createLease("lease1", null, 100L);
        Lease lease2 = createLease("lease2", null, 200L);
        List<Lease> unassignedLeases = new ArrayList<>(Arrays.asList(lease1, lease2));

        Map<String, Set<Lease>> workerToLeasesMap = new HashMap<>();
        workerToLeasesMap.put("worker1", new HashSet<>());

        when(inMemoryStorageView.getWorkerToLeasesMap()).thenReturn(workerToLeasesMap);
        when(inMemoryStorageView.getActiveWorkerIdSet()).thenReturn(Collections.singleton("worker1"));
        when(inMemoryStorageView.getLeaseList()).thenReturn(Arrays.asList(lease1, lease2));

        // Execute
        decider.assignExpiredOrUnassignedLeases(unassignedLeases);

        // Verify
        verify(inMemoryStorageView).performLeaseAssignment(lease1, "worker1");
        verify(inMemoryStorageView).performLeaseAssignment(lease2, "worker1");
        assertTrue(unassignedLeases.isEmpty());
    }

    @Test
    void assignExpiredOrUnassignedLeases_multipleWorkers_distributesEvenly() {
        // Setup
        Lease lease1 = createLease("lease1", null, 100L);
        Lease lease2 = createLease("lease2", null, 200L);
        Lease lease3 = createLease("lease3", null, 300L);
        List<Lease> unassignedLeases = new ArrayList<>(Arrays.asList(lease1, lease2, lease3));

        Map<String, Set<Lease>> workerToLeasesMap = new HashMap<>();
        workerToLeasesMap.put("worker1", new HashSet<>());
        workerToLeasesMap.put("worker2", new HashSet<>());

        Set<String> workers = new HashSet<>();
        workers.add("worker1");
        workers.add("worker2");

        when(inMemoryStorageView.getWorkerToLeasesMap()).thenReturn(workerToLeasesMap);
        when(inMemoryStorageView.getActiveWorkerIdSet()).thenReturn(workers);
        when(inMemoryStorageView.getLeaseList()).thenReturn(Arrays.asList(lease1, lease2, lease3));

        // Execute
        decider.assignExpiredOrUnassignedLeases(unassignedLeases);

        // Verify assignments (should distribute evenly)
        verify(inMemoryStorageView, times(3)).performLeaseAssignment(any(), any());
        assertTrue(unassignedLeases.isEmpty());
    }

    @Test
    void balanceWorkerVariance_evenDistribution_noRebalancing() {
        // Setup - already balanced
        Lease lease1 = createLease("lease1", "worker1", 100L);
        Lease lease2 = createLease("lease2", "worker2", 200L);

        Map<String, Set<Lease>> workerToLeasesMap = new HashMap<>();
        Set<Lease> worker1Leases = new HashSet<>();
        worker1Leases.add(lease1);
        Set<Lease> worker2Leases = new HashSet<>();
        worker2Leases.add(lease2);
        workerToLeasesMap.put("worker1", worker1Leases);
        workerToLeasesMap.put("worker2", worker2Leases);

        Set<String> workers = new HashSet<>();
        workers.add("worker1");
        workers.add("worker2");

        when(inMemoryStorageView.getWorkerToLeasesMap()).thenReturn(workerToLeasesMap);
        when(inMemoryStorageView.getActiveWorkerIdSet()).thenReturn(workers);
        when(inMemoryStorageView.getLeaseList()).thenReturn(Arrays.asList(lease1, lease2));

        // Execute
        decider.balanceWorkerVariance();

        // Verify no rebalancing occurred
        verify(inMemoryStorageView, never()).performLeaseAssignment(any(), any());
    }

    @Test
    void balanceWorkerVariance_unbalanced_rebalances() {
        // Setup - worker1 has 2 leases, worker2 has 0
        Lease lease1 = createLease("lease1", "worker1", 100L);
        Lease lease2 = createLease("lease2", "worker1", 200L);

        Map<String, Set<Lease>> workerToLeasesMap = new HashMap<>();
        Set<Lease> worker1Leases = new HashSet<>();
        worker1Leases.add(lease1);
        worker1Leases.add(lease2);
        workerToLeasesMap.put("worker1", worker1Leases);
        workerToLeasesMap.put("worker2", new HashSet<>());

        Set<String> workers = new HashSet<>();
        workers.add("worker1");
        workers.add("worker2");

        when(inMemoryStorageView.getWorkerToLeasesMap()).thenReturn(workerToLeasesMap);
        when(inMemoryStorageView.getActiveWorkerIdSet()).thenReturn(workers);
        when(inMemoryStorageView.getLeaseList()).thenReturn(Arrays.asList(lease1, lease2));

        // Execute
        decider.balanceWorkerVariance();

        // Verify one lease is transferred from worker1 to worker2
        verify(inMemoryStorageView).performLeaseAssignment(any(), eq("worker2"));
    }

    @Test
    void sequentialAssignmentAndBalancing_worksWithUpdatedWorkerToLeasesMap() {
        // Setup - 3 expired leases and 2 workers, worker1 already has 2 leases
        Lease existingLease1 = createLease("existing1", "worker1", 40L);
        Lease existingLease2 = createLease("existing2", "worker1", 50L);
        Lease expiredLease1 = createLease("expired1", null, 100L);
        Lease expiredLease2 = createLease("expired2", null, 200L);
        Lease expiredLease3 = createLease("expired3", null, 300L);

        List<Lease> expiredLeases = new ArrayList<>(Arrays.asList(expiredLease1, expiredLease2, expiredLease3));

        // Initial state: worker1 has 2 leases, worker2 has 0
        Map<String, Set<Lease>> initialWorkerToLeasesMap = new HashMap<>();
        Set<Lease> worker1InitialLeases = new HashSet<>();
        worker1InitialLeases.add(existingLease1);
        worker1InitialLeases.add(existingLease2);
        initialWorkerToLeasesMap.put("worker1", worker1InitialLeases);
        initialWorkerToLeasesMap.put("worker2", new HashSet<>());

        Set<String> workers = new HashSet<>();
        workers.add("worker1");
        workers.add("worker2");

        when(inMemoryStorageView.getActiveWorkerIdSet()).thenReturn(workers);
        when(inMemoryStorageView.getLeaseList())
                .thenReturn(Arrays.asList(existingLease1, existingLease2, expiredLease1, expiredLease2, expiredLease3));
        when(inMemoryStorageView.getWorkerToLeasesMap()).thenReturn(initialWorkerToLeasesMap);

        // Simulate performLeaseAssignment updating the map
        doAnswer(invocation -> {
                    Lease lease = invocation.getArgument(0);
                    String workerId = invocation.getArgument(1);
                    // Simulate the real behavior of moving lease between workers
                    initialWorkerToLeasesMap.get(workerId).add(lease);
                    return null;
                })
                .when(inMemoryStorageView)
                .performLeaseAssignment(any(), any());

        // Execute expired lease assignment first
        decider.assignExpiredOrUnassignedLeases(expiredLeases);

        // Verify expired leases were assigned
        verify(inMemoryStorageView, times(3)).performLeaseAssignment(any(), any());
        assertTrue(expiredLeases.isEmpty());

        // Now execute variance balancing - should use updated workerToLeasesMap
        decider.balanceWorkerVariance();

        // Should have no more assignment after assigning expired leases.
        verify(inMemoryStorageView, atLeast(3)).performLeaseAssignment(any(), any());
    }

    @Test
    void balanceWorkerVariance_hasPendingHandoffLeases_avoidsStealingHandoffLeases() {
        // Setup - worker1 has 1 normal lease and 1 handoff lease, worker2 has 0
        Lease normalLease = createLease("lease1", "worker1", 100L);
        Lease handoffLease = createLeaseWithHandoff("lease2", "worker2", "worker1", 200L);

        Map<String, Set<Lease>> workerToLeasesMap = new HashMap<>();
        Set<Lease> worker1Leases = new HashSet<>();
        worker1Leases.add(normalLease);
        worker1Leases.add(handoffLease);
        workerToLeasesMap.put("worker1", worker1Leases);
        workerToLeasesMap.put("worker2", new HashSet<>());

        Set<String> workers = new HashSet<>();
        workers.add("worker1");
        workers.add("worker2");

        when(inMemoryStorageView.getWorkerToLeasesMap()).thenReturn(workerToLeasesMap);
        when(inMemoryStorageView.getActiveWorkerIdSet()).thenReturn(workers);
        when(inMemoryStorageView.getLeaseList()).thenReturn(Arrays.asList(normalLease, handoffLease));

        // Execute
        decider.balanceWorkerVariance();

        // Verify handoff lease is not stolen
        verify(inMemoryStorageView, never()).performLeaseAssignment(eq(handoffLease), any());
    }

    @Test
    void balanceWorkerVariance_maxLeasesForWorkerLimitReached_stopsRebalancing() {
        // Setup - both workers have 12 leases (above max), no rebalancing should occur
        Map<String, Set<Lease>> workerToLeasesMap = new HashMap<>();
        Set<Lease> worker1Leases = new HashSet<>();
        Set<Lease> worker2Leases = new HashSet<>();
        for (int i = 1; i <= 12; i++) {
            worker1Leases.add(createLease("lease" + i, "worker1", i * 100L));
            worker2Leases.add(createLease("lease" + (i + 12), "worker2", (i + 12) * 100L));
        }
        workerToLeasesMap.put("worker1", worker1Leases);
        workerToLeasesMap.put("worker2", worker2Leases);

        Set<String> workers = new HashSet<>();
        workers.add("worker1");
        workers.add("worker2");

        List<Lease> allLeases = new ArrayList<>();
        allLeases.addAll(worker1Leases);
        allLeases.addAll(worker2Leases);

        when(inMemoryStorageView.getWorkerToLeasesMap()).thenReturn(workerToLeasesMap);
        when(inMemoryStorageView.getActiveWorkerIdSet()).thenReturn(workers);
        when(inMemoryStorageView.getLeaseList()).thenReturn(allLeases);

        // Execute
        decider.balanceWorkerVariance();

        // Verify no assignments occur since both workers are at max
        verify(inMemoryStorageView, never()).performLeaseAssignment(any(), any());
    }

    @Test
    void assignExpiredOrUnassignedLeases_maxLeasesForWorkerLimitReached_stopsAssigning() {
        // Setup - single worker with 15 unassigned leases, but max limit is 10
        List<Lease> unassignedLeases = new ArrayList<>();
        for (int i = 1; i <= 15; i++) {
            unassignedLeases.add(createLease("lease" + i, null, i * 100L));
        }

        Map<String, Set<Lease>> workerToLeasesMap = new HashMap<>();
        workerToLeasesMap.put("worker1", new HashSet<>());

        when(inMemoryStorageView.getWorkerToLeasesMap()).thenReturn(workerToLeasesMap);
        when(inMemoryStorageView.getActiveWorkerIdSet()).thenReturn(Collections.singleton("worker1"));
        when(inMemoryStorageView.getLeaseList()).thenReturn(unassignedLeases);

        // Execute
        decider.assignExpiredOrUnassignedLeases(unassignedLeases);

        // Verify only MAX_LEASES_FOR_WORKER (10) assignments were made
        verify(inMemoryStorageView, times(MAX_LEASES_FOR_WORKER)).performLeaseAssignment(any(), eq("worker1"));
        assertEquals(5, unassignedLeases.size()); // 5 leases should remain unassigned
    }

    private Lease createLease(String leaseKey, String owner, long lastCounterIncrement) {
        Lease lease = mock(Lease.class);
        when(lease.leaseKey()).thenReturn(leaseKey);
        when(lease.leaseOwner()).thenReturn(owner);
        when(lease.actualOwner()).thenReturn(owner);
        when(lease.lastCounterIncrementNanos()).thenReturn(lastCounterIncrement);
        when(lease.shutdownRequested()).thenReturn(false);
        return lease;
    }

    private Lease createLeaseWithHandoff(
            String leaseKey, String leaseOwner, String checkpointOwner, long lastCounterIncrement) {
        Lease lease = mock(Lease.class);
        when(lease.leaseKey()).thenReturn(leaseKey);
        when(lease.leaseOwner()).thenReturn(leaseOwner);
        when(lease.checkpointOwner()).thenReturn(checkpointOwner);
        when(lease.actualOwner()).thenReturn(checkpointOwner); // During handoff, checkpointOwner is actual owner
        when(lease.lastCounterIncrementNanos()).thenReturn(lastCounterIncrement);
        when(lease.shutdownRequested()).thenReturn(true);
        return lease;
    }
}
