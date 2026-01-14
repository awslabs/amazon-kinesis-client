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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.kinesis.leases.Lease;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaseCountBasedLeaseAssignmentDeciderTest {

    @Mock
    private LeaseAssignmentManager.InMemoryStorageView mockStorageView;

    private LeaseCountBasedLeaseAssignmentDecider decider;
    private final int maxLeasesForWorker = 10;
    private final int maxLeasesToStealAtOneTime = 3;

    @BeforeEach
    void setup() {
        decider = new LeaseCountBasedLeaseAssignmentDecider(
                mockStorageView, maxLeasesForWorker, maxLeasesToStealAtOneTime);
    }

    @Test
    void assignExpiredOrUnassignedLeases_withAvailableWorkers_assignsLeasesRoundRobin() {
        final List<String> workers = Arrays.asList("worker1", "worker2", "worker3");
        final List<Lease> unassignedLeases = createUnassignedLeases(5);

        when(mockStorageView.getActiveWorkerIdSet()).thenReturn(new HashSet<>(workers));
        when(mockStorageView.getLeaseList()).thenReturn(new ArrayList<>());

        decider.assignExpiredOrUnassignedLeases(unassignedLeases);

        verify(mockStorageView, times(2)).performLeaseAssignment(any(), eq("worker1"));
        verify(mockStorageView, times(2)).performLeaseAssignment(any(), eq("worker2"));
        verify(mockStorageView, times(1)).performLeaseAssignment(any(), eq("worker3"));
    }

    @Test
    void assignExpiredOrUnassignedLeases_withExpiredLeases_prioritizesOlderLeases() {
        final List<Lease> expiredLeases = new ArrayList<>(Arrays.asList(
                createExpiredLease("lease1", 1000L),
                createExpiredLease("lease2", 500L),
                createExpiredLease("lease3", 1500L)));

        when(mockStorageView.getActiveWorkerIdSet()).thenReturn(new HashSet<>(Arrays.asList("worker1")));
        when(mockStorageView.getLeaseList()).thenReturn(new ArrayList<>());

        decider.assignExpiredOrUnassignedLeases(expiredLeases);

        InOrder inOrder = inOrder(mockStorageView);
        inOrder.verify(mockStorageView).performLeaseAssignment(argThat(l -> "lease2".equals(l.leaseKey())), any());
    }

    @Test
    void balanceWorkerVariance_withMoreWorkersThanLeases_setsTargetToOne() {
        Map<String, Integer> workerLeaseMap = new HashMap<>();
        workerLeaseMap.put("worker1", 2);
        workerLeaseMap.put("worker2", 1);
        workerLeaseMap.put("worker3", 0);
        workerLeaseMap.put("worker4", 0);
        workerLeaseMap.put("worker5", 0);
        setupWorkersAndLeases(5, 3, workerLeaseMap);

        decider.balanceWorkerVariance();

        verify(mockStorageView, times(1)).performLeaseAssignment(any(), eq("worker3"));
    }

    @Test
    void balanceWorkerVariance_withMoreLeasesThanWorkers_calculatesCorrectTarget() {
        Map<String, Integer> workerLeaseMap = new HashMap<>();
        workerLeaseMap.put("worker1", 5);
        workerLeaseMap.put("worker2", 2);
        setupWorkersAndLeases(2, 7, workerLeaseMap);

        decider.balanceWorkerVariance();

        verify(mockStorageView, times(1)).performLeaseAssignment(any(), eq("worker2"));
    }

    @Test
    void balanceWorkerVariance_respectsMaxLeasesToStealLimit() {
        Map<String, Integer> workerLeaseMap = new HashMap<>();
        workerLeaseMap.put("worker1", 10);
        workerLeaseMap.put("worker2", 0);
        setupWorkersAndLeases(2, 10, workerLeaseMap);

        decider.balanceWorkerVariance();

        verify(mockStorageView, times(3)).performLeaseAssignment(any(), eq("worker2"));
    }

    @Test
    void balanceWorkerVariance_stealsFromMostLoadedWorker() {
        Map<String, Integer> workerLeaseMap = new HashMap<>();
        workerLeaseMap.put("worker1", 5);
        workerLeaseMap.put("worker2", 3);
        workerLeaseMap.put("worker3", 1);
        setupWorkersAndLeases(3, 9, workerLeaseMap);

        decider.balanceWorkerVariance();

        verify(mockStorageView, times(2)).performLeaseAssignment(any(), eq("worker3"));
    }

    @Test
    void balanceWorkerVariance_withEqualDistribution_doesNotSteal() {
        Map<String, Integer> workerLeaseMap = new HashMap<>();
        workerLeaseMap.put("worker1", 2);
        workerLeaseMap.put("worker2", 2);
        workerLeaseMap.put("worker3", 2);
        setupWorkersAndLeases(3, 6, workerLeaseMap);

        decider.balanceWorkerVariance();

        verify(mockStorageView, never()).performLeaseAssignment(any(), any());
    }

    @Test
    void balanceWorkerVariance_respectsMaxLeasesForWorker() {
        when(mockStorageView.getActiveWorkerIdSet()).thenReturn(new HashSet<>(Arrays.asList("worker1", "worker2")));
        Map<String, Integer> workerLeaseMap = new HashMap<>();
        workerLeaseMap.put("worker1", 12);
        workerLeaseMap.put("worker2", 8);
        when(mockStorageView.getLeaseList()).thenReturn(createLeasesOwnedBy(workerLeaseMap));

        decider.balanceWorkerVariance();

        verify(mockStorageView, times(2)).performLeaseAssignment(any(), eq("worker2"));
    }

    private List<Lease> createUnassignedLeases(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> {
                    Lease lease = new Lease();
                    lease.leaseKey("lease" + i);
                    lease.lastCounterIncrementNanos(0L);
                    return lease;
                })
                .collect(Collectors.toList());
    }

    private Lease createExpiredLease(String leaseKey, long lastCounterIncrementNanos) {
        Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.leaseOwner("expiredOwner");
        lease.lastCounterIncrementNanos(lastCounterIncrementNanos);
        return lease;
    }

    private void setupWorkersAndLeases(int workerCount, int leaseCount, Map<String, Integer> distribution) {
        Set<String> workers =
                IntStream.range(1, workerCount + 1).mapToObj(i -> "worker" + i).collect(Collectors.toSet());
        List<Lease> leases = createLeasesOwnedBy(distribution);

        when(mockStorageView.getActiveWorkerIdSet()).thenReturn(workers);

        when(mockStorageView.getLeaseList()).thenReturn(leases);
    }

    private List<Lease> createLeasesOwnedBy(Map<String, Integer> distribution) {
        List<Lease> leases = new ArrayList<>();
        int leaseId = 1;

        for (Map.Entry<String, Integer> entry : distribution.entrySet()) {
            String owner = entry.getKey();
            int count = entry.getValue();

            for (int i = 0; i < count; i++) {
                Lease lease = new Lease();
                lease.leaseKey("lease" + leaseId++);
                lease.leaseOwner(owner);
                leases.add(lease);
            }
        }

        return leases;
    }
}
