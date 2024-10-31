package software.amazon.kinesis.coordinator.assignment;

import java.util.List;

import software.amazon.kinesis.leases.Lease;

public interface LeaseAssignmentDecider {

    /**
     * Assigns expiredOrUnAssignedLeases to the available workers.
     */
    void assignExpiredOrUnassignedLeases(final List<Lease> expiredOrUnAssignedLeases);

    /**
     * Balances the leases between workers in the fleet.
     * Implementation can choose to balance leases based on lease count or throughput or to bring the variance in
     * resource utilization to a minimum.
     * Check documentation on implementation class to see how it balances the leases.
     */
    void balanceWorkerVariance();
}
