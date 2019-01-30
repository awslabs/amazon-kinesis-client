package com.amazonaws.services.kinesis.leases.interfaces;

import com.amazonaws.services.kinesis.leases.impl.Lease;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ILeaseSelector abstracts away the lease selection logic from the application code that's using leasing.
 * It owns filtering of the leases to be taken.
 */
public interface ILeaseSelector<T extends Lease> {

    /**
     * Provides the list of leases to be taken.
     * @param expiredLeases list of leases that are currently expired
     * @param numLeasesToReachTarget the number of leases to be taken
     * @return
     */
    Set<T> getLeasesToTakeFromExpiredLeases(List<T> expiredLeases, int numLeasesToReachTarget);

    /**
     * Provides the number of leases that should be taken by the worker.
     * @param allLeases list of all existing leases
     * @return
     */
    int getLeaseCountThatCanBeTaken(Collection<T> allLeases);
}
