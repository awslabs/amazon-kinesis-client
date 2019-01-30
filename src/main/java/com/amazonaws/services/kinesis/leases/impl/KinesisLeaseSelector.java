package com.amazonaws.services.kinesis.leases.impl;

import com.amazonaws.services.kinesis.leases.interfaces.ILeaseSelector;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * KinesisLeaseSelector abstracts away the lease selection logic from the application code that's using leasing.
 * It owns filtering of the leases to be taken.
 */
public class KinesisLeaseSelector<T extends Lease> implements ILeaseSelector<T> {

    /**
     * Provides the list of leases to be taken.
     * @param expiredLeases list of leases that are currently expired
     * @param numLeasesToReachTarget the number of leases to be taken
     * @return
     */
    @Override
    public Set<T> getLeasesToTakeFromExpiredLeases(List<T> expiredLeases, int numLeasesToReachTarget) {
        Set<T> leasesToTake = new HashSet<T>();

        // If we have expired leases, get up to <needed> leases from expiredLeases
        for (; numLeasesToReachTarget > 0 && expiredLeases.size() > 0; numLeasesToReachTarget--) {
            leasesToTake.add(expiredLeases.remove(0));
        }

        return leasesToTake;
    }

    /**
     * Provides the number of leases that should be taken by the worker.
     * @param allLeases list of all existing leases
     * @return
     */
    @Override
    public int getLeaseCountThatCanBeTaken(Collection<T> allLeases) {
        return allLeases.size();
    }
}
