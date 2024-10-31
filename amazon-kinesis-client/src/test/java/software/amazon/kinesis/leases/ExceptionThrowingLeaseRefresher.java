/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.leases;

import java.util.Arrays;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Mock LeaseRefresher by randomly throwing Leasing Exceptions.
 *
 */
@RequiredArgsConstructor
@Slf4j
public class ExceptionThrowingLeaseRefresher implements LeaseRefresher {
    private static final Throwable EXCEPTION_MSG = new Throwable("Test Exception");

    // The real local lease refresher which would do the real implementations.
    private final LeaseRefresher leaseRefresher;
    // Use array below to control in what situations we want to throw exceptions.
    private int[] leaseRefresherMethodCallingCount = new int[ExceptionThrowingLeaseRefresherMethods.values().length];

    /**
     * Methods which we support (simulate exceptions).
     */
    public enum ExceptionThrowingLeaseRefresherMethods {
        CREATELEASETABLEIFNOTEXISTS(0),
        LEASETABLEEXISTS(1),
        WAITUNTILLEASETABLEEXISTS(2),
        LISTLEASES(3),
        CREATELEASEIFNOTEXISTS(4),
        GETLEASE(5),
        RENEWLEASE(6),
        TAKELEASE(7),
        EVICTLEASE(8),
        DELETELEASE(9),
        DELETEALL(10),
        UPDATELEASE(11),
        LISTLEASESFORSTREAM(12),
        IS_LEASE_OWNER_TO_LEASE_KEY_INDEX_ACTIVE(13),
        NONE(Integer.MIN_VALUE);

        private Integer index;

        ExceptionThrowingLeaseRefresherMethods(Integer index) {
            this.index = index;
        }

        Integer index() {
            return this.index;
        }
    }

    // Define which method should throw exception and when it should throw exception.
    private ExceptionThrowingLeaseRefresherMethods methodThrowingException =
            ExceptionThrowingLeaseRefresherMethods.NONE;
    private int timeThrowingException = Integer.MAX_VALUE;

    /**
     * Set parameters used for throwing exception.
     *
     * @param method which would throw exception
     * @param throwingTime defines what time to throw exception
     */
    void leaseRefresherThrowingExceptionScenario(ExceptionThrowingLeaseRefresherMethods method, int throwingTime) {
        this.methodThrowingException = method;
        this.timeThrowingException = throwingTime;
    }

    /**
     * Reset all parameters used for throwing exception.
     */
    void clearLeaseRefresherThrowingExceptionScenario() {
        Arrays.fill(leaseRefresherMethodCallingCount, 0);
        this.methodThrowingException = ExceptionThrowingLeaseRefresherMethods.NONE;
        this.timeThrowingException = Integer.MAX_VALUE;
    }

    // Throw exception when the conditions are satisfied :
    // 1). method equals to methodThrowingException
    // 2). method calling count equals to what we want
    private void throwExceptions(String methodName, ExceptionThrowingLeaseRefresherMethods method)
            throws DependencyException {
        // Increase calling count for this method
        leaseRefresherMethodCallingCount[method.index()]++;
        if (method.equals(methodThrowingException)
                && (leaseRefresherMethodCallingCount[method.index()] == timeThrowingException)) {
            // Throw Dependency Exception if all conditions are satisfied.
            log.debug("Throwing DependencyException in {}", methodName);
            throw new DependencyException(EXCEPTION_MSG);
        }
    }

    @Override
    public boolean createLeaseTableIfNotExists(Long readCapacity, Long writeCapacity)
            throws ProvisionedThroughputException, DependencyException {
        throwExceptions(
                "createLeaseTableIfNotExists", ExceptionThrowingLeaseRefresherMethods.CREATELEASETABLEIFNOTEXISTS);

        return leaseRefresher.createLeaseTableIfNotExists(readCapacity, writeCapacity);
    }

    @Override
    public boolean createLeaseTableIfNotExists() throws ProvisionedThroughputException, DependencyException {
        throwExceptions(
                "createLeaseTableIfNotExists", ExceptionThrowingLeaseRefresherMethods.CREATELEASETABLEIFNOTEXISTS);

        return leaseRefresher.createLeaseTableIfNotExists();
    }

    @Override
    public boolean leaseTableExists() throws DependencyException {
        throwExceptions("leaseTableExists", ExceptionThrowingLeaseRefresherMethods.LEASETABLEEXISTS);

        return leaseRefresher.leaseTableExists();
    }

    @Override
    public boolean waitUntilLeaseTableExists(long secondsBetweenPolls, long timeoutSeconds) throws DependencyException {
        throwExceptions("waitUntilLeaseTableExists", ExceptionThrowingLeaseRefresherMethods.WAITUNTILLEASETABLEEXISTS);

        return leaseRefresher.waitUntilLeaseTableExists(secondsBetweenPolls, timeoutSeconds);
    }

    @Override
    public List<Lease> listLeasesForStream(StreamIdentifier streamIdentifier)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("listLeasesForStream", ExceptionThrowingLeaseRefresherMethods.LISTLEASESFORSTREAM);

        return leaseRefresher.listLeasesForStream(streamIdentifier);
    }

    @Override
    public List<Lease> listLeases() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("listLeases", ExceptionThrowingLeaseRefresherMethods.LISTLEASES);

        return leaseRefresher.listLeases();
    }

    @Override
    public boolean createLeaseIfNotExists(Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("createLeaseIfNotExists", ExceptionThrowingLeaseRefresherMethods.CREATELEASEIFNOTEXISTS);

        return leaseRefresher.createLeaseIfNotExists(lease);
    }

    @Override
    public boolean renewLease(Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("renewLease", ExceptionThrowingLeaseRefresherMethods.RENEWLEASE);

        return leaseRefresher.renewLease(lease);
    }

    @Override
    public boolean takeLease(Lease lease, String owner)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("takeLease", ExceptionThrowingLeaseRefresherMethods.TAKELEASE);

        return leaseRefresher.takeLease(lease, owner);
    }

    @Override
    public boolean evictLease(Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("evictLease", ExceptionThrowingLeaseRefresherMethods.EVICTLEASE);

        return leaseRefresher.evictLease(lease);
    }

    @Override
    public void deleteLease(Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("deleteLease", ExceptionThrowingLeaseRefresherMethods.DELETELEASE);

        leaseRefresher.deleteLease(lease);
    }

    @Override
    public boolean updateLease(Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("updateLease", ExceptionThrowingLeaseRefresherMethods.UPDATELEASE);

        return leaseRefresher.updateLease(lease);
    }

    @Override
    public Lease getLease(String leaseKey)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("getLease", ExceptionThrowingLeaseRefresherMethods.GETLEASE);

        return leaseRefresher.getLease(leaseKey);
    }

    @Override
    public void deleteAll() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("deleteAll", ExceptionThrowingLeaseRefresherMethods.DELETEALL);

        leaseRefresher.deleteAll();
    }

    @Override
    public boolean isLeaseTableEmpty()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return false;
    }

    @Override
    public ExtendedSequenceNumber getCheckpoint(final String leaseKey)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        return null;
    }

    @Override
    public boolean isLeaseOwnerToLeaseKeyIndexActive() throws DependencyException {
        throwExceptions(
                "isLeaseOwnerToLeaseKeyIndexActive",
                ExceptionThrowingLeaseRefresherMethods.IS_LEASE_OWNER_TO_LEASE_KEY_INDEX_ACTIVE);
        return false;
    }
}
