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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;

/**
 * ILeaseTaker is used by LeaseCoordinator to take new leases, or leases that other workers fail to renew. Each
 * LeaseCoordinator instance corresponds to one worker and uses exactly one ILeaseTaker to take leases for that worker.
 */
public interface LeaseTaker {

    /**
     * Compute the set of leases available to be taken and attempt to take them. Lease taking rules are:
     *
     * 1) If a lease's counter hasn't changed in long enough, try to take it.
     * 2) If we see a lease we've never seen before, take it only if owner == null. If it's owned, odds are the owner is
     * holding it. We can't tell until we see it more than once.
     * 3) For load balancing purposes, you may violate rules 1 and 2 for EXACTLY ONE lease per call of takeLeases().
     *
     * @return map of shardId to Lease object for leases we just successfully took.
     *
     * @throws DependencyException on unexpected DynamoDB failures
     * @throws InvalidStateException if lease table does not exist
     */
    Map<String, Lease> takeLeases() throws DependencyException, InvalidStateException;

    /**
     * @return workerIdentifier for this LeaseTaker
     */
    String getWorkerIdentifier();

    /**
     * Default implementation returns an empty list and concrete implementaion is expected to return all leases
     * for the application that are in the lease table either by reading lease table or from an internal cache.
     *
     * @return all leases for the application that are in the lease table
     */
    default List<Lease> allLeases() {
        return Collections.emptyList();
    }
}
