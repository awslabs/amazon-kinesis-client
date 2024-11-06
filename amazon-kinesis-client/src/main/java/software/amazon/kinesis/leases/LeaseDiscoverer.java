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

package software.amazon.kinesis.leases;

import java.util.List;

import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

public interface LeaseDiscoverer {
    /**
     * Identifies the leases that are assigned to the current worker but are not being tracked and processed by the
     * current worker.
     *
     * @return list of leases assigned to worker which doesn't exist in {@param currentHeldLeaseKeys}
     * @throws DependencyException if DynamoDB scan fails in an unexpected way
     * @throws InvalidStateException if lease table does not exist
     * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
     */
    List<Lease> discoverNewLeases() throws ProvisionedThroughputException, InvalidStateException, DependencyException;
}
