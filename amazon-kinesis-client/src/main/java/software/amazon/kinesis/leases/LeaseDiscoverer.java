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
