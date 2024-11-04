package software.amazon.kinesis.leases.dynamodb;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseDiscoverer;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseRenewer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

import static java.util.Objects.isNull;

/**
 * An implementation of {@link LeaseDiscoverer}, it uses {@link LeaseRefresher} to query
 * {@link DynamoDBLeaseRefresher#LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME } and find the leases assigned
 * to current worker and then filter and returns the leases that have not started processing (looks at
 * {@link LeaseRenewer#getCurrentlyHeldLeases()} to find out which leases are currently held leases).
 */
@Slf4j
@RequiredArgsConstructor
public class DynamoDBLeaseDiscoverer implements LeaseDiscoverer {

    private final LeaseRefresher leaseRefresher;
    private final LeaseRenewer leaseRenewer;
    private final MetricsFactory metricsFactory;
    private final String workerIdentifier;
    private final ExecutorService executorService;

    @Override
    public List<Lease> discoverNewLeases()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, "LeaseDiscovery");
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            final Set<String> currentHeldLeaseKeys =
                    leaseRenewer.getCurrentlyHeldLeases().keySet();

            final long listLeaseKeysForWorkerStartTime = System.currentTimeMillis();
            final List<String> leaseKeys = leaseRefresher.listLeaseKeysForWorker(workerIdentifier);
            MetricsUtil.addLatency(
                    metricsScope, "ListLeaseKeysForWorker", listLeaseKeysForWorkerStartTime, MetricsLevel.DETAILED);

            final List<String> newLeaseKeys = leaseKeys.stream()
                    .filter(leaseKey -> !currentHeldLeaseKeys.contains(leaseKey))
                    .collect(Collectors.toList());

            final long fetchNewLeasesStartTime = System.currentTimeMillis();
            final List<CompletableFuture<Lease>> completableFutures = newLeaseKeys.stream()
                    .map(leaseKey ->
                            CompletableFuture.supplyAsync(() -> fetchLease(leaseKey, metricsScope), executorService))
                    .collect(Collectors.toList());

            final List<Lease> newLeases = completableFutures.stream()
                    .map(CompletableFuture::join)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            log.info(
                    "New leases assigned to worker : {}, count : {}, leases : {}",
                    workerIdentifier,
                    newLeases.size(),
                    newLeases.stream().map(Lease::leaseKey).collect(Collectors.toList()));

            MetricsUtil.addLatency(metricsScope, "FetchNewLeases", fetchNewLeasesStartTime, MetricsLevel.DETAILED);

            success = true;
            MetricsUtil.addCount(metricsScope, "NewLeasesDiscovered", newLeases.size(), MetricsLevel.DETAILED);
            return newLeases;
        } finally {
            MetricsUtil.addWorkerIdentifier(metricsScope, workerIdentifier);
            MetricsUtil.addSuccessAndLatency(metricsScope, success, startTime, MetricsLevel.SUMMARY);
            MetricsUtil.endScope(metricsScope);
        }
    }

    private Lease fetchLease(final String leaseKey, final MetricsScope metricsScope) {
        try {
            final Lease lease = leaseRefresher.getLease(leaseKey);
            if (isNull(lease)) {
                return null;
            }
            // GSI is eventually consistent thus, validate that the fetched lease is indeed assigned to this
            // worker, if not just pass in this run.
            if (!lease.leaseOwner().equals(workerIdentifier)) {
                MetricsUtil.addCount(metricsScope, "OwnerMismatch", 1, MetricsLevel.DETAILED);
                return null;
            }
            // if checkpointOwner is not null, it means that the lease is still pending shutdown for the last owner.
            // Don't add the lease to the in-memory map yet.
            if (lease.checkpointOwner() != null) {
                return null;
            }
            // when a new lease is discovered, set the lastCounterIncrementNanos to current time as the time
            // when it has become visible, on next renewer interval this will be updated by LeaseRenewer to
            // correct time.
            lease.lastCounterIncrementNanos(System.nanoTime());
            return lease;
        } catch (final Exception e) {
            // if getLease on some lease key fail, continue and fetch other leases, the one failed will
            // be fetched in the next iteration or will be reassigned if stayed idle for long.
            MetricsUtil.addCount(metricsScope, "GetLease:Error", 1, MetricsLevel.SUMMARY);
            log.error("GetLease failed for leaseKey : {}", leaseKey, e);
            return null;
        }
    }
}
