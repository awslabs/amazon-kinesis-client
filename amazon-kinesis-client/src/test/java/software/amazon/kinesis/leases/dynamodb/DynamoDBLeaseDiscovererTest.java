package software.amazon.kinesis.leases.dynamodb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseRenewer;
import software.amazon.kinesis.leases.LeaseStatsRecorder;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

class DynamoDBLeaseDiscovererTest {

    private static final MetricsFactory TEST_METRICS_FACTORY = new NullMetricsFactory();
    private static final String TEST_WORKER_IDENTIFIER = "TestWorkerIdentifier";
    private static final String TEST_LEASE_TABLE_NAME = "TestTableName";

    private final DynamoDbAsyncClient dynamoDbAsyncClient =
            DynamoDBEmbedded.create().dynamoDbAsyncClient();
    private final LeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
            TEST_LEASE_TABLE_NAME,
            dynamoDbAsyncClient,
            new DynamoDBLeaseSerializer(),
            true,
            TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK,
            Duration.ofSeconds(10),
            new DdbTableConfig(),
            true,
            false,
            new ArrayList<>());
    private final LeaseRenewer leaseRenewer = new DynamoDBLeaseRenewer(
            leaseRefresher,
            TEST_WORKER_IDENTIFIER,
            Duration.ofSeconds(10).toMillis(),
            Executors.newFixedThreadPool(1),
            TEST_METRICS_FACTORY,
            new LeaseStatsRecorder(30000L, System::currentTimeMillis),
            lease -> {});
    private final DynamoDBLeaseDiscoverer dynamoDBLeaseDiscoverer = new DynamoDBLeaseDiscoverer(
            leaseRefresher,
            leaseRenewer,
            TEST_METRICS_FACTORY,
            TEST_WORKER_IDENTIFIER,
            Executors.newFixedThreadPool(2));

    @BeforeEach
    void setUp() throws ProvisionedThroughputException, DependencyException {
        this.leaseRefresher.createLeaseTableIfNotExists();
        this.leaseRefresher.waitUntilLeaseTableExists(1, 30);
        this.leaseRefresher.createLeaseOwnerToLeaseKeyIndexIfNotExists();
        this.leaseRefresher.waitUntilLeaseOwnerToLeaseKeyIndexExists(1, 30);
    }

    @Test
    void discoverNewLeases_happyCase_assertCorrectNewLeases()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {

        leaseRenewer.addLeasesToRenew(
                Arrays.asList(createAssignAndAddLease("lease-1"), createAssignAndAddLease("lease-2")));
        createAssignAndAddLease("lease-3");
        createAssignAndAddLease("lease-4");

        assertEquals(2, leaseRenewer.getCurrentlyHeldLeases().size());

        final List<Lease> response = dynamoDBLeaseDiscoverer.discoverNewLeases();
        final List<String> responseLeaseKeys =
                response.stream().map(Lease::leaseKey).collect(Collectors.toList());

        assertEquals(2, response.size());
        assertTrue(responseLeaseKeys.contains("lease-3"));
        assertTrue(responseLeaseKeys.contains("lease-4"));
    }

    @Test
    void discoverNewLeases_noLeasesInRenewer_assertCorrectNewLeases()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        createAssignAndAddLease("lease-3");
        createAssignAndAddLease("lease-4");

        assertEquals(0, leaseRenewer.getCurrentlyHeldLeases().size());

        final List<Lease> response = dynamoDBLeaseDiscoverer.discoverNewLeases();
        assertEquals(2, response.size());
    }

    @Test
    void discoverNewLeases_leaseRefresherThrowsException_assertEmptyResponse()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final LeaseRefresher leaseRefresher1 = Mockito.mock(LeaseRefresher.class);
        when(leaseRefresher1.getLease(any())).thenThrow(new DependencyException(new RuntimeException()));
        when(leaseRefresher1.listLeaseKeysForWorker(any())).thenReturn(ImmutableList.of("lease-3"));

        final DynamoDBLeaseDiscoverer dynamoDBLeaseDiscoverer = new DynamoDBLeaseDiscoverer(
                leaseRefresher1,
                leaseRenewer,
                TEST_METRICS_FACTORY,
                TEST_WORKER_IDENTIFIER,
                Executors.newFixedThreadPool(2));

        final List<Lease> response = dynamoDBLeaseDiscoverer.discoverNewLeases();
        assertEquals(0, response.size());
    }

    @Test
    void discoverNewLeases_inconsistentGSI_assertEmptyResponse()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final LeaseRefresher leaseRefresher1 = Mockito.mock(LeaseRefresher.class);

        final Lease ownerNotMatchingLease = new Lease();
        ownerNotMatchingLease.leaseKey("ownerNotMatchingKey");
        // Any random owner
        ownerNotMatchingLease.leaseOwner("RandomOwner");

        final Lease ownerMatchingLease = new Lease();
        ownerMatchingLease.leaseKey("ownerMatchingKey");
        // Any random owner
        ownerMatchingLease.leaseOwner(TEST_WORKER_IDENTIFIER);

        when(leaseRefresher1.getLease(ownerNotMatchingLease.leaseKey())).thenReturn(ownerNotMatchingLease);
        when(leaseRefresher1.getLease(ownerMatchingLease.leaseKey())).thenReturn(ownerMatchingLease);
        when(leaseRefresher1.listLeaseKeysForWorker(TEST_WORKER_IDENTIFIER))
                .thenReturn(ImmutableList.of("ownerMatchingKey", "ownerNotMatchingKey"));

        final DynamoDBLeaseDiscoverer dynamoDBLeaseDiscoverer = new DynamoDBLeaseDiscoverer(
                leaseRefresher1,
                leaseRenewer,
                TEST_METRICS_FACTORY,
                TEST_WORKER_IDENTIFIER,
                Executors.newFixedThreadPool(2));

        final List<Lease> response = dynamoDBLeaseDiscoverer.discoverNewLeases();
        // Validate that only 1 lease is returned.
        assertEquals(1, response.size());
    }

    @Test
    void discoverNewLeases_ignorePendingCheckpointLeases_assertReadyLeases()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        createAssignAndAddLease("lease-3");
        createAssignAndAddLease("lease-4");
        // create one lease with the same leaseOwner as the first two leases except this has
        // value in checkpointOwner
        final Lease lease = createLease("pendingCheckpointLease");
        lease.checkpointOwner("other_worker");
        this.leaseRefresher.createLeaseIfNotExists(lease);

        assertEquals(0, leaseRenewer.getCurrentlyHeldLeases().size());

        assertEquals(2, dynamoDBLeaseDiscoverer.discoverNewLeases().size());
    }

    private Lease createAssignAndAddLease(final String leaseKey)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        final Lease lease = createLease(leaseKey);
        this.leaseRefresher.createLeaseIfNotExists(lease);
        return lease;
    }

    private Lease createLease(final String leaseKey) {
        final Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.leaseOwner(TEST_WORKER_IDENTIFIER);
        lease.leaseCounter(13L);
        lease.checkpoint(new ExtendedSequenceNumber("123"));
        lease.lastCounterIncrementNanos(System.nanoTime());
        return lease;
    }
}
