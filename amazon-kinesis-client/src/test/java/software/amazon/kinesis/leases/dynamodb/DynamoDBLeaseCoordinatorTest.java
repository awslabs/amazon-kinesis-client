package software.amazon.kinesis.leases.dynamodb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.metrics.MetricsFactory;

import java.util.UUID;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBLeaseCoordinatorTest {

    private static final String WORKER_ID = UUID.randomUUID().toString();
    private static final long LEASE_DURATION_MILLIS = 5000L;
    private static final long EPSILON_MILLIS = 25L;
    private static final int MAX_LEASES_FOR_WORKER = Integer.MAX_VALUE;
    private static final int MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1;
    private static final int MAX_LEASE_RENEWER_THREAD_COUNT = 20;
    private static final long INITIAL_LEASE_TABLE_READ_CAPACITY = 10L;
    private static final long INITIAL_LEASE_TABLE_WRITE_CAPACITY = 10L;
    private static final long SECONDS_BETWEEN_POLLS = 10L;
    private static final long TIMEOUT_SECONDS = 600L;

    @Mock
    private LeaseRefresher leaseRefresher;
    @Mock
    private MetricsFactory metricsFactory;

    private DynamoDBLeaseCoordinator leaseCoordinator;

    @Before
    public void setup() {
        this.leaseCoordinator = new DynamoDBLeaseCoordinator(leaseRefresher, WORKER_ID, LEASE_DURATION_MILLIS,
                EPSILON_MILLIS, MAX_LEASES_FOR_WORKER, MAX_LEASES_TO_STEAL_AT_ONE_TIME, MAX_LEASE_RENEWER_THREAD_COUNT,
                INITIAL_LEASE_TABLE_READ_CAPACITY, INITIAL_LEASE_TABLE_WRITE_CAPACITY, metricsFactory);
    }

    @Test
    public void testInitialize_tableCreationSucceeds() throws Exception {
        when(leaseRefresher.createLeaseTableIfNotExists()).thenReturn(true);
        when(leaseRefresher.waitUntilLeaseTableExists(SECONDS_BETWEEN_POLLS, TIMEOUT_SECONDS)).thenReturn(true);

        leaseCoordinator.initialize();

        verify(leaseRefresher, times(1)).createLeaseTableIfNotExists();
        verify(leaseRefresher, times(1)).waitUntilLeaseTableExists(SECONDS_BETWEEN_POLLS, TIMEOUT_SECONDS);
    }

    @Test
    public void testInitialize_tableCreationFails() throws Exception {
        when(leaseRefresher.createLeaseTableIfNotExists()).thenReturn(false);
        when(leaseRefresher.waitUntilLeaseTableExists(SECONDS_BETWEEN_POLLS, TIMEOUT_SECONDS)).thenReturn(false);

        Assert.assertThrows(DependencyException.class, () -> leaseCoordinator.initialize());
        verify(leaseRefresher, times(1)).createLeaseTableIfNotExists();
        verify(leaseRefresher, times(1)).waitUntilLeaseTableExists(SECONDS_BETWEEN_POLLS, TIMEOUT_SECONDS);
    }
}
