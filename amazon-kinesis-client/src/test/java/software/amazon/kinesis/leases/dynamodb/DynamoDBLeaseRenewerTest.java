package software.amazon.kinesis.leases.dynamodb;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseStatsRecorder;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.leases.dynamodb.TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK;
import static software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber.TRIM_HORIZON;

class DynamoDBLeaseRenewerTest {

    private static final String TEST_NUMBER_VALUE_BELOW_DDB_RANGE =
            "0.00000000000000000000000000000000000000000000000000000000000000"
                    + "000000000000000000000000000000000000000000000000000000000000000000001";
    private static final String TEST_NUMBER_HIGHER_PRECISION = "1.00000000000000000000000000000000000001";
    private static final String TEST_NUMBER_WITH_HIGH_DECIMAL_VALUES =
            "0.00000000000000000000000000000000000000000000000000000000000000"
                    + "000000000000000000000000000000000000000000000000000000000000000000016843473634062791";
    private static final String TEST_NUMBER_WITH_ALL_ZERO_DECIMAL_VALUES =
            "0.00000000000000000000000000000000000000000000000000000000000000"
                    + "000000000000000000000000000000000000000000000000000000000000000000000000000000";

    private static final String WORKER_ID = "WorkerId";
    private static final String TEST_LEASE_TABLE = "SomeTable";
    private DynamoDBLeaseRefresher leaseRefresher;
    private DynamoDBLeaseRenewer leaseRenewer;
    private LeaseStatsRecorder leaseStatsRecorder;

    @Mock
    private Consumer<Lease> mockLeaseGracefulShutdownCallBack;

    @Mock
    private ExecutorService mockExecutorService;

    @Mock
    private Future mockFuture;

    private Callable leaseRenewalCallable;

    private final DynamoDbAsyncClient dynamoDbAsyncClient =
            DynamoDBEmbedded.create().dynamoDbAsyncClient();

    @BeforeEach
    void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.leaseStatsRecorder = Mockito.mock(LeaseStatsRecorder.class);
        this.leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                dynamoDbAsyncClient,
                new DynamoDBLeaseSerializer(),
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
                new DdbTableConfig().billingMode(BillingMode.PAY_PER_REQUEST),
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
                DefaultSdkAutoConstructList.getInstance());
        this.leaseRenewer = new DynamoDBLeaseRenewer(
                leaseRefresher,
                WORKER_ID,
                Duration.ofHours(1).toMillis(),
                Executors.newFixedThreadPool(1),
                new NullMetricsFactory(),
                leaseStatsRecorder,
                mockLeaseGracefulShutdownCallBack);
        this.leaseRefresher.createLeaseTableIfNotExists();
        this.leaseRefresher.waitUntilLeaseTableExists(1, 30);
    }

    @ParameterizedTest
    @CsvSource({
        TEST_NUMBER_VALUE_BELOW_DDB_RANGE + ",0.0",
        TEST_NUMBER_HIGHER_PRECISION + ",1.0",
        "1.024,1.024",
        "1024.1024, 1024.1024",
        "1024.102412324, 1024.102412",
        "1999999.123213213123123213213, 1999999.123213",
        TEST_NUMBER_WITH_HIGH_DECIMAL_VALUES + ",0.0",
        TEST_NUMBER_WITH_ALL_ZERO_DECIMAL_VALUES + ",0.0"
    })
    void renewLeases_withDifferentInputFromLeaseRecorder_assertNoFailureAndExpectedValue(
            final String inputNumber, final double expected) throws Exception {
        when(leaseStatsRecorder.getThroughputKBps("key-1")).thenReturn(Double.parseDouble(inputNumber));
        final Lease lease = createDummyLease("key-1", WORKER_ID);
        leaseRefresher.createLeaseIfNotExists(lease);
        leaseRenewer.addLeasesToRenew(ImmutableList.of(lease));
        leaseRenewer.renewLeases();
        assertEquals(expected, leaseRefresher.getLease("key-1").throughputKBps(), "Throughput value is not matching");
    }

    @Test
    void renewLeases_enqueueShutdownRequestedLease_sanity() throws Exception {
        createRenewer();
        final Lease lease = createDummyLease("key-1", WORKER_ID);
        leaseRefresher.createLeaseIfNotExists(lease);
        leaseRenewer.addLeasesToRenew(ImmutableList.of(lease));
        leaseRenewer.renewLeases();
        leaseRenewalCallable.call();
        verify(mockLeaseGracefulShutdownCallBack, never()).accept(lease);

        leaseRefresher.initiateGracefulLeaseHandoff(lease, "newOwner");
        leaseRenewalCallable.call();
        verify(mockLeaseGracefulShutdownCallBack, times(1)).accept(any());

        leaseRenewalCallable.call();
        verify(mockLeaseGracefulShutdownCallBack, times(2)).accept(any());
    }

    @Test
    void renewLeases_withHighInitialDecimalDigit_assertUpdateWithoutFailureAndNewStats() throws Exception {
        when(leaseStatsRecorder.getThroughputKBps("key-1")).thenReturn(0.10000000000000000001);
        final Lease lease = createDummyLease("key-1", WORKER_ID);
        lease.throughputKBps(
                0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001);
        leaseRefresher.createLeaseIfNotExists(lease);
        leaseRenewer.addLeasesToRenew(ImmutableList.of(lease));
        leaseRenewer.renewLeases();
        assertEquals(0.1D, leaseRefresher.getLease("key-1").throughputKBps(), "Throughput value is not matching");
    }

    private Lease createDummyLease(final String leaseKey, final String leaseOwner) {
        final Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.checkpoint(TRIM_HORIZON);
        lease.leaseOwner(leaseOwner);
        lease.leaseCounter(123L);
        lease.throughputKBps(1);
        lease.lastCounterIncrementNanos(System.nanoTime());
        return lease;
    }

    @Test
    void initialize_badLeaseInTableExists_assertInitializationWithOtherLeases()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey1", WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey2", WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey3", WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey4", WORKER_ID));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey5", "leaseOwner2"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("leaseKey6", "leaseOwner2"));
        createAndPutBadLeaseEntryInTable();

        leaseRenewer.initialize();

        final Map<String, Lease> leaseKeyToLeaseMap = leaseRenewer.getCurrentlyHeldLeases();

        assertEquals(4, leaseKeyToLeaseMap.size());
        assertTrue(leaseKeyToLeaseMap.containsKey("leaseKey1"));
        assertTrue(leaseKeyToLeaseMap.containsKey("leaseKey2"));
        assertTrue(leaseKeyToLeaseMap.containsKey("leaseKey3"));
        assertTrue(leaseKeyToLeaseMap.containsKey("leaseKey4"));
    }

    // TODO: add testLeaseRenewerDoesNotUpdateInMemoryLeaseIfDDBFailsUpdate

    private void createAndPutBadLeaseEntryInTable() {
        final PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(TEST_LEASE_TABLE)
                .item(ImmutableMap.of(
                        "leaseKey", AttributeValue.builder().s("badLeaseKey").build()))
                .build();

        dynamoDbAsyncClient.putItem(putItemRequest);
    }

    private void createRenewer() throws Exception {
        when(mockExecutorService.submit(any(Callable.class))).thenAnswer(invocation -> {
            this.leaseRenewalCallable = (Callable) invocation.getArguments()[0];
            return mockFuture;
        });
        when(mockFuture.get()).thenReturn(false);
        this.leaseRenewer = new DynamoDBLeaseRenewer(
                leaseRefresher,
                WORKER_ID,
                Duration.ofHours(1).toMillis(),
                mockExecutorService,
                new NullMetricsFactory(),
                leaseStatsRecorder,
                mockLeaseGracefulShutdownCallBack);
        this.leaseRefresher.createLeaseTableIfNotExists();
        this.leaseRefresher.waitUntilLeaseTableExists(1, 30);
    }
}
