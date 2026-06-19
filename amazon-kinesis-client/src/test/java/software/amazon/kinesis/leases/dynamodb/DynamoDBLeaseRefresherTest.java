package software.amazon.kinesis.leases.dynamodb;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.IndexStatus;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsResponse;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.dynamodb.model.IndexStatus.ACTIVE;
import static software.amazon.awssdk.services.dynamodb.model.IndexStatus.CREATING;
import static software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher.LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME;
import static software.amazon.kinesis.leases.dynamodb.TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK;

class DynamoDBLeaseRefresherTest {

    private static final String TEST_LEASE_TABLE = "SomeTable";
    private final DynamoDbAsyncClient dynamoDbAsyncClient =
            DynamoDBEmbedded.create().dynamoDbAsyncClient();

    @Test
    void createLeaseTableWithPitr() throws DependencyException, ProvisionedThroughputException {
        // DynamoDBLocal does not support PITR operations on table so using mocks
        final DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class, Mockito.RETURNS_MOCKS);
        DynamoDBLeaseRefresher dynamoDBLeaseRefresherWithPitr =
                createLeaseRefresher(new DdbTableConfig(), mockDdbClient, false, true);

        when(mockDdbClient.describeTable(any(DescribeTableRequest.class)))
                .thenThrow(ResourceNotFoundException.builder()
                        .message("Mock table does not exist scenario")
                        .build());

        final CompletableFuture<UpdateContinuousBackupsResponse> future = new CompletableFuture<>();
        future.complete(UpdateContinuousBackupsResponse.builder().build());

        when(mockDdbClient.updateContinuousBackups(any(UpdateContinuousBackupsRequest.class)))
                .thenReturn(future);

        setupTable(dynamoDBLeaseRefresherWithPitr);

        UpdateContinuousBackupsRequest updateContinuousBackupsRequest = UpdateContinuousBackupsRequest.builder()
                .tableName(TEST_LEASE_TABLE)
                .pointInTimeRecoverySpecification(builder -> builder.pointInTimeRecoveryEnabled(true))
                .build();

        verify(mockDdbClient, times(1)).updateContinuousBackups(updateContinuousBackupsRequest);
    }

    @Test
    void createLeaseTableWithDeletionProtection() throws DependencyException, ProvisionedThroughputException {
        DynamoDBLeaseRefresher dynamoDBLeaseRefresherWithDeletionProtection =
                createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient, true, false);

        dynamoDBLeaseRefresherWithDeletionProtection.createLeaseTableIfNotExists();
        dynamoDBLeaseRefresherWithDeletionProtection.waitUntilLeaseTableExists(1, 30);

        final DescribeTableResponse describeTableResponse = dynamoDbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_LEASE_TABLE)
                        .build())
                .join();

        assertTrue(describeTableResponse.table().deletionProtectionEnabled());
    }

    @Test
    void createWorkerIdToLeaseKeyIndexIfNotExists_sanity() throws DependencyException, ProvisionedThroughputException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);

        assertFalse(leaseRefresher.isLeaseOwnerToLeaseKeyIndexActive());

        final String creationResponse = leaseRefresher.createLeaseOwnerToLeaseKeyIndexIfNotExists();

        final boolean waitResponse = leaseRefresher.waitUntilLeaseOwnerToLeaseKeyIndexExists(1, 30);
        assertTrue(leaseRefresher.isLeaseOwnerToLeaseKeyIndexActive());

        assertEquals(creationResponse, CREATING.toString(), "Index status mismatch");
        assertTrue(waitResponse);

        final DescribeTableResponse describeTableResponse = dynamoDbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_LEASE_TABLE)
                        .build())
                .join();
        assertEquals(
                1,
                describeTableResponse.table().globalSecondaryIndexes().size(),
                "No. of index on lease table is not 1");
        assertEquals(
                LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME,
                describeTableResponse.table().globalSecondaryIndexes().get(0).indexName(),
                "Index name mismatch");
        assertEquals(
                IndexStatus.ACTIVE,
                describeTableResponse.table().globalSecondaryIndexes().get(0).indexStatus());
    }

    @Test
    void waitUntilLeaseOwnerToLeaseKeyIndexExists_noTransitionToActive_assertFalse()
            throws DependencyException, ProvisionedThroughputException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);

        dynamoDbAsyncClient.deleteTable(
                DeleteTableRequest.builder().tableName(TEST_LEASE_TABLE).build());

        final boolean response = leaseRefresher.waitUntilLeaseOwnerToLeaseKeyIndexExists(1, 3);
        assertFalse(response);
        assertFalse(leaseRefresher.isLeaseOwnerToLeaseKeyIndexActive());
    }

    @Test
    void isLeaseOwnerGsiIndexActive() throws DependencyException, ProvisionedThroughputException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);

        final DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class, Mockito.RETURNS_MOCKS);
        final LeaseRefresher leaseRefresherForTest = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                mockDdbClient,
                new DynamoDBLeaseSerializer(),
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                true,
                true,
                new ArrayList<>());

        when(mockDdbClient.describeTable(any(DescribeTableRequest.class)))
                .thenThrow(ResourceNotFoundException.builder()
                        .message("Mock table does not exist scenario")
                        .build());

        // before creating the GSI it is not active
        assertFalse(leaseRefresherForTest.isLeaseOwnerToLeaseKeyIndexActive());

        reset(mockDdbClient);
        final CompletableFuture<DescribeTableResponse> creatingTableFuture = new CompletableFuture<>();
        creatingTableFuture.complete(DescribeTableResponse.builder()
                .table(TableDescription.builder()
                        .tableStatus(TableStatus.CREATING)
                        .build())
                .build());
        when(mockDdbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(creatingTableFuture);

        // If describe table does not have gsi status, it will be false
        assertFalse(leaseRefresherForTest.isLeaseOwnerToLeaseKeyIndexActive());

        reset(mockDdbClient);
        final CompletableFuture<DescribeTableResponse> noGsiFuture = new CompletableFuture<>();
        noGsiFuture.complete(DescribeTableResponse.builder()
                .table(TableDescription.builder()
                        .creationDateTime(Instant.now())
                        .itemCount(100L)
                        .tableStatus(TableStatus.ACTIVE)
                        .globalSecondaryIndexes(GlobalSecondaryIndexDescription.builder()
                                .indexName("A_DIFFERENT_INDEX")
                                .indexStatus(ACTIVE)
                                .build())
                        .build())
                .build());
        when(mockDdbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(noGsiFuture);

        // before creating the GSI it is not active
        assertFalse(leaseRefresherForTest.isLeaseOwnerToLeaseKeyIndexActive());

        reset(mockDdbClient);
        final CompletableFuture<DescribeTableResponse> gsiInactiveFuture = new CompletableFuture<>();
        gsiInactiveFuture.complete(DescribeTableResponse.builder()
                .table(TableDescription.builder()
                        .creationDateTime(Instant.now())
                        .itemCount(100L)
                        .tableStatus(TableStatus.ACTIVE)
                        .globalSecondaryIndexes(
                                GlobalSecondaryIndexDescription.builder()
                                        .indexName("A_DIFFERENT_INDEX")
                                        .indexStatus(ACTIVE)
                                        .build(),
                                GlobalSecondaryIndexDescription.builder()
                                        .indexName(LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME)
                                        .indexStatus(CREATING)
                                        .build())
                        .build())
                .build());
        when(mockDdbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(gsiInactiveFuture);

        // returns false if GSI is not active
        assertFalse(leaseRefresherForTest.isLeaseOwnerToLeaseKeyIndexActive());

        reset(mockDdbClient);
        final CompletableFuture<DescribeTableResponse> gsiActiveFuture = new CompletableFuture<>();
        gsiActiveFuture.complete(DescribeTableResponse.builder()
                .table(TableDescription.builder()
                        .creationDateTime(Instant.now())
                        .itemCount(100L)
                        .tableStatus(TableStatus.ACTIVE)
                        .globalSecondaryIndexes(
                                GlobalSecondaryIndexDescription.builder()
                                        .indexName("A_DIFFERENT_INDEX")
                                        .indexStatus(ACTIVE)
                                        .build(),
                                GlobalSecondaryIndexDescription.builder()
                                        .indexName(LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME)
                                        .indexStatus(ACTIVE)
                                        .build())
                        .build())
                .build());
        when(mockDdbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(gsiActiveFuture);

        // returns true if GSI is not active
        assertTrue(leaseRefresherForTest.isLeaseOwnerToLeaseKeyIndexActive());
    }

    @Test
    void assignLease_leaseWithPrevOwner_assertAssignmentToNewOwner()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", "leaseOwner1"));

        // Fetch a lease from assign it to owner2
        boolean response = leaseRefresher.assignLease(leaseRefresher.getLease("lease1"), "leaseOwner2");
        assertTrue(response);
        assertEquals(leaseRefresher.getLease("lease1").leaseOwner(), "leaseOwner2");
    }

    @Test
    void assignLease_unassignedLease_assertAssignmentToNewOwner()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", null));

        // Fetch a lease from assign it to owner2
        boolean response = leaseRefresher.assignLease(leaseRefresher.getLease("lease1"), "leaseOwner2");
        assertTrue(response);
        assertEquals(leaseRefresher.getLease("lease1").leaseOwner(), "leaseOwner2");
    }

    // validates that the lease assignment fails if unassigned lease after fetch is deleted
    @Test
    void assignLease_unAssignedLeaseGetsDeleted_assertAssignemntFailure()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", null));

        // Lease fetched before delete
        final Lease leaseFetchedBeforeDelete = leaseRefresher.getLease("lease1");

        // Deleted lease
        leaseRefresher.deleteLease(leaseRefresher.getLease("lease1"));
        assertNull(leaseRefresher.getLease("lease1"));

        // Assert that in this case the lease assignment fails
        boolean response = leaseRefresher.assignLease(leaseFetchedBeforeDelete, "leaseOwner2");
        assertFalse(response);
        assertNull(leaseRefresher.getLease("lease1"));
    }

    // validates that the lease assignment fails if assigned lease after fetch is deleted
    @Test
    void assignLease_AssignedLeaseGetsDeleted_assertAssignemntFailure()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", "leaseOwner1"));

        // Lease fetched before delete
        final Lease leaseFetchedBeforeDelete = leaseRefresher.getLease("lease1");

        // Deleted lease
        leaseRefresher.deleteLease(leaseRefresher.getLease("lease1"));
        assertNull(leaseRefresher.getLease("lease1"));

        // Assert that in this case the lease assignment fails
        boolean response = leaseRefresher.assignLease(leaseFetchedBeforeDelete, "leaseOwner2");
        assertFalse(response);
        assertNull(leaseRefresher.getLease("lease1"));
    }

    /**
     * This test validates the behavior that a lease is assigned as long a leaseOwner has not changed but other
     * field like leaseCounter or checkpoint updates are done after fetch and before assign call. And also
     * validates that after assignment the updates on the lease with old references fails.
     */
    @Test
    void assignLease_updatesOnTheLeaseFailsAfterAssignment()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        final Lease originalLeaseOnWorker = createDummyLease("lease1", "leaseOwner1");
        leaseRefresher.createLeaseIfNotExists(originalLeaseOnWorker);

        // Normal lease renewal happens
        leaseRefresher.renewLease(originalLeaseOnWorker);
        leaseRefresher.renewLease(originalLeaseOnWorker);

        // Checkpoint
        originalLeaseOnWorker.checkpoint(new ExtendedSequenceNumber("100"));
        leaseRefresher.updateLease(originalLeaseOnWorker);

        // Asserting that the updates have gone correctly
        assertEquals(3, leaseRefresher.getLease("lease1").leaseCounter(), "LeaseCounter mismatch");

        // Lease is read for assignment (e.g. for LAM)
        final Lease freshFetchedLease = leaseRefresher.getLease("lease1");

        // Normal lease renewal and checkpoint happens again.
        leaseRefresher.renewLease(originalLeaseOnWorker);
        originalLeaseOnWorker.checkpoint(new ExtendedSequenceNumber("105"));
        leaseRefresher.updateLease(originalLeaseOnWorker);
        assertEquals(5, leaseRefresher.getLease("lease1").leaseCounter(), "LeaseCounter mismatch");

        // assert assignment happens on lease object as the owner has not changed only heartbeat and checkpoint has
        // updated.
        final boolean assignmentResponse = leaseRefresher.assignLease(freshFetchedLease, "owner2");
        assertTrue(assignmentResponse, "Assignment on lease failed");
        assertEquals(6, leaseRefresher.getLease("lease1").leaseCounter(), "LeaseCounter mismatch");

        // Assert that update or renwer fails after assignment using originalLeaseOnWorker instance.
        assertFalse(leaseRefresher.updateLease(originalLeaseOnWorker), "Update on lease happened after reassignment");
        assertFalse(leaseRefresher.renewLease(originalLeaseOnWorker), "Update on lease happened after reassignment");
        assertEquals(6, leaseRefresher.getLease("lease1").leaseCounter(), "LeaseCounter mismatch");
    }

    @Test
    void listLeasesParallely_sanity()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", "leaseOwner1"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease2", "leaseOwner2"));
        final Map.Entry<List<Lease>, List<String>> response =
                leaseRefresher.listLeasesParallely(Executors.newFixedThreadPool(2), 2);
        assertEquals(2, response.getKey().size());
        assertEquals(0, response.getValue().size());
    }

    @Test
    void listLeasesParallely_leaseWithFailingDeserialization_assertCorrectResponse()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", "leaseOwner1"));
        createAndPutBadLeaseEntryInTable();
        final Map.Entry<List<Lease>, List<String>> response =
                leaseRefresher.listLeasesParallely(Executors.newFixedThreadPool(2), 2);
        assertEquals(1, response.getKey().size());
        assertEquals("lease1", response.getKey().get(0).leaseKey());
        assertEquals(1, response.getValue().size());
        assertEquals("badLeaseKey", response.getValue().get(0));
    }

    @Test
    public void listLeasesParallely_UseCachedTotalSegment()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class);
        final long oneGBInBytes = 1073741824L;

        when(mockDdbClient.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DescribeTableResponse.builder()
                        .table(TableDescription.builder()
                                .tableName(TEST_LEASE_TABLE)
                                .tableStatus(TableStatus.ACTIVE)
                                .tableSizeBytes(oneGBInBytes)
                                .build())
                        .build()));
        when(mockDdbClient.scan(any(ScanRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        ScanResponse.builder().items(new ArrayList<>()).build()));

        final LeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                mockDdbClient,
                new DynamoDBLeaseSerializer(),
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                true,
                true,
                new ArrayList<>());

        leaseRefresher.listLeasesParallely(Executors.newFixedThreadPool(2), 0);
        verify(mockDdbClient, times(5)).scan(any(ScanRequest.class));

        // calling second to test cached value is used
        leaseRefresher.listLeasesParallely(Executors.newFixedThreadPool(2), 0);

        // verify if describe table is called once even when listLeasesParallely is called twice
        verify(mockDdbClient, times(1)).describeTable(any(DescribeTableRequest.class));
        verify(mockDdbClient, times(10)).scan(any(ScanRequest.class));
    }

    @Test
    public void listLeasesParallely_DescribeTableNotCalledWhenSegmentGreaterThanZero()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class);

        when(mockDdbClient.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DescribeTableResponse.builder()
                        .table(TableDescription.builder()
                                .tableName(TEST_LEASE_TABLE)
                                .tableStatus(TableStatus.ACTIVE)
                                .tableSizeBytes(1000L)
                                .build())
                        .build()));
        when(mockDdbClient.scan(any(ScanRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        ScanResponse.builder().items(new ArrayList<>()).build()));

        final LeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                mockDdbClient,
                new DynamoDBLeaseSerializer(),
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                true,
                true,
                new ArrayList<>());

        leaseRefresher.listLeasesParallely(Executors.newFixedThreadPool(2), 2);
        verify(mockDdbClient, times(0)).describeTable(any(DescribeTableRequest.class));
    }

    @Test
    public void listLeasesParallely_TotalSegmentIsDefaultWhenDescribeTableThrowsException()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class);

        when(mockDdbClient.describeTable(any(DescribeTableRequest.class)))
                .thenThrow(ResourceNotFoundException.builder()
                        .message("Mock table does not exist scenario")
                        .build());

        when(mockDdbClient.scan(any(ScanRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        ScanResponse.builder().items(new ArrayList<>()).build()));

        final LeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                mockDdbClient,
                new DynamoDBLeaseSerializer(),
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                true,
                true,
                new ArrayList<>());

        leaseRefresher.listLeasesParallely(Executors.newFixedThreadPool(2), 0);
        verify(mockDdbClient, times(10)).scan(any(ScanRequest.class));
    }

    @ParameterizedTest
    @CsvSource({
        "0, 1", // 0
        "1024, 1", // 1KB
        "104857600, 1", // 100MB
        "214748364, 1", // 0.2GB
        "322122547, 2", // 1.3GB
        "1073741824, 5", // 1GB
        "2147483648, 10", // 2GB
        "5368709120, 25", // 5GB
    })
    public void listLeasesParallely_TotalSegmentForDifferentTableSize(long tableSizeBytes, int totalSegments)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class);

        when(mockDdbClient.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DescribeTableResponse.builder()
                        .table(TableDescription.builder()
                                .tableName(TEST_LEASE_TABLE)
                                .tableStatus(TableStatus.ACTIVE)
                                .tableSizeBytes(tableSizeBytes)
                                .build())
                        .build()));
        when(mockDdbClient.scan(any(ScanRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        ScanResponse.builder().items(new ArrayList<>()).build()));

        final LeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                mockDdbClient,
                new DynamoDBLeaseSerializer(),
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                true,
                true,
                new ArrayList<>());

        leaseRefresher.listLeasesParallely(Executors.newFixedThreadPool(2), 0);
        verify(mockDdbClient, times(totalSegments)).scan(any(ScanRequest.class));
    }

    @Test
    void initiateGracefulLeaseHandoff_sanity() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        final String nextOwner = "nextOwner";
        final String currentOwner = "currentOwner";
        final Lease lease = createDummyLease("lease1", currentOwner);
        leaseRefresher.createLeaseIfNotExists(lease);
        leaseRefresher.initiateGracefulLeaseHandoff(lease, nextOwner);
        final Lease updatedLease = leaseRefresher.getLease(lease.leaseKey());

        assertEquals(nextOwner, updatedLease.leaseOwner());
        assertEquals(currentOwner, updatedLease.checkpointOwner());
    }

    @Test
    void initiateGracefulLeaseHandoff_conditionalFailure() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        final String nextOwner = "nextOwner";
        final String currentOwner = "currentOwner";
        final Lease lease = createDummyLease("lease1", currentOwner);
        // should not assign if there is a checkpointOwner is not null.
        lease.checkpointOwner(currentOwner);
        leaseRefresher.createLeaseIfNotExists(lease);
        assertFalse(leaseRefresher.initiateGracefulLeaseHandoff(lease, nextOwner));
    }

    @Test
    void renewLease_testGracefulShutdown_updateLeaseWhenDetectedShutdown() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        DynamoDBLeaseRefresher leaseRefresherSpy = spy(leaseRefresher);
        setupTable(leaseRefresher);
        final String nextOwner = "nextOwner";
        final String currentOwner = "currentOwner";
        final Lease lease = createDummyLease("lease1", currentOwner);
        leaseRefresher.createLeaseIfNotExists(lease);
        leaseRefresher.initiateGracefulLeaseHandoff(lease, nextOwner);
        // remove local checkpointOwner and reset leaseOwner to pretend we don't know that shutdown is requested
        lease.checkpointOwner(null);
        lease.leaseOwner(currentOwner);
        // renew should see that the lease has the shutdown attributes and so mark them on the passed-in lease.
        assertTrue(leaseRefresherSpy.renewLease(lease));
        assertEquals(currentOwner, lease.checkpointOwner());
        assertEquals(nextOwner, lease.leaseOwner());
        assertEquals(lease, leaseRefresher.getLease(lease.leaseKey()));
        verify(leaseRefresherSpy, times(2)).renewLease(lease);
    }

    @Test
    void renewLease_testGracefulShutdown_conditionalFailureDueToNoLeaseInDdb_NotTryingToRenew() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        DynamoDBLeaseRefresher leaseRefresherSpy = spy(leaseRefresher);
        setupTable(leaseRefresher);
        final Lease lease = createDummyLease("lease1", "currentOwner");
        assertFalse(leaseRefresherSpy.renewLease(lease));
        verify(leaseRefresherSpy, times(1)).renewLease(lease);
    }

    @Test
    void renewLease_testGracefulShutdown_remoteLeaseHasDifferentOwner_NotTryingToRenew() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        DynamoDBLeaseRefresher leaseRefresherSpy = spy(leaseRefresher);
        final Lease lease = createDummyLease("lease1", "currentOwner");
        final Lease originalLease = lease.copy();
        leaseRefresher.createLeaseIfNotExists(lease);

        // call assignLease to change owner and call initiateGracefulLeaseHandoff to add shutdown attribute
        leaseRefresher.assignLease(lease, "nextOwner");
        leaseRefresher.initiateGracefulLeaseHandoff(lease, "nextOwner2");

        assertFalse(leaseRefresherSpy.renewLease(originalLease));
        verify(leaseRefresherSpy, times(1)).renewLease(originalLease);
    }

    @Test
    void renewLease_testGracefulShutdown_continueUpdateLeaseUntilLeaseIsTransferred() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        final String nextOwner = "nextOwner";
        final String currentOwner = "currentOwner";
        final Lease lease = createDummyLease("lease1", currentOwner);
        leaseRefresher.createLeaseIfNotExists(lease);

        assertTrue(leaseRefresher.initiateGracefulLeaseHandoff(lease, nextOwner));
        // try consecutive renews and see if they pass
        assertTrue(leaseRefresher.renewLease(lease));
        assertTrue(leaseRefresher.renewLease(lease));

        // now we call assignLease, this will remove the checkpointOwner attribute and increment leaseCounter
        final Long currentCounter = lease.leaseCounter();
        assertTrue(leaseRefresher.assignLease(lease, lease.leaseOwner()));
        assertEquals(currentCounter + 1, lease.leaseCounter());
        // On the lease renewal side, we want to pretend to simulate that the current owner doesn't know about the
        // lease re-assignment yet. So we reset leaseCounter and the owner fields.
        lease.leaseCounter(currentCounter);
        lease.leaseOwner(nextOwner);
        lease.checkpointOwner(currentOwner);
        assertFalse(leaseRefresher.renewLease(lease));
    }

    @Test
    void assignLease_alwaysRemoveCheckpointOwner() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        final String nextOwner = "nextOwner";
        final String currentOwner = "currentOwner";
        final Lease lease = createDummyLease("lease1", currentOwner);
        leaseRefresher.createLeaseIfNotExists(lease);
        leaseRefresher.initiateGracefulLeaseHandoff(lease, nextOwner);

        assertEquals(currentOwner, leaseRefresher.getLease(lease.leaseKey()).checkpointOwner());
        assertTrue(leaseRefresher.assignLease(lease, nextOwner));
        final Lease updatedLease = leaseRefresher.getLease(lease.leaseKey());
        assertNull(updatedLease.checkpointOwner());
        assertEquals(nextOwner, updatedLease.leaseOwner());
    }

    @Test
    void assignLease_conditionalFailureBecauseCheckpointOwnerIsNotExpected() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        final String nextOwner = "nextOwner";
        final String currentOwner = "currentOwner";

        final Lease lease = createDummyLease("lease1", nextOwner);
        lease.checkpointOwner(currentOwner);
        leaseRefresher.createLeaseIfNotExists(lease);

        lease.checkpointOwner("someone else now");
        assertFalse(leaseRefresher.assignLease(lease, lease.leaseOwner()));
    }

    @Test
    void createLeaseTableIfNotExists_billingModeProvisioned_assertCorrectModeAndCapacity() throws Exception {
        final DynamoDbAsyncClient dbAsyncClient = DynamoDBEmbedded.create().dynamoDbAsyncClient();
        final LeaseRefresher leaseRefresher = createLeaseRefresher(createProvisionedTableConfig(), dbAsyncClient);
        setupTable(leaseRefresher);

        final DescribeTableResponse describeTableResponse = dbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_LEASE_TABLE)
                        .build())
                .join();

        assertProvisionTableMode(describeTableResponse, 100L, 200L);
    }

    @Test
    void createLeaseTableIfNotExists_billingModeOnDemand_assertCorrectMode() throws Exception {
        final DynamoDbAsyncClient dbAsyncClient = DynamoDBEmbedded.create().dynamoDbAsyncClient();
        final LeaseRefresher leaseRefresher = createLeaseRefresher(createOnDemandTableConfig(), dbAsyncClient);
        setupTable(leaseRefresher);

        final DescribeTableResponse describeTableResponse = dbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_LEASE_TABLE)
                        .build())
                .join();

        assertOnDemandTableMode(describeTableResponse);
    }

    @Test
    void createLeaseTableIfNotExistsOverloadedMethod_billingModeOnDemand_assertProvisionModeWithOveridenCapacity()
            throws DependencyException, ProvisionedThroughputException {
        final DynamoDbAsyncClient dbAsyncClient = DynamoDBEmbedded.create().dynamoDbAsyncClient();
        final LeaseRefresher leaseRefresher = createLeaseRefresher(createOnDemandTableConfig(), dbAsyncClient);
        leaseRefresher.createLeaseTableIfNotExists(50L, 100L);
        leaseRefresher.waitUntilLeaseTableExists(1, 1000);

        final DescribeTableResponse describeTableResponse = dbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_LEASE_TABLE)
                        .build())
                .join();

        assertProvisionTableMode(describeTableResponse, 50L, 100L);
    }

    @Test
    void createLeaseTableIfNotExistsOverloadedMethod_billingModeProvisioned_assertProvisionModeWithOveridenCapacity()
            throws ProvisionedThroughputException, DependencyException {
        final DynamoDbAsyncClient dbAsyncClient = DynamoDBEmbedded.create().dynamoDbAsyncClient();
        final LeaseRefresher leaseRefresher = createLeaseRefresher(createProvisionedTableConfig(), dbAsyncClient);
        leaseRefresher.createLeaseTableIfNotExists(50L, 100L);
        leaseRefresher.waitUntilLeaseTableExists(1, 1000);

        final DescribeTableResponse describeTableResponse = dbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_LEASE_TABLE)
                        .build())
                .join();

        assertProvisionTableMode(describeTableResponse, 50L, 100L);
    }

    @Test
    void createLeaseOwnerToLeaseKeyIndexIfNotExists_baseTableInProvisionedMode_assertGSIInProvisionedMode()
            throws ProvisionedThroughputException, DependencyException {
        final DynamoDbAsyncClient dbAsyncClient = DynamoDBEmbedded.create().dynamoDbAsyncClient();
        final LeaseRefresher leaseRefresher = createLeaseRefresher(createProvisionedTableConfig(), dbAsyncClient);

        // Creates base table and GSI
        setupTableWithLeaseKeyIndex(leaseRefresher);

        final DescribeTableResponse describeTableResponse = dbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_LEASE_TABLE)
                        .build())
                .join();

        assertProvisionTableMode(describeTableResponse, 100L, 200L);
        assertEquals(
                100L,
                describeTableResponse
                        .table()
                        .globalSecondaryIndexes()
                        .get(0)
                        .provisionedThroughput()
                        .readCapacityUnits(),
                "GSI RCU is not 100L");
        assertEquals(
                200L,
                describeTableResponse
                        .table()
                        .globalSecondaryIndexes()
                        .get(0)
                        .provisionedThroughput()
                        .writeCapacityUnits(),
                "GSI RCU is not 100L");
    }

    @Test
    void createLeaseOwnerToLeaseKeyIndexIfNotExists_baseTableInOnDemandMode_assertGSIInOnDemandMode()
            throws ProvisionedThroughputException, DependencyException {
        final DynamoDbAsyncClient dbAsyncClient = DynamoDBEmbedded.create().dynamoDbAsyncClient();
        final LeaseRefresher leaseRefresher = createLeaseRefresher(createOnDemandTableConfig(), dbAsyncClient);

        // Creates base table and GSI
        setupTableWithLeaseKeyIndex(leaseRefresher);

        final DescribeTableResponse describeTableResponse = dbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_LEASE_TABLE)
                        .build())
                .join();

        assertOnDemandTableMode(describeTableResponse);
        assertEquals(
                0L,
                describeTableResponse
                        .table()
                        .globalSecondaryIndexes()
                        .get(0)
                        .provisionedThroughput()
                        .readCapacityUnits(),
                "GSI RCU is not 100L");
        assertEquals(
                0L,
                describeTableResponse
                        .table()
                        .globalSecondaryIndexes()
                        .get(0)
                        .provisionedThroughput()
                        .writeCapacityUnits(),
                "GSI RCU is not 100L");
    }

    @Test
    public void takeLease_removesCheckpointOwner() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        final Lease lease = createPendingCheckpointOwnerLease(leaseRefresher);
        assertTrue(leaseRefresher.takeLease(lease, "newOwner"));

        final Lease updatedLease = leaseRefresher.getLease(lease.leaseKey());
        assertEquals(lease, updatedLease);
        assertNull(updatedLease.checkpointOwner());
    }

    @Test
    public void evictLease_removesCheckpointOwner() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        final Lease lease = createPendingCheckpointOwnerLease(leaseRefresher);
        final long originalCounter = lease.leaseCounter();
        assertTrue(leaseRefresher.evictLease(lease));

        final Lease updatedLease = leaseRefresher.getLease(lease.leaseKey());
        assertEquals(lease, updatedLease);
        assertNull(updatedLease.checkpointOwner());
        assertNotNull(updatedLease.leaseOwner());
        assertEquals(originalCounter + 1, lease.leaseCounter());
    }

    @Test
    public void evictLease_removesOwnerIfCheckpointOwnerIsNull() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        final Lease lease = createDummyLease("1", "ownerA");
        final long originalCounter = lease.leaseCounter();
        leaseRefresher.createLeaseIfNotExists(lease);
        assertTrue(leaseRefresher.evictLease(lease));

        final Lease updatedLease = leaseRefresher.getLease(lease.leaseKey());
        assertEquals(lease, updatedLease);
        assertNull(updatedLease.checkpointOwner());
        assertNull(updatedLease.leaseOwner());
        assertEquals(originalCounter + 1, lease.leaseCounter());
    }

    @Test
    public void evictLease_noOpIfLeaseNotExists() throws Exception {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        final Lease lease = createDummyLease("1", "ownerA");
        assertFalse(leaseRefresher.evictLease(lease));

        // now evictLease should use the notExist condition to try updating the lease.
        // we want to see it fails
        lease.leaseOwner(null);
        assertFalse(leaseRefresher.evictLease(lease));
    }

    private Lease createPendingCheckpointOwnerLease(final LeaseRefresher leaseRefresher) throws Exception {
        final Lease lease = createDummyLease("1", "ownerA");
        lease.checkpointOwner("checkpointOwner");
        leaseRefresher.createLeaseIfNotExists(lease);
        return lease;
    }

    private static void assertOnDemandTableMode(final DescribeTableResponse describeTableResponse) {
        assertEquals(
                BillingMode.PAY_PER_REQUEST,
                describeTableResponse.table().billingModeSummary().billingMode(),
                "Table mode is not PAY_PER_REQUEST");
        assertEquals(
                0L,
                describeTableResponse.table().provisionedThroughput().readCapacityUnits(),
                "PAY_PER_REQUEST mode on table does not have 0 RCU");
        assertEquals(
                0L,
                describeTableResponse.table().provisionedThroughput().writeCapacityUnits(),
                "PAY_PER_REQUEST mode on table does not have 0 WCU");
    }

    private static void assertProvisionTableMode(
            final DescribeTableResponse describeTableResponse, final long rcu, final long wcu) {
        // BillingModeSummary is null in case of PROVISIONED
        assertNull(
                describeTableResponse.table().billingModeSummary(), "BillingModeSummary is not null for provisionMode");
        assertEquals(
                rcu,
                describeTableResponse.table().provisionedThroughput().readCapacityUnits(),
                "RCU set on the Table is incorrect");
        assertEquals(
                wcu,
                describeTableResponse.table().provisionedThroughput().writeCapacityUnits(),
                "WCU set on the Table is incorrect");
    }

    private static DdbTableConfig createProvisionedTableConfig() {
        final DdbTableConfig ddbTableConfig = new DdbTableConfig();
        ddbTableConfig.billingMode(BillingMode.PROVISIONED);
        ddbTableConfig.readCapacity(100);
        ddbTableConfig.writeCapacity(200);
        return ddbTableConfig;
    }

    private static DdbTableConfig createOnDemandTableConfig() {
        final DdbTableConfig ddbTableConfig = new DdbTableConfig();
        ddbTableConfig.billingMode(BillingMode.PAY_PER_REQUEST);
        return ddbTableConfig;
    }

    private DynamoDBLeaseRefresher createLeaseRefresher(
            final DdbTableConfig ddbTableConfig, final DynamoDbAsyncClient dynamoDbAsyncClient) {
        return createLeaseRefresher(ddbTableConfig, dynamoDbAsyncClient, false, false);
    }

    private DynamoDBLeaseRefresher createLeaseRefresher(
            final DdbTableConfig ddbTableConfig,
            final DynamoDbAsyncClient dynamoDbAsyncClient,
            boolean deletionProtectionEnabled,
            boolean pitrEnabled) {
        return new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                dynamoDbAsyncClient,
                new DynamoDBLeaseSerializer(),
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                ddbTableConfig,
                deletionProtectionEnabled,
                pitrEnabled,
                new ArrayList<>());
    }

    private Lease createDummyLease(final String leaseKey, final String leaseOwner) {
        final Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.leaseOwner(leaseOwner);
        lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        return lease;
    }

    private void setupTable(final LeaseRefresher refresher) throws ProvisionedThroughputException, DependencyException {
        refresher.createLeaseTableIfNotExists();
        refresher.waitUntilLeaseTableExists(1, 100);
    }

    private void setupTableWithLeaseKeyIndex(final LeaseRefresher refresher)
            throws ProvisionedThroughputException, DependencyException {
        refresher.createLeaseTableIfNotExists();
        refresher.waitUntilLeaseTableExists(1, 100);
        refresher.createLeaseOwnerToLeaseKeyIndexIfNotExists();
        refresher.waitUntilLeaseOwnerToLeaseKeyIndexExists(1, 100);
    }

    // This entry is bad as it does not have required field and thus deserialization fails.
    private void createAndPutBadLeaseEntryInTable() {
        final PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(TEST_LEASE_TABLE)
                .item(ImmutableMap.of(
                        "leaseKey", AttributeValue.builder().s("badLeaseKey").build()))
                .build();

        dynamoDbAsyncClient.putItem(putItemRequest);
    }

    @Test
    void listLeases_returnsAllLeases_whenNoLimitSpecified()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", "owner1"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease2", "owner2"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease3", "owner3"));

        List<Lease> result = leaseRefresher.listLeases();

        assertEquals(3, result.size());
    }

    @Test
    void listLeases_withLimit_returnsOnlyLimitedLeases()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", "owner1"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease2", "owner2"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease3", "owner3"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease4", "owner4"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease5", "owner5"));

        List<Lease> result = leaseRefresher.list(2, null);

        assertEquals(2, result.size());
    }

    @Test
    void listLeases_withLimitGreaterThanTotal_returnsAllLeases()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", "owner1"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease2", "owner2"));

        List<Lease> result = leaseRefresher.list(10, null);

        assertEquals(2, result.size());
    }

    @Test
    void listLeases_withLimit1_returnsExactlyOneLease()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", "owner1"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease2", "owner2"));
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease3", "owner3"));

        List<Lease> result = leaseRefresher.list(1, null);

        assertEquals(1, result.size());
    }

    @Test
    void isLeaseTableEmpty_returnsTrue_whenTableIsEmpty()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);

        assertTrue(leaseRefresher.isLeaseTableEmpty());
    }

    @Test
    void isLeaseTableEmpty_returnsFalse_whenTableHasLeases()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        DynamoDBLeaseRefresher leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);
        setupTable(leaseRefresher);
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", "owner1"));

        assertFalse(leaseRefresher.isLeaseTableEmpty());
    }

    @Test
    void isLeaseTableEmpty_returnsFalse_whenNonLeaseEntitiesOnFirstPageAndLeaseOnSecondPage()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        // Simulate a table with non-lease entities (e.g. WorkerMetricStats, CoordinatorState)
        // returned on the first page, and the actual lease on the second page.
        // The do-while loop must paginate past non-lease entities to find the lease.
        DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class);
        DynamoDBLeaseSerializer serializer = new DynamoDBLeaseSerializer();

        DynamoDBLeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                mockDdbClient,
                serializer,
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                false,
                false,
                new ArrayList<>());

        // Non-lease entities: items with entityType != "LEASE" (e.g. "WORKER_METRIC_STATS", "COORDINATOR_STATE")
        // fromDynamoRecord returns null for these, so they don't count toward the limit
        Map<String, AttributeValue> workerMetricStatsItem = ImmutableMap.of(
                "leaseKey",
                        AttributeValue.builder()
                                .s("AAA_workerMetricStats_worker1")
                                .build(),
                "entityType", AttributeValue.builder().s("WORKER_METRIC_STATS").build());
        Map<String, AttributeValue> coordinatorStateItem = ImmutableMap.of(
                "leaseKey",
                        AttributeValue.builder().s("AAA_coordinatorState_key1").build(),
                "entityType", AttributeValue.builder().s("COORDINATOR_STATE").build());

        // Actual lease item on the second page
        Map<String, AttributeValue> leaseItem = serializer.toDynamoRecord(createDummyLease("zzz_lease1", "owner1"));

        // Page 1: only non-lease entities, has more pages
        ScanResponse page1 = ScanResponse.builder()
                .items(workerMetricStatsItem, coordinatorStateItem)
                .lastEvaluatedKey(ImmutableMap.of(
                        "leaseKey",
                        AttributeValue.builder().s("AAA_coordinatorState_key1").build()))
                .build();

        // Page 2: the actual lease
        ScanResponse page2 = ScanResponse.builder().items(leaseItem).build();

        when(mockDdbClient.scan(any(ScanRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(page1))
                .thenReturn(CompletableFuture.completedFuture(page2));

        // isLeaseTableEmpty uses list(1, null) - must paginate past non-lease entities
        assertFalse(leaseRefresher.isLeaseTableEmpty());
        // Verify both pages were fetched
        verify(mockDdbClient, times(2)).scan(any(ScanRequest.class));
    }

    @Test
    void isLeaseTableEmpty_returnsTrue_whenOnlyNonLeaseEntitiesExist()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        // Table has only non-lease entities (WorkerMetricStats, CoordinatorState).
        // isLeaseTableEmpty should return true because no actual leases exist.
        DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class);
        DynamoDBLeaseSerializer serializer = new DynamoDBLeaseSerializer();

        DynamoDBLeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                mockDdbClient,
                serializer,
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                false,
                false,
                new ArrayList<>());

        Map<String, AttributeValue> workerMetricStatsItem = ImmutableMap.of(
                "leaseKey",
                        AttributeValue.builder()
                                .s("AAA_workerMetricStats_worker1")
                                .build(),
                "entityType", AttributeValue.builder().s("WORKER_METRIC_STATS").build());
        Map<String, AttributeValue> coordinatorStateItem = ImmutableMap.of(
                "leaseKey",
                        AttributeValue.builder().s("AAA_coordinatorState_key1").build(),
                "entityType", AttributeValue.builder().s("COORDINATOR_STATE").build());

        // Single page with only non-lease entities, no more pages
        ScanResponse page1 = ScanResponse.builder()
                .items(workerMetricStatsItem, coordinatorStateItem)
                .build();

        when(mockDdbClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(page1));

        assertTrue(leaseRefresher.isLeaseTableEmpty());
        verify(mockDdbClient, times(1)).scan(any(ScanRequest.class));
    }

    @Test
    void listLeases_paginatesAcrossMultiplePages_returnsAllLeases()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        // Use mocks to simulate DDB pagination with lastEvaluatedKey
        DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class);
        DynamoDBLeaseSerializer serializer = new DynamoDBLeaseSerializer();

        DynamoDBLeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                mockDdbClient,
                serializer,
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                false,
                false,
                new ArrayList<>());

        Map<String, AttributeValue> item1 = serializer.toDynamoRecord(createDummyLease("lease1", "owner1"));
        Map<String, AttributeValue> item2 = serializer.toDynamoRecord(createDummyLease("lease2", "owner2"));
        Map<String, AttributeValue> item3 = serializer.toDynamoRecord(createDummyLease("lease3", "owner3"));

        ScanResponse page1 = ScanResponse.builder()
                .items(item1, item2)
                .lastEvaluatedKey(ImmutableMap.of(
                        "leaseKey", AttributeValue.builder().s("lease2").build()))
                .build();
        ScanResponse page2 = ScanResponse.builder().items(item3).build();

        when(mockDdbClient.scan(any(ScanRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(page1))
                .thenReturn(CompletableFuture.completedFuture(page2));

        List<Lease> result = leaseRefresher.listLeases();

        assertEquals(3, result.size());
        verify(mockDdbClient, times(2)).scan(any(ScanRequest.class));
    }

    @Test
    void listLeases_paginationStopsWhenLimitReached_doesNotFetchNextPage()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        // Use mocks to simulate DDB pagination and verify limit stops pagination
        DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class);
        DynamoDBLeaseSerializer serializer = new DynamoDBLeaseSerializer();

        DynamoDBLeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                mockDdbClient,
                serializer,
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                false,
                false,
                new ArrayList<>());

        Map<String, AttributeValue> item1 = serializer.toDynamoRecord(createDummyLease("lease1", "owner1"));
        Map<String, AttributeValue> item2 = serializer.toDynamoRecord(createDummyLease("lease2", "owner2"));
        Map<String, AttributeValue> item3 = serializer.toDynamoRecord(createDummyLease("lease3", "owner3"));

        // Page 1 has 2 items and indicates more pages
        ScanResponse page1 = ScanResponse.builder()
                .items(item1, item2)
                .lastEvaluatedKey(ImmutableMap.of(
                        "leaseKey", AttributeValue.builder().s("lease2").build()))
                .build();
        ScanResponse page2 = ScanResponse.builder().items(item3).build();

        when(mockDdbClient.scan(any(ScanRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(page1))
                .thenReturn(CompletableFuture.completedFuture(page2));

        // Limit=2 should be satisfied on first page, no second scan
        List<Lease> result = leaseRefresher.list(2, null);

        assertEquals(2, result.size());
        verify(mockDdbClient, times(1)).scan(any(ScanRequest.class));
    }

    @Test
    void listLeases_paginationContinuesAcrossPages_untilLimitReached()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        // Use mocks to verify pagination continues until limit is satisfied
        DynamoDbAsyncClient mockDdbClient = mock(DynamoDbAsyncClient.class);
        DynamoDBLeaseSerializer serializer = new DynamoDBLeaseSerializer();

        DynamoDBLeaseRefresher leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                mockDdbClient,
                serializer,
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                false,
                false,
                new ArrayList<>());

        Map<String, AttributeValue> item1 = serializer.toDynamoRecord(createDummyLease("lease1", "owner1"));
        Map<String, AttributeValue> item2 = serializer.toDynamoRecord(createDummyLease("lease2", "owner2"));
        Map<String, AttributeValue> item3 = serializer.toDynamoRecord(createDummyLease("lease3", "owner3"));

        // 1 item per page, 3 pages total
        ScanResponse page1 = ScanResponse.builder()
                .items(item1)
                .lastEvaluatedKey(ImmutableMap.of(
                        "leaseKey", AttributeValue.builder().s("lease1").build()))
                .build();
        ScanResponse page2 = ScanResponse.builder()
                .items(item2)
                .lastEvaluatedKey(ImmutableMap.of(
                        "leaseKey", AttributeValue.builder().s("lease2").build()))
                .build();
        ScanResponse page3 = ScanResponse.builder().items(item3).build();

        when(mockDdbClient.scan(any(ScanRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(page1))
                .thenReturn(CompletableFuture.completedFuture(page2))
                .thenReturn(CompletableFuture.completedFuture(page3));

        // Limit=2, needs 2 pages (1 item each)
        List<Lease> result = leaseRefresher.list(2, null);

        assertEquals(2, result.size());
        verify(mockDdbClient, times(2)).scan(any(ScanRequest.class));
    }
}
