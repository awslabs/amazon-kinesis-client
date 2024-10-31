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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
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
    private DynamoDBLeaseRefresher leaseRefresher;
    private final DynamoDbAsyncClient dynamoDbAsyncClient =
            DynamoDBEmbedded.create().dynamoDbAsyncClient();

    @BeforeEach
    void setup() throws ProvisionedThroughputException, DependencyException {
        this.leaseRefresher = createLeaseRefresher(new DdbTableConfig(), dynamoDbAsyncClient);

        this.leaseRefresher.createLeaseTableIfNotExists();
        this.leaseRefresher.waitUntilLeaseTableExists(1, 30);
    }

    @Test
    void createWorkerIdToLeaseKeyIndexIfNotExists_sanity() throws DependencyException {
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

    // TODO Add CreateLeaseTablePITR test and CreateLeaseTableDeletionProtection test

    @Test
    void waitUntilLeaseOwnerToLeaseKeyIndexExists_noTransitionToActive_assertFalse() throws DependencyException {
        dynamoDbAsyncClient.deleteTable(
                DeleteTableRequest.builder().tableName(TEST_LEASE_TABLE).build());

        final boolean response = leaseRefresher.waitUntilLeaseOwnerToLeaseKeyIndexExists(1, 3);
        assertFalse(response);
        assertFalse(leaseRefresher.isLeaseOwnerToLeaseKeyIndexActive());
    }

    @Test
    void isLeaseOwnerGsiIndexActive() throws DependencyException {
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
        setupTable();
        leaseRefresher.createLeaseIfNotExists(createDummyLease("lease1", "leaseOwner1"));

        // Fetch a lease from assign it to owner2
        boolean response = leaseRefresher.assignLease(leaseRefresher.getLease("lease1"), "leaseOwner2");
        assertTrue(response);
        assertEquals(leaseRefresher.getLease("lease1").leaseOwner(), "leaseOwner2");
    }

    @Test
    void assignLease_unassignedLease_assertAssignmentToNewOwner()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        setupTable();
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
        setupTable();
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
        setupTable();
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
        setupTable();
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
        setupTable();
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
        setupTable();
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
    void initiateGracefulLeaseHandoff_sanity() throws Exception {
        setupTable();
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
        setupTable();
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
        leaseRefresher = spy(leaseRefresher);
        final String nextOwner = "nextOwner";
        final String currentOwner = "currentOwner";
        final Lease lease = createDummyLease("lease1", currentOwner);
        leaseRefresher.createLeaseIfNotExists(lease);
        leaseRefresher.initiateGracefulLeaseHandoff(lease, nextOwner);
        // remove local checkpointOwner and reset leaseOwner to pretend we don't know that shutdown is requested
        lease.checkpointOwner(null);
        lease.leaseOwner(currentOwner);
        // renew should see that the lease has the shutdown attributes and so mark them on the passed-in lease.
        assertTrue(leaseRefresher.renewLease(lease));
        assertEquals(currentOwner, lease.checkpointOwner());
        assertEquals(nextOwner, lease.leaseOwner());
        assertEquals(lease, leaseRefresher.getLease(lease.leaseKey()));
        verify(leaseRefresher, times(2)).renewLease(lease);
    }

    @Test
    void renewLease_testGracefulShutdown_conditionalFailureDueToNoLeaseInDdb_NotTryingToRenew() throws Exception {
        leaseRefresher = spy(leaseRefresher);
        final Lease lease = createDummyLease("lease1", "currentOwner");
        assertFalse(leaseRefresher.renewLease(lease));
        verify(leaseRefresher, times(1)).renewLease(lease);
    }

    @Test
    void renewLease_testGracefulShutdown_remoteLeaseHasDifferentOwner_NotTryingToRenew() throws Exception {
        leaseRefresher = spy(leaseRefresher);
        final Lease lease = createDummyLease("lease1", "currentOwner");
        final Lease originalLease = lease.copy();
        leaseRefresher.createLeaseIfNotExists(lease);

        // call assignLease to change owner and call initiateGracefulLeaseHandoff to add shutdown attribute
        leaseRefresher.assignLease(lease, "nextOwner");
        leaseRefresher.initiateGracefulLeaseHandoff(lease, "nextOwner2");

        assertFalse(leaseRefresher.renewLease(originalLease));
        verify(leaseRefresher, times(1)).renewLease(originalLease);
    }

    @Test
    void renewLease_testGracefulShutdown_continueUpdateLeaseUntilLeaseIsTransferred() throws Exception {
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
        setupTable();
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
        setupTable();
        final String nextOwner = "nextOwner";
        final String currentOwner = "currentOwner";

        final Lease lease = createDummyLease("lease1", nextOwner);
        lease.checkpointOwner(currentOwner);
        leaseRefresher.createLeaseIfNotExists(lease);

        lease.checkpointOwner("someone else now");
        assertFalse(leaseRefresher.assignLease(lease, lease.leaseOwner()));
    }

    void createLeaseTableIfNotExists_billingModeProvisioned_assertCorrectModeAndCapacity() throws Exception {
        final DynamoDbAsyncClient dbAsyncClient = DynamoDBEmbedded.create().dynamoDbAsyncClient();
        final LeaseRefresher leaseRefresher = createLeaseRefresher(createProvisionedTableConfig(), dbAsyncClient);
        leaseRefresher.createLeaseTableIfNotExists();
        leaseRefresher.waitUntilLeaseTableExists(1, 1000);

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
        leaseRefresher.createLeaseTableIfNotExists();
        leaseRefresher.waitUntilLeaseTableExists(1, 1000);

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
        setupTable(leaseRefresher);

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
        setupTable(leaseRefresher);

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
        final Lease lease = createPendingCheckpointOwnerLease();
        assertTrue(leaseRefresher.takeLease(lease, "newOwner"));

        final Lease updatedLease = leaseRefresher.getLease(lease.leaseKey());
        assertEquals(lease, updatedLease);
        assertNull(updatedLease.checkpointOwner());
    }

    @Test
    public void evictLease_removesCheckpointOwner() throws Exception {
        final Lease lease = createPendingCheckpointOwnerLease();
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
        final Lease lease = createDummyLease("1", "ownerA");
        assertFalse(leaseRefresher.evictLease(lease));

        // now evictLease should use the notExist condition to try updating the lease.
        // we want to see it fails
        lease.leaseOwner(null);
        assertFalse(leaseRefresher.evictLease(lease));
    }

    private Lease createPendingCheckpointOwnerLease() throws Exception {
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
        return new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                dynamoDbAsyncClient,
                new DynamoDBLeaseSerializer(),
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                ddbTableConfig,
                true,
                false,
                new ArrayList<>());
    }

    private Lease createDummyLease(final String leaseKey, final String leaseOwner) {
        final Lease lease = new Lease();
        lease.leaseKey(leaseKey);
        lease.leaseOwner(leaseOwner);
        lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        return lease;
    }

    private void setupTable() throws ProvisionedThroughputException, DependencyException {
        setupTable(leaseRefresher);
    }

    private void setupTable(final LeaseRefresher refresher) throws ProvisionedThroughputException, DependencyException {
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
}
