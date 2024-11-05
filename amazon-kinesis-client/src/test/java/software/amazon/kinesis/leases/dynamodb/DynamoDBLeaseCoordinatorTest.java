package software.amazon.kinesis.leases.dynamodb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.IndexStatus;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.kinesis.leases.dynamodb.TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK;

class DynamoDBLeaseCoordinatorTest {

    private static final String TEST_LEASE_TABLE = "SomeTable";
    private DynamoDBLeaseRefresher leaseRefresher;
    private DynamoDBLeaseCoordinator dynamoDBLeaseCoordinator;
    private final DynamoDbAsyncClient dynamoDbAsyncClient =
            DynamoDBEmbedded.create().dynamoDbAsyncClient();

    @BeforeEach
    void setUp() {
        this.leaseRefresher = new DynamoDBLeaseRefresher(
                TEST_LEASE_TABLE,
                dynamoDbAsyncClient,
                new DynamoDBLeaseSerializer(),
                true,
                NOOP_TABLE_CREATOR_CALLBACK,
                Duration.ofSeconds(10),
                new DdbTableConfig(),
                true,
                true,
                new ArrayList<>());
    }

    // TODO - move this test to migration state machine which creates the GSI
    @Disabled
    @Test
    void initialize_withLeaseAssignmentManagerMode_assertIndexOnTable()
            throws ProvisionedThroughputException, DependencyException {

        constructCoordinatorAndInitialize();

        final DescribeTableResponse response = dynamoDbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_LEASE_TABLE)
                        .build())
                .join();

        assertEquals(1, response.table().globalSecondaryIndexes().size());
        assertEquals(
                IndexStatus.ACTIVE,
                response.table().globalSecondaryIndexes().get(0).indexStatus());
    }

    // TODO - move this to migration state machine test
    @Disabled
    @Test
    void initialize_withDefaultMode_assertIndexInCreating() throws ProvisionedThroughputException, DependencyException {
        constructCoordinatorAndInitialize();

        final DescribeTableResponse response = dynamoDbAsyncClient
                .describeTable(DescribeTableRequest.builder()
                        .tableName(TEST_LEASE_TABLE)
                        .build())
                .join();

        assertEquals(1, response.table().globalSecondaryIndexes().size());
        assertEquals(
                IndexStatus.CREATING,
                response.table().globalSecondaryIndexes().get(0).indexStatus());
    }

    private void constructCoordinatorAndInitialize() throws ProvisionedThroughputException, DependencyException {
        this.dynamoDBLeaseCoordinator = new DynamoDBLeaseCoordinator(
                leaseRefresher,
                "Identifier",
                100L,
                true,
                100L,
                10,
                10,
                10,
                100L,
                100L,
                new NullMetricsFactory(),
                new LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig(),
                LeaseManagementConfig.GracefulLeaseHandoffConfig.builder().build(),
                new ConcurrentHashMap<>());
        this.dynamoDBLeaseCoordinator.initialize();
    }
}
