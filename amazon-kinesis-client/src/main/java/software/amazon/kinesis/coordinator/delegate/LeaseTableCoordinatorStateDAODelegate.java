package software.amazon.kinesis.coordinator.delegate;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;

public class LeaseTableCoordinatorStateDAODelegate extends CoordinatorStateDAODelegate {

    public LeaseTableCoordinatorStateDAODelegate(final DynamoDbAsyncClient dynamoDbAsyncClient, String leaseTableName) {
        super(dynamoDbAsyncClient, leaseTableName, DynamoDBLeaseSerializer.LEASE_KEY_KEY);
    }

    @Override
    public void initialize() {
        // No-op for lease table — always available
    }
}
