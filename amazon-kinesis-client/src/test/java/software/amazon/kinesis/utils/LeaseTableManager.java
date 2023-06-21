package software.amazon.kinesis.utils;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.kinesis.common.FutureUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@AllArgsConstructor
public class LeaseTableManager extends AWSResourceManager {

    private final DynamoDbAsyncClient dynamoClient;

    public boolean _isResourceActive(String tableName) {
        final DescribeTableRequest request = DescribeTableRequest.builder().tableName(tableName).build();
        final CompletableFuture<DescribeTableResponse> describeTableResponseCompletableFuture = dynamoClient.describeTable(request);

        try {
            final DescribeTableResponse response = describeTableResponseCompletableFuture.get(30, TimeUnit.SECONDS);
            boolean isActive = response.table().tableStatus().equals(TableStatus.ACTIVE);
            if (!isActive) {
                throw new RuntimeException("Table is not active, instead in status: " + response.table().tableStatus());
            }
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ResourceNotFoundException) {
                return false;
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void _deleteResource(String tableName) throws Exception {
        final DeleteTableRequest request = DeleteTableRequest.builder().tableName(tableName).build();
        FutureUtils.resolveOrCancelFuture(dynamoClient.deleteTable(request), Duration.ofSeconds(60));
    }

    public List<String> _getAllResourceNames() throws Exception {
        List<String> tableNames = new ArrayList<>();
        ListTablesRequest request = ListTablesRequest.builder().build();
        ListTablesResponse response = null;
        String startTableName = null;

        // Continue while paginated call is still returning table names
        while(response == null || response.lastEvaluatedTableName() != null) {
            if (startTableName != null) {
                request = ListTablesRequest.builder().exclusiveStartTableName(startTableName).build();
            }
            try {
                response = FutureUtils.resolveOrCancelFuture(dynamoClient.listTables(request), Duration.ofSeconds(60));
            } catch (ExecutionException | InterruptedException e) {
                throw new Exception("Error listing all lease tables");
            }
            // Add all table names to list
            tableNames.addAll(response.tableNames());
            // Set startTableName for next call to be the last table name evaluated in current call
            startTableName = response.lastEvaluatedTableName();
        }
        return tableNames;
    }

}
