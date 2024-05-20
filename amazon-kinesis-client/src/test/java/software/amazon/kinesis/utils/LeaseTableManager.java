package software.amazon.kinesis.utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

@Slf4j
@AllArgsConstructor
public class LeaseTableManager extends AWSResourceManager {

    private final DynamoDbAsyncClient dynamoClient;

    public boolean isResourceActive(String tableName) {
        final DescribeTableRequest request =
                DescribeTableRequest.builder().tableName(tableName).build();
        final CompletableFuture<DescribeTableResponse> describeTableResponseCompletableFuture =
                dynamoClient.describeTable(request);

        try {
            final DescribeTableResponse response = describeTableResponseCompletableFuture.get(30, TimeUnit.SECONDS);
            boolean isActive = response.table().tableStatus().equals(TableStatus.ACTIVE);
            if (!isActive) {
                throw new RuntimeException("Table is not active, instead in status: "
                        + response.table().tableStatus());
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

    public void deleteResourceCall(String tableName) throws Exception {
        final DeleteTableRequest request =
                DeleteTableRequest.builder().tableName(tableName).build();
        FutureUtils.resolveOrCancelFuture(dynamoClient.deleteTable(request), Duration.ofSeconds(60));
    }

    public List<String> getAllResourceNames() throws Exception {
        ListTablesRequest listTableRequest = ListTablesRequest.builder().build();
        List<String> allTableNames = new ArrayList<>();
        ListTablesResponse result = null;
        do {
            result = FutureUtils.resolveOrCancelFuture(
                    dynamoClient.listTables(listTableRequest), Duration.ofSeconds(60));
            allTableNames.addAll(result.tableNames());
            listTableRequest = ListTablesRequest.builder()
                    .exclusiveStartTableName(result.lastEvaluatedTableName())
                    .build();
        } while (result.lastEvaluatedTableName() != null);
        return allTableNames;
    }
}
