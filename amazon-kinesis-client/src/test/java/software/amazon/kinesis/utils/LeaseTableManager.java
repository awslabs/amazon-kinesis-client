package software.amazon.kinesis.utils;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.kinesis.common.FutureUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LeaseTableManager {

    private final DynamoDbAsyncClient dynamoClient;

    public LeaseTableManager(DynamoDbAsyncClient dynamoClient) throws URISyntaxException, IOException {
        this.dynamoClient = dynamoClient;
    }

    private List<String> listAllLeaseTables() throws Exception {
        final ListTablesRequest request = ListTablesRequest.builder().build();
        final ListTablesResponse response;
        try {
            response = FutureUtils.resolveOrCancelFuture(dynamoClient.listTables(request), Duration.ofSeconds(60));
        } catch (ExecutionException | InterruptedException e) {
            throw new Exception("Error listing all lease tables");
        }
        return response.tableNames();
    }

    public void deleteLeaseTable(String tableName) throws Exception {
        final DeleteTableRequest request = DeleteTableRequest.builder().tableName(tableName).build();
        try {
            FutureUtils.resolveOrCancelFuture(dynamoClient.deleteTable(request), Duration.ofSeconds(60));
        } catch (ExecutionException | InterruptedException e) {
            throw new Exception("Could not delete lease table: {}", e);
        }

        // Wait till table is deleted to return
        int i = 0;
        while (true) {
            i++;
            if (i > 100) {
                throw new RuntimeException("Failed lease table deletion");
            }

            List<String> leaseTableNames = listAllLeaseTables();
            log.info("All lease tables name: {}. Looking for: {}", leaseTableNames, tableName);
            if (!listAllLeaseTables().contains(tableName)) {
                log.info("Succesfully deleted the lease table {}", tableName);
                return;
            } else {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                } catch (InterruptedException e1) {}
                log.info("Lease table {} is not deleted yet, exception: ", tableName);
            }
        }
    }

    public void deleteAllLeaseTables() throws Exception {

        final List<String> tableNames = listAllLeaseTables();
        for (String tableName : tableNames) {
            deleteLeaseTable(tableName);
        }
    }

}
