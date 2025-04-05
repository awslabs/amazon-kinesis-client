package com.amazonaws.services.kinesis.clientlibrary.utils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@AllArgsConstructor
public class LeaseTableManager extends AWSResourceManager {

    private final AmazonDynamoDB dynamoClient;
    private static final String ACTIVE_TABLE_STATE = "ACTIVE";

    public boolean isResourceActive(String tableName) {

        DescribeTableRequest request = new DescribeTableRequest();
        request.withTableName(tableName);

        try {
            final DescribeTableResult response = this.dynamoClient.describeTable(request);
            String tableStatus = response.getTable().getTableStatus();
            boolean isActive = tableStatus.equals(ACTIVE_TABLE_STATE);
            if (!isActive) {
                throw new RuntimeException("Table is not active, instead in status: " + response.getTable().getTableStatus());
            }
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteResourceCall(String tableName) throws Exception {
        DeleteTableRequest request = new DeleteTableRequest();
        request.setTableName(tableName);
        this.dynamoClient.deleteTable(request);
    }

    public List<String> getAllResourceNames() throws Exception {
        ListTablesRequest listTableRequest = new ListTablesRequest();
        List<String> allTableNames = new ArrayList<>();
        ListTablesResult result = null;
        do {
            result = dynamoClient.listTables(listTableRequest);
            allTableNames.addAll(result.getTableNames());
            listTableRequest = listTableRequest.withExclusiveStartTableName(result.getLastEvaluatedTableName());
        } while (result.getLastEvaluatedTableName() != null);
        return allTableNames;
    }
}
