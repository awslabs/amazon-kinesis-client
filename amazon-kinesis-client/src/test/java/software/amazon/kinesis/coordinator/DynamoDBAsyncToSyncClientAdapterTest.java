package software.amazon.kinesis.coordinator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamoDBAsyncToSyncClientAdapterTest {

    private static final String TEST_TABLE_NAME = "TestTable";

    @Mock
    private DynamoDbAsyncClient mockAsyncClient;

    private DynamoDbAsyncToSyncClientAdapter adapter;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        adapter = new DynamoDbAsyncToSyncClientAdapter(mockAsyncClient);
    }

    @Test
    public void testGetItem() {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("1").build());
        final Map<String, AttributeValue> item = new HashMap<>(key);
        item.put("data", AttributeValue.builder().s("test data").build());
        final GetItemRequest request =
                GetItemRequest.builder().key(key).tableName(TEST_TABLE_NAME).build();
        final GetItemResponse expectedResponse =
                GetItemResponse.builder().item(item).build();
        when(mockAsyncClient.getItem(request)).thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final GetItemResponse actualResponse = adapter.getItem(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).getItem(request);
    }

    @Test
    public void testPutItem() {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("1").build());
        item.put("data", AttributeValue.builder().s("test data").build());
        final PutItemRequest request =
                PutItemRequest.builder().tableName(TEST_TABLE_NAME).item(item).build();
        final PutItemResponse expectedResponse = PutItemResponse.builder().build();
        when(mockAsyncClient.putItem(request)).thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final PutItemResponse actualResponse = adapter.putItem(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).putItem(request);
    }

    @Test
    public void testUpdateItem() {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("1").build());

        final Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put(
                "data",
                AttributeValueUpdate.builder()
                        .value(AttributeValue.builder().s("updated data").build())
                        .action(AttributeAction.PUT)
                        .build());

        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(TEST_TABLE_NAME)
                .key(key)
                .attributeUpdates(updates)
                .build();

        final UpdateItemResponse expectedResponse = UpdateItemResponse.builder().build();

        when(mockAsyncClient.updateItem(request)).thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final UpdateItemResponse actualResponse = adapter.updateItem(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).updateItem(request);
    }

    @Test
    public void testDeleteItem() {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("1").build());
        final DeleteItemResponse expectedResponse = DeleteItemResponse.builder().build();
        final DeleteItemRequest request =
                DeleteItemRequest.builder().tableName(TEST_TABLE_NAME).key(key).build();
        when(mockAsyncClient.deleteItem(request)).thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final DeleteItemResponse actualResponse = adapter.deleteItem(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).deleteItem(request);
    }

    @Test
    public void testCreateTable() {
        final CreateTableRequest request =
                CreateTableRequest.builder().tableName(TEST_TABLE_NAME).build();
        final CreateTableResponse expectedResponse = CreateTableResponse.builder()
                .tableDescription(
                        TableDescription.builder().tableName(TEST_TABLE_NAME).build())
                .build();
        when(mockAsyncClient.createTable(request)).thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final CreateTableResponse actualResponse = adapter.createTable(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).createTable(request);
    }

    @Test
    public void testDescribeTable() {
        final DescribeTableRequest request =
                DescribeTableRequest.builder().tableName(TEST_TABLE_NAME).build();
        final DescribeTableResponse expectedResponse = DescribeTableResponse.builder()
                .table(TableDescription.builder().tableName(TEST_TABLE_NAME).build())
                .build();
        when(mockAsyncClient.describeTable(request)).thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final DescribeTableResponse actualResponse = adapter.describeTable(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).describeTable(request);
    }

    @Test
    public void testDeleteTable() {
        final DeleteTableRequest request =
                DeleteTableRequest.builder().tableName(TEST_TABLE_NAME).build();
        final DeleteTableResponse expectedResponse = DeleteTableResponse.builder()
                .tableDescription(
                        TableDescription.builder().tableName(TEST_TABLE_NAME).build())
                .build();
        when(mockAsyncClient.deleteTable(request)).thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final DeleteTableResponse actualResponse = adapter.deleteTable(request);

        assertEquals(expectedResponse, actualResponse);
        verify(mockAsyncClient).deleteTable(request);
    }

    @Test
    public void testException() {
        final GetItemRequest request = GetItemRequest.builder()
                .tableName(TEST_TABLE_NAME)
                .key(new HashMap<String, AttributeValue>() {
                    {
                        put("key", AttributeValue.fromS("anyKey"));
                    }
                })
                .build();
        final ProvisionedThroughputExceededException exception = ProvisionedThroughputExceededException.builder()
                .message("Test exception")
                .build();
        when(mockAsyncClient.getItem(request)).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw exception;
        }));

        try {
            adapter.getItem(request);
            fail("Expected RuntimeException");
        } catch (final ProvisionedThroughputExceededException e) {
            assertEquals(exception, e);
        }
        verify(mockAsyncClient).getItem(request);
    }
}
