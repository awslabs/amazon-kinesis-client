package software.amazon.kinesis.coordinator.delegate;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The request shape of updateAttributesIfMatch, and that a failed condition returns the item DynamoDB sends back.
 * DynamoDB Local doesn't return the item on a failed condition, so this uses a mocked client.
 */
class CoordinatorStateDAODelegateUpdateAttributesIfMatchTest {

    private final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
    private final CoordinatorStateDAODelegate delegate =
            new LeaseTableCoordinatorStateDAODelegate(client, "table", Duration.ofSeconds(1));

    private final Map<String, AttributeValue> updates =
            Collections.singletonMap("heartbeat", AttributeValue.fromN("2"));
    private final Map<String, AttributeValue> expected =
            Collections.singletonMap("ownerName", AttributeValue.fromS("worker-1"));

    @Test
    void updateAttributesIfMatch_requestsOldItemOnConditionFailure() throws Exception {
        when(client.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        UpdateItemResponse.builder().build()));

        assertFalse(delegate.updateAttributesIfMatch("LeaderVersion", updates, expected)
                .isPresent());

        final ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(captor.capture());
        final UpdateItemRequest request = captor.getValue();
        assertEquals("SET #u0 = :u0", request.updateExpression());
        assertEquals("attribute_exists(#pk) AND #e0 = :e0", request.conditionExpression());
        assertEquals("heartbeat", request.expressionAttributeNames().get("#u0"));
        assertEquals("ownerName", request.expressionAttributeNames().get("#e0"));
        assertEquals("worker-1", request.expressionAttributeValues().get(":e0").s());
        assertEquals(ReturnValuesOnConditionCheckFailure.ALL_OLD, request.returnValuesOnConditionCheckFailure());
    }

    @Test
    void updateAttributesIfMatch_conditionFailed_returnsOldItem() throws Exception {
        final Map<String, AttributeValue> oldItem = Collections.singletonMap("ownerName", AttributeValue.fromS("w2"));
        final CompletableFuture<UpdateItemResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(
                ConditionalCheckFailedException.builder().item(oldItem).build());
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(failed);

        final Optional<Map<String, AttributeValue>> result =
                delegate.updateAttributesIfMatch("LeaderVersion", updates, expected);

        assertTrue(result.isPresent());
        assertEquals("w2", result.get().get("ownerName").s());
    }

    @Test
    void updateAttributesIfMatch_itemMissing_returnsEmptyItem() throws Exception {
        final CompletableFuture<UpdateItemResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(ConditionalCheckFailedException.builder().build());
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(failed);

        final Optional<Map<String, AttributeValue>> result =
                delegate.updateAttributesIfMatch("LeaderVersion", updates, expected);

        assertTrue(result.isPresent());
        assertTrue(result.get().isEmpty());
    }
}
