package software.amazon.kinesis.coordinator.delegate;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Every coordinator-state call waits for DynamoDB for at most the DynamoDB request timeout. These calls run on the
 * leader decider, the migration monitors and the worker heartbeat, so none of them may wait forever.
 */
class CoordinatorStateDAODelegateGetTimeoutTest {

    private static final Duration TIMEOUT = Duration.ofMillis(200);
    private static final long MAX_WAIT_MILLIS = 5_000;

    private final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
    private final CoordinatorStateDAODelegate delegate =
            new LeaseTableCoordinatorStateDAODelegate(client, "table", TIMEOUT);

    private final CoordinatorState state = CoordinatorState.builder()
            .key("someKey")
            .attributes(Collections.emptyMap())
            .build();

    @Test
    void getCoordinatorState_timesOut_whenDynamoDbNeverResponds() {
        final CompletableFuture<GetItemResponse> neverCompletes = new CompletableFuture<>();
        when(client.getItem(any(GetItemRequest.class))).thenReturn(neverCompletes);

        assertTimesOut(() -> delegate.getCoordinatorState("Leader"));
        assertTrue(neverCompletes.isCancelled());
    }

    @Test
    void getCoordinatorState_stillMapsThrottling() {
        when(client.getItem(any(GetItemRequest.class)))
                .thenReturn(failed(ProvisionedThroughputExceededException.builder()
                        .message("throttled")
                        .build()));

        assertThrows(ProvisionedThroughputException.class, () -> delegate.getCoordinatorState("Leader"));
    }

    @Test
    void listCoordinatorState_timesOut_whenDynamoDbNeverResponds() {
        final CompletableFuture<ScanResponse> neverCompletes = new CompletableFuture<>();
        when(client.scan(any(ScanRequest.class))).thenReturn(neverCompletes);

        assertTimesOut(delegate::listCoordinatorState);
        assertTrue(neverCompletes.isCancelled());
    }

    @Test
    void listCoordinatorStateByEntityType_timesOut_whenDynamoDbNeverResponds() {
        final CompletableFuture<ScanResponse> neverCompletes = new CompletableFuture<>();
        when(client.scan(any(ScanRequest.class))).thenReturn(neverCompletes);

        assertTimesOut(() -> delegate.listCoordinatorStateByEntityType(EntityType.CoordinatorStateType.LEADER_LOCK));
        assertTrue(neverCompletes.isCancelled());
    }

    @Test
    void createCoordinatorStateIfNotExists_timesOut_whenDynamoDbNeverResponds() {
        final CompletableFuture<PutItemResponse> neverCompletes = new CompletableFuture<>();
        when(client.putItem(any(PutItemRequest.class))).thenReturn(neverCompletes);

        assertTimesOut(() -> delegate.createCoordinatorStateIfNotExists(state));
        assertTrue(neverCompletes.isCancelled());
    }

    @Test
    void createCoordinatorStateIfNotExists_stillReturnsFalse_whenKeyExists() throws Exception {
        when(client.putItem(any(PutItemRequest.class)))
                .thenReturn(failed(ConditionalCheckFailedException.builder()
                        .message("exists")
                        .build()));

        assertFalse(delegate.createCoordinatorStateIfNotExists(state));
    }

    @Test
    void deleteCoordinatorState_timesOut_whenDynamoDbNeverResponds() {
        final CompletableFuture<DeleteItemResponse> neverCompletes = new CompletableFuture<>();
        when(client.deleteItem(any(DeleteItemRequest.class))).thenReturn(neverCompletes);

        assertTimesOut(() -> delegate.deleteCoordinatorState("someKey"));
        assertTrue(neverCompletes.isCancelled());
    }

    @Test
    void updateCoordinatorStateWithExpectation_timesOut_whenDynamoDbNeverResponds() {
        final CompletableFuture<UpdateItemResponse> neverCompletes = new CompletableFuture<>();
        when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(neverCompletes);

        assertTimesOut(() -> delegate.updateCoordinatorStateWithExpectation(state, null));
        assertTrue(neverCompletes.isCancelled());
    }

    @Test
    void updateCoordinatorStateWithExpectation_stillReturnsFalse_whenConditionFails() throws Exception {
        when(client.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(failed(ConditionalCheckFailedException.builder()
                        .message("condition failed")
                        .build()));

        assertFalse(delegate.updateCoordinatorStateWithExpectation(state, null));
    }

    @Test
    void legacyInitialize_timesOut_whenDynamoDbNeverResponds() {
        final CompletableFuture<DescribeTableResponse> neverCompletes = new CompletableFuture<>();
        when(client.describeTable(any(DescribeTableRequest.class))).thenReturn(neverCompletes);
        final LegacyTableCoordinatorStateDAODelegate legacy =
                new LegacyTableCoordinatorStateDAODelegate(client, legacyTableConfig(), TIMEOUT);

        assertTimesOut(legacy::initialize);
        assertTrue(neverCompletes.isCancelled());
    }

    @Test
    void legacyInitialize_stillDisables_whenTableMissing() throws Exception {
        when(client.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(failed(
                        ResourceNotFoundException.builder().message("missing").build()));
        final LegacyTableCoordinatorStateDAODelegate legacy =
                new LegacyTableCoordinatorStateDAODelegate(client, legacyTableConfig(), TIMEOUT);

        legacy.initialize();

        assertFalse(legacy.isEnabled());
    }

    private static CoordinatorConfig.CoordinatorStateTableConfig legacyTableConfig() {
        final CoordinatorConfig.CoordinatorStateTableConfig config =
                mock(CoordinatorConfig.CoordinatorStateTableConfig.class);
        when(config.tableName()).thenReturn("legacyTable");
        return config;
    }

    private static <T> CompletableFuture<T> failed(final Throwable cause) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(cause);
        return future;
    }

    private static void assertTimesOut(final org.junit.jupiter.api.function.Executable call) {
        final long start = System.currentTimeMillis();
        assertThrows(DependencyException.class, call);
        assertTrue(System.currentTimeMillis() - start < MAX_WAIT_MILLIS);
    }
}
