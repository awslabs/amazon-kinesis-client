package software.amazon.kinesis.coordinator.delegate;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * getCoordinatorState runs inside the leader decider's synchronized isLeader(), so it must not wait
 * on DynamoDB forever.
 */
class CoordinatorStateDAODelegateGetTimeoutTest {

    private final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
    private final CoordinatorStateDAODelegate delegate =
            new LeaseTableCoordinatorStateDAODelegate(client, "table", Duration.ofMillis(200));

    @Test
    void getCoordinatorState_timesOut_whenDynamoDbNeverResponds() {
        final CompletableFuture<GetItemResponse> neverCompletes = new CompletableFuture<>();
        when(client.getItem(any(GetItemRequest.class))).thenReturn(neverCompletes);

        final long start = System.currentTimeMillis();
        assertThrows(DependencyException.class, () -> delegate.getCoordinatorState("Leader"));

        assertTrue(System.currentTimeMillis() - start < 5_000);
        assertTrue(neverCompletes.isCancelled());
    }

    @Test
    void getCoordinatorState_stillMapsThrottling() {
        final CompletableFuture<GetItemResponse> throttled = new CompletableFuture<>();
        throttled.completeExceptionally(ProvisionedThroughputExceededException.builder()
                .message("throttled")
                .build());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(throttled);

        assertThrows(ProvisionedThroughputException.class, () -> delegate.getCoordinatorState("Leader"));
    }
}
