package software.amazon.kinesis.coordinator;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.kinesis.coordinator.CoordinatorConfig.CoordinatorStateTableConfig;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatus;
import software.amazon.kinesis.coordinator.migration.TableMigrationStatusProvider;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.segmenting.LeaderVersionState;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.leader.LeaderLock.LEADER_HASH_KEY;

/**
 * The fenced version write runs on the leader heartbeat thread, so it must not wait on DynamoDB forever.
 */
class CoordinatorStateDAOFencedWriteTimeoutTest {

    private final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);

    @Test
    void putCoordinatorStateIfLockOwnedBy_timesOut_whenDynamoDbNeverResponds() throws Exception {
        final CompletableFuture<TransactWriteItemsResponse> neverCompletes = new CompletableFuture<>();
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class))).thenReturn(neverCompletes);
        final CoordinatorStateDAO dao = createDAO(Duration.ofMillis(200));

        final long start = System.currentTimeMillis();
        assertThrows(
                DependencyException.class,
                () -> dao.putCoordinatorStateIfLockOwnedBy(
                        new LeaderVersionState(
                                LeaderVersionState.CURRENT_LEADER_VERSION_STATE_KEY, "hash", "worker-1", 1L),
                        LEADER_HASH_KEY,
                        "worker-1"));

        assertTrue(System.currentTimeMillis() - start < 5_000);
        assertTrue(neverCompletes.isCancelled());
    }

    private CoordinatorStateDAO createDAO(final Duration timeout) throws Exception {
        final CoordinatorStateTableConfig config = mock(CoordinatorStateTableConfig.class);
        when(config.tableName()).thenReturn("legacyTable");
        final TableMigrationStatusProvider provider = mock(TableMigrationStatusProvider.class);
        when(provider.getTableMigrationStatus()).thenReturn(TableMigrationStatus.TABLE_MIGRATION_STATUS_COMPLETE);
        final CoordinatorStateDAO dao = new CoordinatorStateDAO(client, config, "leaseTable", provider, timeout);
        dao.initialize();
        return dao;
    }
}
