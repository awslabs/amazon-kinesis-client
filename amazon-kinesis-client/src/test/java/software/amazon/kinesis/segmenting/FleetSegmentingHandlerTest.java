package software.amazon.kinesis.segmenting;

import java.util.Collections;
import java.util.Optional;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.LeaseAssignmentMetric;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FleetSegmentingHandlerTest {

    private AmazonDynamoDBLockClient mockLockClient;
    private LockItem mockLockItem;
    private FleetSegmentingHandler handler;
    private String expectedVersionHash;

    private LeaseManagementConfig config;

    @BeforeEach
    void setup() {
        mockLockClient = mock(AmazonDynamoDBLockClient.class);
        mockLockItem = mock(LockItem.class);
        expectedVersionHash = String.valueOf(LeaseAssignmentMetric.CPU.name().hashCode());
        config = getTestConfigsBuilder().leaseManagementConfig();
        handler = new FleetSegmentingHandler(config);
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenVersionHashMatches() {
        when(mockLockClient.getLock(CoordinatorState.LEADER_HASH_KEY, Optional.empty()))
                .thenReturn(Optional.of(mockLockItem));
        when(mockLockItem.getAdditionalAttributes())
                .thenReturn(Collections.singletonMap("versionHash", AttributeValue.fromS(expectedVersionHash)));

        assertEquals(CoordinatorState.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock(mockLockClient));
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashKeyMissing() {
        when(mockLockClient.getLock(CoordinatorState.LEADER_HASH_KEY, Optional.empty()))
                .thenReturn(Optional.of(mockLockItem));
        when(mockLockItem.getAdditionalAttributes()).thenReturn(Collections.emptyMap());

        assertEquals(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock(mockLockClient));
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashDoesNotMatch() {
        when(mockLockClient.getLock(CoordinatorState.LEADER_HASH_KEY, Optional.empty()))
                .thenReturn(Optional.of(mockLockItem));
        when(mockLockItem.getAdditionalAttributes())
                .thenReturn(Collections.singletonMap("versionHash", AttributeValue.fromS("differentHash")));

        assertEquals(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock(mockLockClient));
    }

    @Test
    void getVersionHash_returnsHashOfMetricName() {
        // TODO: unit test that confirms that the version hash is not dependent on location of memory
        assertEquals(expectedVersionHash, handler.getVersionHash());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenLockNotPresent() {
        when(mockLockClient.getLock(CoordinatorState.LEADER_HASH_KEY, Optional.empty()))
                .thenReturn(Optional.empty());

        assertEquals(CoordinatorState.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock(mockLockClient));
    }

    private ConfigsBuilder getTestConfigsBuilder() {
        return new ConfigsBuilder(
                "StreamName",
                "AppName",
                Mockito.mock(KinesisAsyncClient.class),
                Mockito.mock(DynamoDbAsyncClient.class),
                Mockito.mock(CloudWatchAsyncClient.class),
                "dummyWorkerIdentifier",
                Mockito.mock(ShardRecordProcessorFactory.class));
    }
}
