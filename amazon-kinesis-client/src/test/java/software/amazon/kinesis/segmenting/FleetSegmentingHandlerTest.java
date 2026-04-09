package software.amazon.kinesis.segmenting;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.LeaseAssignmentMetric;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FleetSegmentingHandlerTest {

    private DynamoDbClient mockDdbClient;
    private FleetSegmentingHandler handler;
    private String expectedVersionHash;
    private LeaseManagementConfig config;

    @BeforeEach
    void setup() {
        mockDdbClient = mock(DynamoDbClient.class);
        expectedVersionHash = String.valueOf(LeaseAssignmentMetric.CPU.name().hashCode());
        config = new ConfigsBuilder(
                        "StreamName",
                        "AppName",
                        Mockito.mock(KinesisAsyncClient.class),
                        Mockito.mock(DynamoDbAsyncClient.class),
                        Mockito.mock(CloudWatchAsyncClient.class),
                        "dummyWorkerIdentifier",
                        Mockito.mock(ShardRecordProcessorFactory.class))
                .leaseManagementConfig();
        handler = new FleetSegmentingHandler(config, mockDdbClient, "dummyTableName");
    }

    @Test
    void getVersionHash_returnsDeterministicValue() {
        String expected = String.valueOf("CPU".hashCode());
        assertEquals(expected, handler.getVersionHash());
    }

    @Test
    void getVersionHash_isSameAcrossInstances() {
        FleetSegmentingHandler handler2 = new FleetSegmentingHandler(config, mockDdbClient, "dummyTableName");
        assertEquals(handler.getVersionHash(), handler2.getVersionHash());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenVersionHashMatches() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("versionHash", AttributeValue.fromS(expectedVersionHash));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertEquals(CoordinatorState.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashKeyMissing() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("someOtherKey", AttributeValue.fromS("value"));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertEquals(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsDeployingLeaderKey_whenVersionHashDoesNotMatch() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("versionHash", AttributeValue.fromS("differentHash"));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertEquals(CoordinatorState.DEPLOYING_LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void getHashKeyForLeaderLock_returnsLeaderKey_whenItemNotPresent() {
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().build());

        assertEquals(CoordinatorState.LEADER_HASH_KEY, handler.getHashKeyForLeaderLock());
    }

    @Test
    void isWorkerOnDeployingVersion_returnsTrue_whenVersionHashMatches() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("versionHash", AttributeValue.fromS(expectedVersionHash));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertTrue(handler.isWorkerOnDeployingVersion());
    }

    @Test
    void isWorkerOnDeployingVersion_returnsFalse_whenVersionHashDoesNotMatch() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("versionHash", AttributeValue.fromS("differentHash"));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertFalse(handler.isWorkerOnDeployingVersion());
    }

    @Test
    void isWorkerOnDeployingVersion_returnsFalse_whenItemNotPresent() {
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().build());

        assertFalse(handler.isWorkerOnDeployingVersion());
    }

    @Test
    void isWorkerOnDeployingVersion_returnsFalse_whenVersionHashKeyMissing() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("someOtherKey", AttributeValue.fromS("value"));
        when(mockDdbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        assertFalse(handler.isWorkerOnDeployingVersion());
    }
}
