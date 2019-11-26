/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.kinesis.leases.dynamodb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;

import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
@RunWith(MockitoJUnitRunner.class)
public class DynamoDBLeaseRefresherTest {

    private static final String TABLE_NAME = "test";
    private static final boolean CONSISTENT_READS = true;

    @Mock
    private DynamoDbAsyncClient dynamoDbClient;
    @Mock
    private LeaseSerializer leaseSerializer;
    @Mock
    private TableCreatorCallback tableCreatorCallback;
    @Mock
    private CompletableFuture<ScanResponse> mockScanFuture;
    @Mock
    private CompletableFuture<PutItemResponse> mockPutItemFuture;
    @Mock
    private CompletableFuture<GetItemResponse> mockGetItemFuture;
    @Mock
    private CompletableFuture<UpdateItemResponse> mockUpdateFuture;
    @Mock
    private CompletableFuture<DeleteItemResponse> mockDeleteFuture;
    @Mock
    private CompletableFuture<DescribeTableResponse> mockDescribeTableFuture;
    @Mock
    private CompletableFuture<CreateTableResponse> mockCreateTableFuture;
    @Mock
    private Lease lease;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private DynamoDBLeaseRefresher leaseRefresher;

    private Map<String, AttributeValue> serializedLease;

    @Before
    public void setup() throws Exception {
        leaseRefresher = new DynamoDBLeaseRefresher(TABLE_NAME, dynamoDbClient, leaseSerializer, CONSISTENT_READS,
                tableCreatorCallback);
        serializedLease = new HashMap<>();

    }

    @Test
    public void testListLeasesHandlesTimeout() throws Exception {
        TimeoutException te = setRuleForDependencyTimeout();

        when(mockScanFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(te);
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(mockScanFuture);

        verifyCancel(mockScanFuture, () -> leaseRefresher.listLeases());
    }

    @Test
    public void testListLeasesSucceedsThenFails() throws Exception {
        TimeoutException te = setRuleForDependencyTimeout();

        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(mockScanFuture);

        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("Test", AttributeValue.builder().s("test").build());

        when(mockScanFuture.get(anyLong(), any(TimeUnit.class)))
                .thenReturn(ScanResponse.builder().lastEvaluatedKey(lastEvaluatedKey).build())
                .thenThrow(te);

        verifyCancel(mockScanFuture, () -> leaseRefresher.listLeases());

        verify(mockScanFuture, times(2)).get(anyLong(), any(TimeUnit.class));
        verify(dynamoDbClient, times(2)).scan(any(ScanRequest.class));

    }

    @Test
    public void testCreateLeaseIfNotExistsTimesOut() throws Exception {
        TimeoutException te = setRuleForDependencyTimeout();

        when(dynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(mockPutItemFuture);
        when(mockPutItemFuture.get(anyLong(), any())).thenThrow(te);

        when(leaseSerializer.toDynamoRecord(any())).thenReturn(serializedLease);
        when(leaseSerializer.getDynamoNonexistantExpectation()).thenReturn(Collections.emptyMap());

        verifyCancel(mockPutItemFuture, () -> leaseRefresher.createLeaseIfNotExists(lease));
    }

    @Test
    public void testGetLeaseTimesOut() throws Exception {
        TimeoutException te = setRuleForDependencyTimeout();

        when(dynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(mockGetItemFuture);
        when(mockGetItemFuture.get(anyLong(), any())).thenThrow(te);

        when(leaseSerializer.getDynamoHashKey(anyString())).thenReturn(Collections.emptyMap());

        verifyCancel(mockGetItemFuture, () -> leaseRefresher.getLease("test"));
    }

    @Test
    public void testRenewLeaseTimesOut() throws Exception {
        setupUpdateItemTest();
        verifyCancel(mockUpdateFuture, () ->leaseRefresher.renewLease(lease));
    }

    @Test
    public void testTakeLeaseTimesOut() throws Exception {
        setupUpdateItemTest();
        verifyCancel(mockUpdateFuture, () -> leaseRefresher.takeLease(lease, "owner"));
    }

    @Test
    public void testEvictLeaseTimesOut() throws Exception {
        setupUpdateItemTest();
        verifyCancel(mockUpdateFuture, () -> leaseRefresher.evictLease(lease));
    }

    @Test
    public void testUpdateLeaseTimesOut() throws Exception {
        setupUpdateItemTest();
        verifyCancel(mockUpdateFuture, () -> leaseRefresher.updateLease(lease));
    }

    @Test
    public void testDeleteAllLeasesTimesOut() throws Exception {
        TimeoutException te = setRuleForDependencyTimeout();
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(mockScanFuture);
        when(mockScanFuture.get(anyLong(), any())).thenReturn(ScanResponse.builder().items(Collections.emptyMap()).build());
        when(leaseSerializer.fromDynamoRecord(any())).thenReturn(lease);
        when(leaseSerializer.getDynamoHashKey(any(Lease.class))).thenReturn(Collections.emptyMap());

        when(dynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(mockDeleteFuture);
        when(mockDeleteFuture.get(anyLong(), any())).thenThrow(te);

        verifyCancel(mockDeleteFuture, () -> leaseRefresher.deleteAll());
    }

    @Test
    public void testDeleteLeaseTimesOut() throws Exception {
        TimeoutException te = setRuleForDependencyTimeout();
        when(leaseSerializer.getDynamoHashKey(any(Lease.class))).thenReturn(Collections.emptyMap());

        when(dynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(mockDeleteFuture);
        when(mockDeleteFuture.get(anyLong(), any())).thenThrow(te);

        verifyCancel(mockDeleteFuture, () -> leaseRefresher.deleteLease(lease));
    }

    @Test
    public void testLeaseTableExistsTimesOut() throws Exception {
        TimeoutException te = setRuleForDependencyTimeout();

        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(anyLong(), any())).thenThrow(te);

        verifyCancel(mockDescribeTableFuture, () -> leaseRefresher.leaseTableExists());
    }

    @Test
    public void testCreateLeaseTableTimesOut() throws Exception {
        TimeoutException te = setRuleForDependencyTimeout();

        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(anyLong(), any()))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());

        when(dynamoDbClient.createTable(any(CreateTableRequest.class))).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(anyLong(), any())).thenThrow(te);

        verifyCancel(mockCreateTableFuture, () -> leaseRefresher.createLeaseTableIfNotExists(10L, 10L));
    }

    @Test
    public void testCreateLeaseTableBillingMode() throws Exception {
        leaseRefresher = new DynamoDBLeaseRefresher(TABLE_NAME, dynamoDbClient, leaseSerializer, CONSISTENT_READS,
                tableCreatorCallback, LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT, BillingMode.PAY_PER_REQUEST);

        TimeoutException te = setRuleForDependencyTimeout();

        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(anyLong(), any()))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());

        when(dynamoDbClient.createTable(any(CreateTableRequest.class))).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(anyLong(), any())).thenThrow(te);

        verifyCancel(mockCreateTableFuture, () -> leaseRefresher.createLeaseTableIfNotExists(10L, 10L));
    }

    @FunctionalInterface
    private interface TestCaller {
        void call() throws Exception;
    }

    private void verifyCancel(Future<?> future, TestCaller toExecute) throws Exception {
        try {
            toExecute.call();
        } finally {
            verify(future).cancel(anyBoolean());
        }
    }

    private void setupUpdateItemTest() throws Exception {
        TimeoutException te = setRuleForDependencyTimeout();

        when(leaseSerializer.getDynamoHashKey(any(Lease.class))).thenReturn(Collections.emptyMap());
        when(leaseSerializer.getDynamoLeaseCounterExpectation(any(Lease.class))).thenReturn(Collections.emptyMap());
        when(leaseSerializer.getDynamoLeaseCounterUpdate(any(Lease.class))).thenReturn(Collections.emptyMap());
        when(leaseSerializer.getDynamoTakeLeaseUpdate(any(), anyString())).thenReturn(Collections.emptyMap());

        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(mockUpdateFuture);
        when(mockUpdateFuture.get(anyLong(), any())).thenThrow(te);
    }

    private TimeoutException setRuleForDependencyTimeout() {
        TimeoutException te = new TimeoutException("Timeout");
        expectedException.expect(DependencyException.class);
        expectedException.expectCause(equalTo(te));

        return te;
    }

}