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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
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
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Tag;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;

import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

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
    private DescribeTableRequest describeTableRequest;
    private CreateTableRequest createTableRequest;
    private Collection<Tag> tags;

    private Map<String, AttributeValue> serializedLease;

    @Before
    public void setup() throws Exception {
        leaseRefresher = new DynamoDBLeaseRefresher(TABLE_NAME, dynamoDbClient, leaseSerializer, CONSISTENT_READS,
                tableCreatorCallback);
        serializedLease = new HashMap<>();

        describeTableRequest = DescribeTableRequest.builder().tableName(TABLE_NAME).build();
        createTableRequest = CreateTableRequest.builder()
                .tableName(TABLE_NAME)
                .keySchema(leaseSerializer.getKeySchema())
                .attributeDefinitions(leaseSerializer.getAttributeDefinitions())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();
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
    public void testWaitUntilLeaseTableExistsUpdatingStatus() throws Exception {
        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(anyLong(), any()))
                .thenReturn(DescribeTableResponse.builder()
                        .table(TableDescription.builder().tableStatus(TableStatus.UPDATING).build())
                        .build());
        assertTrue(leaseRefresher.waitUntilLeaseTableExists(0, 0));
    }

    @Test
    public void testWaitUntilLeaseTableExistsActiveStatus() throws Exception {
        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(anyLong(), any()))
                .thenReturn(DescribeTableResponse.builder()
                        .table(TableDescription.builder().tableStatus(TableStatus.ACTIVE).build())
                        .build());
        assertTrue(leaseRefresher.waitUntilLeaseTableExists(0, 0));
    }

    @Test
    public void testWaitUntilLeaseTableExistsCreatingStatus() throws Exception {
        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(anyLong(), any()))
                .thenReturn(DescribeTableResponse.builder()
                        .table(TableDescription.builder().tableStatus(TableStatus.CREATING).build())
                        .build());
        assertFalse(leaseRefresher.waitUntilLeaseTableExists(0, 0));
    }

    @Test
    public void testWaitUntilLeaseTableExistsDeletingStatus() throws Exception {
        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(anyLong(), any()))
                .thenReturn(DescribeTableResponse.builder()
                        .table(TableDescription.builder().tableStatus(TableStatus.DELETING).build())
                        .build());
        assertFalse(leaseRefresher.waitUntilLeaseTableExists(0, 0));
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
    public void testCreateLeaseTableProvisionedBillingModeIfNotExists() throws Exception {
        leaseRefresher = new DynamoDBLeaseRefresher(TABLE_NAME, dynamoDbClient, leaseSerializer, CONSISTENT_READS,
                tableCreatorCallback, LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT, BillingMode.PROVISIONED);

        when(dynamoDbClient.describeTable(describeTableRequest)).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());

        final ProvisionedThroughput throughput = ProvisionedThroughput.builder().readCapacityUnits(10L)
                .writeCapacityUnits(10L).build();
        final CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .tableName(TABLE_NAME)
                .keySchema(leaseSerializer.getKeySchema())
                .attributeDefinitions(leaseSerializer.getAttributeDefinitions())
                .provisionedThroughput(throughput)
                .build();
        when(dynamoDbClient.createTable(createTableRequest)).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(null);

        final boolean result = leaseRefresher.createLeaseTableIfNotExists(10L, 10L);

        verify(dynamoDbClient, times(1)).describeTable(describeTableRequest);
        verify(dynamoDbClient, times(1)).createTable(createTableRequest);
        verify(mockDescribeTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
        verify(mockCreateTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
        Assert.assertTrue(result);
    }

    @Test
    public void testCreateLeaseTableWithTagsIfNotExists() throws Exception {
        tags = Collections.singletonList(Tag.builder().key("foo").value("bar").build());
        leaseRefresher = new DynamoDBLeaseRefresher(TABLE_NAME, dynamoDbClient, leaseSerializer, CONSISTENT_READS,
                tableCreatorCallback, LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT, BillingMode.PROVISIONED, tags);

        when(dynamoDbClient.describeTable(describeTableRequest)).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());

        final ProvisionedThroughput throughput = ProvisionedThroughput.builder().readCapacityUnits(10L)
                .writeCapacityUnits(10L).build();
        final CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .tableName(TABLE_NAME)
                .keySchema(leaseSerializer.getKeySchema())
                .attributeDefinitions(leaseSerializer.getAttributeDefinitions())
                .provisionedThroughput(throughput)
                .tags(tags)
                .build();
        when(dynamoDbClient.createTable(createTableRequest)).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .thenReturn(null);

        final boolean result = leaseRefresher.createLeaseTableIfNotExists(10L, 10L);

        verify(dynamoDbClient, times(1)).describeTable(describeTableRequest);
        verify(dynamoDbClient, times(1)).createTable(createTableRequest);
        verify(mockDescribeTableFuture, times(1))
                .get(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        verify(mockCreateTableFuture, times(1))
                .get(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertTrue(result);
    }

    @Test
    public void testCreateLeaseTableIfNotExists() throws Exception {
        when(dynamoDbClient.describeTable(describeTableRequest)).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());
        when(dynamoDbClient.createTable(createTableRequest)).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(null);

        final boolean result = leaseRefresher.createLeaseTableIfNotExists();

        verify(dynamoDbClient, times(1)).describeTable(describeTableRequest);
        verify(dynamoDbClient, times(1)).createTable(createTableRequest);
        verify(mockDescribeTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
        verify(mockCreateTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
        Assert.assertTrue(result);
    }

    @Test
    public void testCreateLeaseTableIfNotExists_throwsDependencyException() throws Exception {
        when(dynamoDbClient.describeTable(describeTableRequest)).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new InterruptedException());
        when(dynamoDbClient.createTable(createTableRequest)).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(ResourceInUseException.builder().message("Table already exists").build());

        Assert.assertFalse(leaseRefresher.createLeaseTableIfNotExists());
        verify(dynamoDbClient, times(1)).describeTable(describeTableRequest);
        verify(dynamoDbClient, times(1)).createTable(createTableRequest);
        verify(mockDescribeTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
        verify(mockCreateTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCreateLeaseTableIfNotExists_tableAlreadyExists_throwsResourceInUseException_expectFalse() throws Exception {
        when(dynamoDbClient.describeTable(describeTableRequest)).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());
        when(dynamoDbClient.createTable(createTableRequest)).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(ResourceInUseException.builder().message("Table already exists").build());

        Assert.assertFalse(leaseRefresher.createLeaseTableIfNotExists());
        verify(dynamoDbClient, times(1)).describeTable(describeTableRequest);
        verify(dynamoDbClient, times(1)).createTable(createTableRequest);
        verify(mockDescribeTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
        verify(mockCreateTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCreateLeaseTableIfNotExists_throwsLimitExceededException_expectProvisionedThroughputException() throws Exception {
        when(dynamoDbClient.describeTable(describeTableRequest)).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());
        when(dynamoDbClient.createTable(createTableRequest)).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(LimitExceededException.builder().build());

        Assert.assertThrows(ProvisionedThroughputException.class, () -> leaseRefresher.createLeaseTableIfNotExists());
        verify(dynamoDbClient, times(1)).describeTable(describeTableRequest);
        verify(dynamoDbClient, times(1)).createTable(createTableRequest);
        verify(mockDescribeTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
        verify(mockCreateTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCreateLeaseTableIfNotExists_throwsDynamoDbException_expectDependencyException() throws Exception {
        when(dynamoDbClient.describeTable(describeTableRequest)).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());
        when(dynamoDbClient.createTable(createTableRequest)).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(DynamoDbException.builder().build());

        Assert.assertThrows(DependencyException.class, () -> leaseRefresher.createLeaseTableIfNotExists());
        verify(dynamoDbClient, times(1)).describeTable(describeTableRequest);
        verify(dynamoDbClient, times(1)).createTable(createTableRequest);
        verify(mockDescribeTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
        verify(mockCreateTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCreateLeaseTableIfNotExists_throwsTimeoutException_expectDependencyException() throws Exception {
        when(dynamoDbClient.describeTable(describeTableRequest)).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());
        when(dynamoDbClient.createTable(createTableRequest)).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new TimeoutException());

        Assert.assertThrows(DependencyException.class, () -> leaseRefresher.createLeaseTableIfNotExists());
        verify(dynamoDbClient, times(1)).describeTable(describeTableRequest);
        verify(dynamoDbClient, times(1)).createTable(createTableRequest);
        verify(mockDescribeTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
        verify(mockCreateTableFuture, times(1))
                .get(eq(LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCreateLeaseTableProvisionedBillingModeTimesOut() throws Exception {
        leaseRefresher = new DynamoDBLeaseRefresher(TABLE_NAME, dynamoDbClient, leaseSerializer, CONSISTENT_READS,
                tableCreatorCallback, LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT, BillingMode.PROVISIONED);
        TimeoutException te = setRuleForDependencyTimeout();

        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(anyLong(), any()))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());

        when(dynamoDbClient.createTable(any(CreateTableRequest.class))).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(anyLong(), any())).thenThrow(te);

        verifyCancel(mockCreateTableFuture, () -> leaseRefresher.createLeaseTableIfNotExists(10L, 10L));
    }

    @Test
    public void testCreateLeaseTableTimesOut() throws Exception {
        TimeoutException te = setRuleForDependencyTimeout();

        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class))).thenReturn(mockDescribeTableFuture);
        when(mockDescribeTableFuture.get(anyLong(), any()))
                .thenThrow(ResourceNotFoundException.builder().message("Table doesn't exist").build());

        when(dynamoDbClient.createTable(any(CreateTableRequest.class))).thenReturn(mockCreateTableFuture);
        when(mockCreateTableFuture.get(anyLong(), any())).thenThrow(te);

        verifyCancel(mockCreateTableFuture, () -> leaseRefresher.createLeaseTableIfNotExists());
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