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

import com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.Tag;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.UpdateField;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.AWSExceptionManager;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * An implementation of {@link LeaseRefresher} that uses DynamoDB.
 */
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLeaseRefresher implements LeaseRefresher {

    protected final String table;
    protected final DynamoDbAsyncClient dynamoDBClient;
    protected final LeaseSerializer serializer;
    protected final boolean consistentReads;
    private final TableCreatorCallback tableCreatorCallback;

    private final Duration dynamoDbRequestTimeout;
    private final BillingMode billingMode;
    private final Collection<Tag> tags;

    private boolean newTableCreated = false;

    private static final String STREAM_NAME = "streamName";
    private static final String DDB_STREAM_NAME = ":streamName";

    /**
     * Constructor.
     *
     * <p>
     * NOTE: This constructor is deprecated and will be removed in a future release.
     * </p>
     *
     * @param table
     * @param dynamoDBClient
     * @param serializer
     * @param consistentReads
     */
    @Deprecated
    public DynamoDBLeaseRefresher(final String table, final DynamoDbAsyncClient dynamoDBClient,
            final LeaseSerializer serializer, final boolean consistentReads) {
        this(table, dynamoDBClient, serializer, consistentReads, TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK);
    }

    /**
     * Constructor.
     *
     * @param table
     * @param dynamoDBClient
     * @param serializer
     * @param consistentReads
     * @param tableCreatorCallback
     */
    @Deprecated
    public DynamoDBLeaseRefresher(final String table, final DynamoDbAsyncClient dynamoDBClient,
                                  final LeaseSerializer serializer, final boolean consistentReads,
                                  @NonNull final TableCreatorCallback tableCreatorCallback) {
        this(table, dynamoDBClient, serializer, consistentReads, tableCreatorCallback, LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT);
    }

    /**
     * Constructor.
     * @param table
     * @param dynamoDBClient
     * @param serializer
     * @param consistentReads
     * @param tableCreatorCallback
     * @param dynamoDbRequestTimeout
     */
    @Deprecated
    public DynamoDBLeaseRefresher(final String table, final DynamoDbAsyncClient dynamoDBClient,
                                  final LeaseSerializer serializer, final boolean consistentReads,
                                  @NonNull final TableCreatorCallback tableCreatorCallback, Duration dynamoDbRequestTimeout) {
        this(table, dynamoDBClient, serializer, consistentReads, tableCreatorCallback, dynamoDbRequestTimeout, BillingMode.PAY_PER_REQUEST);
    }

    /**
     * Constructor.
     * @param table
     * @param dynamoDBClient
     * @param serializer
     * @param consistentReads
     * @param tableCreatorCallback
     * @param dynamoDbRequestTimeout
     * @param billingMode
     */
    @Deprecated
    public DynamoDBLeaseRefresher(final String table, final DynamoDbAsyncClient dynamoDBClient,
                                  final LeaseSerializer serializer, final boolean consistentReads,
                                  @NonNull final TableCreatorCallback tableCreatorCallback, Duration dynamoDbRequestTimeout,
                                  final BillingMode billingMode) {
        this(table, dynamoDBClient, serializer, consistentReads, tableCreatorCallback, dynamoDbRequestTimeout,
                billingMode, DefaultSdkAutoConstructList.getInstance());
    }

    /**
     * Constructor.
     * @param table
     * @param dynamoDBClient
     * @param serializer
     * @param consistentReads
     * @param tableCreatorCallback
     * @param dynamoDbRequestTimeout
     * @param billingMode
     * @param tags
     */
    public DynamoDBLeaseRefresher(final String table, final DynamoDbAsyncClient dynamoDBClient,
                                  final LeaseSerializer serializer, final boolean consistentReads,
                                  @NonNull final TableCreatorCallback tableCreatorCallback, Duration dynamoDbRequestTimeout,
                                  final BillingMode billingMode, final Collection<Tag> tags) {
        this.table = table;
        this.dynamoDBClient = dynamoDBClient;
        this.serializer = serializer;
        this.consistentReads = consistentReads;
        this.tableCreatorCallback = tableCreatorCallback;
        this.dynamoDbRequestTimeout = dynamoDbRequestTimeout;
        this.billingMode = billingMode;
        this.tags = tags;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createLeaseTableIfNotExists(@NonNull final Long readCapacity, @NonNull final Long writeCapacity)
            throws ProvisionedThroughputException, DependencyException {
        final CreateTableRequest.Builder builder = createTableRequestBuilder();
        if (BillingMode.PROVISIONED.equals(billingMode)) {
            ProvisionedThroughput throughput = ProvisionedThroughput.builder().readCapacityUnits(readCapacity)
                .writeCapacityUnits(writeCapacity).build();
            builder.provisionedThroughput(throughput);
        }
        return createTableIfNotExists(builder.build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createLeaseTableIfNotExists()
            throws ProvisionedThroughputException, DependencyException {
        final CreateTableRequest request = createTableRequestBuilder().build();

        return createTableIfNotExists(request);
    }

    private boolean createTableIfNotExists(CreateTableRequest request)
            throws ProvisionedThroughputException, DependencyException {
        try {
            if (tableStatus() != null) {
                return newTableCreated;
            }
        } catch (DependencyException de) {
            //
            // Something went wrong with DynamoDB
            //
            log.error("Failed to get table status for {}", table, de);
        }

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceInUseException.class, t -> t);
        exceptionManager.add(LimitExceededException.class, t -> t);

        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.createTable(request), dynamoDbRequestTimeout);
                newTableCreated = true;
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                throw new DependencyException(e);
            }
        } catch (ResourceInUseException e) {
            log.info("Table {} already exists.", table);
            return newTableCreated;
        } catch (LimitExceededException e) {
            throw new ProvisionedThroughputException("Capacity exceeded when creating table " + table, e);
        } catch (DynamoDbException | TimeoutException e) {
            throw new DependencyException(e);
        }
        return newTableCreated;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean leaseTableExists() throws DependencyException {
        TableStatus tableStatus = tableStatus();
        return TableStatus.ACTIVE == tableStatus || TableStatus.UPDATING == tableStatus;
    }

    private TableStatus tableStatus() throws DependencyException {
        DescribeTableRequest request = DescribeTableRequest.builder().tableName(table).build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);

        DescribeTableResponse result;
        try {
            try {
                result = FutureUtils.resolveOrCancelFuture(dynamoDBClient.describeTable(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: Check if this is the correct behavior
                throw new DependencyException(e);
            }
        } catch (ResourceNotFoundException e) {
            log.debug("Got ResourceNotFoundException for table {} in leaseTableExists, returning false.", table);
            return null;
        } catch (DynamoDbException | TimeoutException e) {
            throw new DependencyException(e);
        }

        TableStatus tableStatus = result.table().tableStatus();
        log.debug("Lease table exists and is in status {}", tableStatus);

        return tableStatus;
    }

    @Override
    public boolean waitUntilLeaseTableExists(long secondsBetweenPolls, long timeoutSeconds) throws DependencyException {
        long sleepTimeRemaining = TimeUnit.SECONDS.toMillis(timeoutSeconds);

        while (!leaseTableExists()) {
            if (sleepTimeRemaining <= 0) {
                return false;
            }

            long timeToSleepMillis = Math.min(TimeUnit.SECONDS.toMillis(secondsBetweenPolls), sleepTimeRemaining);

            sleepTimeRemaining -= sleep(timeToSleepMillis);
        }

        if (newTableCreated) {
            log.debug("Lease table was recently created, will perform post table creation actions");
            performPostTableCreationAction();
        }

        return true;
    }

    /**
     * Exposed for testing purposes.
     *
     * @param timeToSleepMillis time to sleep in milliseconds
     *
     * @return actual time slept in millis
     */
    long sleep(long timeToSleepMillis) {
        long startTime = System.currentTimeMillis();

        try {
            Thread.sleep(timeToSleepMillis);
        } catch (InterruptedException e) {
            log.debug("Interrupted while sleeping");
        }

        return System.currentTimeMillis() - startTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Lease> listLeasesForStream(StreamIdentifier streamIdentifier) throws DependencyException,
            InvalidStateException, ProvisionedThroughputException {
        return list( null, streamIdentifier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Lease> listLeases() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return list(null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeaseTableEmpty()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return list(1, 1, null).isEmpty();
    }

    /**
     * List with the given page size. Package access for integration testing.
     *
     * @param limit number of items to consider at a time - used by integration tests to force paging.
     * @param streamIdentifier streamIdentifier for multi-stream mode. Can be null.
     * @return list of leases
     * @throws InvalidStateException if table does not exist
     * @throws DependencyException if DynamoDB scan fail in an unexpected way
     * @throws ProvisionedThroughputException if DynamoDB scan fail due to exceeded capacity
     */
    List<Lease> list(Integer limit, StreamIdentifier streamIdentifier)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return list(limit, Integer.MAX_VALUE, streamIdentifier);
    }

    /**
     * List with the given page size. Package access for integration testing.
     *
     * @param limit number of items to consider at a time - used by integration tests to force paging.
     * @param maxPages mad paginated scan calls
     * @param streamIdentifier streamIdentifier for multi-stream mode. Can be null.
     * @return list of leases
     * @throws InvalidStateException if table does not exist
     * @throws DependencyException if DynamoDB scan fail in an unexpected way
     * @throws ProvisionedThroughputException if DynamoDB scan fail due to exceeded capacity
     */
    private List<Lease> list(Integer limit, Integer maxPages, StreamIdentifier streamIdentifier) throws DependencyException, InvalidStateException,
            ProvisionedThroughputException {

        log.debug("Listing leases from table {}", table);

        ScanRequest.Builder scanRequestBuilder = ScanRequest.builder().tableName(table);

        if (streamIdentifier != null) {
            final Map<String, AttributeValue> expressionAttributeValues = ImmutableMap.of(
                 DDB_STREAM_NAME, AttributeValue.builder().s(streamIdentifier.serialize()).build()
            );
            scanRequestBuilder = scanRequestBuilder.filterExpression(STREAM_NAME + " = " + DDB_STREAM_NAME)
                    .expressionAttributeValues(expressionAttributeValues);
        }

        if (limit != null) {
            scanRequestBuilder = scanRequestBuilder.limit(limit);
        }
        ScanRequest scanRequest = scanRequestBuilder.build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t ->  t);
        exceptionManager.add(ProvisionedThroughputExceededException.class, t -> t);

        try {
            try {
                ScanResponse scanResult = FutureUtils.resolveOrCancelFuture(dynamoDBClient.scan(scanRequest), dynamoDbRequestTimeout);
                List<Lease> result = new ArrayList<>();

                while (scanResult != null) {
                    for (Map<String, AttributeValue> item : scanResult.items()) {
                        log.debug("Got item {} from DynamoDB.", item.toString());
                        result.add(serializer.fromDynamoRecord(item));
                    }

                    Map<String, AttributeValue> lastEvaluatedKey = scanResult.lastEvaluatedKey();
                    if (CollectionUtils.isNullOrEmpty(lastEvaluatedKey) || --maxPages <= 0) {
                        // Signify that we're done.
                        scanResult = null;
                        log.debug("lastEvaluatedKey was null - scan finished.");
                    } else {
                        // Make another request, picking up where we left off.
                        scanRequest = scanRequest.toBuilder().exclusiveStartKey(lastEvaluatedKey).build();
                        log.debug("lastEvaluatedKey was {}, continuing scan.", lastEvaluatedKey);
                        scanResult = FutureUtils.resolveOrCancelFuture(dynamoDBClient.scan(scanRequest), dynamoDbRequestTimeout);
                    }
                }
                log.debug("Listed {} leases from table {}", result.size(), table);
                return result;
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: Check if this is the correct behavior
                throw new DependencyException(e);
            }
        } catch (ResourceNotFoundException e) {
            throw new InvalidStateException("Cannot scan lease table " + table + " because it does not exist.", e);
        } catch (ProvisionedThroughputExceededException e) {
            throw new ProvisionedThroughputException(e);
        } catch (DynamoDbException | TimeoutException e) {
            throw new DependencyException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createLeaseIfNotExists(@NonNull final Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Creating lease: {}", lease);

        PutItemRequest request = PutItemRequest.builder().tableName(table).item(serializer.toDynamoRecord(lease))
                .expected(serializer.getDynamoNonexistantExpectation()).build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ConditionalCheckFailedException.class, t -> t);

        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.putItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: Check if this is the correct behavior
                throw new DependencyException(e);
            }
        } catch (ConditionalCheckFailedException e) {
            log.debug("Did not create lease {} because it already existed", lease);
            return false;
        } catch (DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("create", lease.leaseKey(), e);
        }
        log.info("Created lease: {}", lease);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Lease getLease(@NonNull final String leaseKey)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Getting lease with key {}", leaseKey);

        GetItemRequest request = GetItemRequest.builder().tableName(table).key(serializer.getDynamoHashKey(leaseKey))
                .consistentRead(consistentReads).build();
        final AWSExceptionManager exceptionManager = createExceptionManager();
        try {
            try {
                GetItemResponse result = FutureUtils.resolveOrCancelFuture(dynamoDBClient.getItem(request), dynamoDbRequestTimeout);

                Map<String, AttributeValue> dynamoRecord = result.item();
                if (CollectionUtils.isNullOrEmpty(dynamoRecord)) {
                    log.debug("No lease found with key {}, returning null.", leaseKey);
                    return null;
                } else {
                    final Lease lease = serializer.fromDynamoRecord(dynamoRecord);
                    log.debug("Retrieved lease: {}", lease);
                    return lease;
                }
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: check behavior
                throw new DependencyException(e);
            }
        } catch (DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("get", leaseKey, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean renewLease(@NonNull final Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Renewing lease with key {}", lease.leaseKey());

        UpdateItemRequest request = UpdateItemRequest.builder().tableName(table).key(serializer.getDynamoHashKey(lease))
                .expected(serializer.getDynamoLeaseCounterExpectation(lease))
                .attributeUpdates(serializer.getDynamoLeaseCounterUpdate(lease)).build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ConditionalCheckFailedException.class, t ->  t);

        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: Check if this is correct behavior
                throw new DependencyException(e);
            }
        } catch (ConditionalCheckFailedException e) {
            log.debug("Lease renewal failed for lease with key {} because the lease counter was not {}",
                    lease.leaseKey(), lease.leaseCounter());

            // If we had a spurious retry during the Dynamo update, then this conditional PUT failure
            // might be incorrect. So, we get the item straight away and check if the lease owner + lease
            // counter are what we expected.
            String expectedOwner = lease.leaseOwner();
            Long expectedCounter = lease.leaseCounter() + 1;
            final Lease updatedLease = getLease(lease.leaseKey());
            if (updatedLease == null || !expectedOwner.equals(updatedLease.leaseOwner())
                    || !expectedCounter.equals(updatedLease.leaseCounter())) {
                return false;
            }

            log.info("Detected spurious renewal failure for lease with key {}, but recovered", lease.leaseKey());
        } catch (DynamoDbException | TimeoutException e) {
            throw new DependencyException(e);
        }

        lease.leaseCounter(lease.leaseCounter() + 1);
        log.debug("Renewed lease with key {}", lease.leaseKey());
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean takeLease(@NonNull final Lease lease, @NonNull final String owner)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final String oldOwner = lease.leaseOwner();

        log.debug("Taking lease with leaseKey {} from {} to {}", lease.leaseKey(),
                lease.leaseOwner() == null ? "nobody" : lease.leaseOwner(), owner);

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ConditionalCheckFailedException.class, t -> t);

        Map<String, AttributeValueUpdate> updates = serializer.getDynamoLeaseCounterUpdate(lease);
        updates.putAll(serializer.getDynamoTakeLeaseUpdate(lease, owner));

        UpdateItemRequest request = UpdateItemRequest.builder().tableName(table).key(serializer.getDynamoHashKey(lease))
                .expected(serializer.getDynamoLeaseCounterExpectation(lease)).attributeUpdates(updates).build();

        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: Check behavior
                throw new DependencyException(e);
            }
        } catch (ConditionalCheckFailedException e) {
            log.debug("Lease renewal failed for lease with key {} because the lease counter was not {}",
                    lease.leaseKey(), lease.leaseCounter());
            return false;
        } catch (DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("take", lease.leaseKey(), e);
        }

        lease.leaseCounter(lease.leaseCounter() + 1);
        lease.leaseOwner(owner);

        if (oldOwner != null && !oldOwner.equals(owner)) {
            lease.ownerSwitchesSinceCheckpoint(lease.ownerSwitchesSinceCheckpoint() + 1);
        }

        log.info("Transferred lease {} ownership from {} to {}", lease.leaseKey(), oldOwner, owner);

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean evictLease(@NonNull final Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Evicting lease with leaseKey {} owned by {}", lease.leaseKey(), lease.leaseOwner());

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ConditionalCheckFailedException.class, t -> t);

        Map<String, AttributeValueUpdate> updates = serializer.getDynamoLeaseCounterUpdate(lease);
        updates.putAll(serializer.getDynamoEvictLeaseUpdate(lease));
        UpdateItemRequest request = UpdateItemRequest.builder().tableName(table).key(serializer.getDynamoHashKey(lease))
                .expected(serializer.getDynamoLeaseOwnerExpectation(lease)).attributeUpdates(updates).build();

        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: check behavior
                throw new DependencyException(e);
            }
        } catch (ConditionalCheckFailedException e) {
            log.debug("Lease eviction failed for lease with key {} because the lease owner was not {}",
                    lease.leaseKey(), lease.leaseOwner());
            return false;
        } catch (DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("evict", lease.leaseKey(), e);
        }

        lease.leaseOwner(null);
        lease.leaseCounter(lease.leaseCounter() + 1);

        log.info("Evicted lease with leaseKey {}", lease.leaseKey());
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public void deleteAll() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        List<Lease> allLeases = listLeases();

        log.warn("Deleting {} items from table {}", allLeases.size(), table);

        final AWSExceptionManager exceptionManager = createExceptionManager();
        for (final Lease lease : allLeases) {
            DeleteItemRequest deleteRequest = DeleteItemRequest.builder().tableName(table)
                    .key(serializer.getDynamoHashKey(lease)).build();

            try {
                try {
                    FutureUtils.resolveOrCancelFuture(dynamoDBClient.deleteItem(deleteRequest), dynamoDbRequestTimeout);
                } catch (ExecutionException e) {
                    throw exceptionManager.apply(e.getCause());
                } catch (InterruptedException e) {
                    // TODO: check the behavior
                    throw new DependencyException(e);
                }
            } catch (DynamoDbException | TimeoutException e) {
                throw convertAndRethrowExceptions("deleteAll", lease.leaseKey(), e);
            }
            log.debug("Deleted lease {} from table {}", lease.leaseKey(), table);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteLease(@NonNull final Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Deleting lease with leaseKey {}", lease.leaseKey());

        DeleteItemRequest deleteRequest = DeleteItemRequest.builder().tableName(table)
                .key(serializer.getDynamoHashKey(lease)).build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.deleteItem(deleteRequest), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: Check if this is the correct behavior
                throw new DependencyException(e);
            }
        } catch (DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("delete", lease.leaseKey(), e);
        }

        log.info("Deleted lease with leaseKey {}", lease.leaseKey());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updateLease(@NonNull final Lease lease)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Updating lease: {}", lease);

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ConditionalCheckFailedException.class, t -> t);

        Map<String, AttributeValueUpdate> updates = serializer.getDynamoLeaseCounterUpdate(lease);
        updates.putAll(serializer.getDynamoUpdateLeaseUpdate(lease));

        UpdateItemRequest request = UpdateItemRequest.builder().tableName(table).key(serializer.getDynamoHashKey(lease))
                .expected(serializer.getDynamoLeaseCounterExpectation(lease)).attributeUpdates(updates).build();

        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                throw new DependencyException(e);
            }
        } catch (ConditionalCheckFailedException e) {
            log.debug("Lease update failed for lease with key {} because the lease counter was not {}",
                    lease.leaseKey(), lease.leaseCounter());
            return false;
        } catch (DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("update", lease.leaseKey(), e);
        }

        lease.leaseCounter(lease.leaseCounter() + 1);
        log.info("Updated lease {}.", lease.leaseKey());
        return true;
    }

    @Override
    public void updateLeaseWithMetaInfo(Lease lease, UpdateField updateField)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Updating lease without expectation {}", lease);
        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ConditionalCheckFailedException.class, t -> t);
        Map<String, AttributeValueUpdate> updates = serializer.getDynamoUpdateLeaseUpdate(lease, updateField);
        UpdateItemRequest request = UpdateItemRequest.builder().tableName(table).key(serializer.getDynamoHashKey(lease))
                .expected(serializer.getDynamoExistentExpectation(lease.leaseKey()))
                .attributeUpdates(updates).build();
        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                throw new DependencyException(e);
            }
        } catch (ConditionalCheckFailedException e) {
            log.warn("Lease update failed for lease with key {} because the lease did not exist at the time of the update",
                    lease.leaseKey(), e);
        } catch (DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("update", lease.leaseKey(), e);
        }

        log.info("Updated lease without expectation {}.", lease);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExtendedSequenceNumber getCheckpoint(String leaseKey)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        ExtendedSequenceNumber checkpoint = null;
        Lease lease = getLease(leaseKey);
        if (lease != null) {
            checkpoint = lease.checkpoint();
        }
        return checkpoint;
    }

    /*
     * This method contains boilerplate exception handling - it throws or returns something to be thrown. The
     * inconsistency there exists to satisfy the compiler when this method is used at the end of non-void methods.
     */
    protected DependencyException convertAndRethrowExceptions(String operation, String leaseKey, Exception e)
            throws ProvisionedThroughputException, InvalidStateException {
        if (e instanceof ProvisionedThroughputExceededException) {
            log.warn("Provisioned Throughput on the lease table has been exceeded. It's recommended that you increase"
                    + " the IOPs on the table. Failure to increase the IOPs may cause the application to not make"
                    + " progress.");
            throw new ProvisionedThroughputException(e);
        } else if (e instanceof ResourceNotFoundException) {
            throw new InvalidStateException(
                    String.format("Cannot %s lease with key %s because table %s does not exist.",
                            operation, leaseKey, table),
                    e);
        } else {
            return new DependencyException(e);
        }
    }

    private CreateTableRequest.Builder createTableRequestBuilder() {
        final CreateTableRequest.Builder builder = CreateTableRequest.builder().tableName(table).keySchema(serializer.getKeySchema())
                        .attributeDefinitions(serializer.getAttributeDefinitions())
                        .tags(tags);
        if (BillingMode.PAY_PER_REQUEST.equals(billingMode)) {
            builder.billingMode(billingMode);
        }
        return builder;
    }

    private AWSExceptionManager createExceptionManager() {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(DynamoDbException.class, t -> t);
        return exceptionManager;
    }

    void performPostTableCreationAction() {
        tableCreatorCallback.performAction(
                TableCreatorCallbackInput.builder().dynamoDbClient(dynamoDBClient).tableName(table).build());
    }
}
