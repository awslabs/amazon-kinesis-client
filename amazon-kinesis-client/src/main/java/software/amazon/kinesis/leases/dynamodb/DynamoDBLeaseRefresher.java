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

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexUpdate;
import software.amazon.awssdk.services.dynamodb.model.IndexStatus;
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.Tag;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableResponse;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.DynamoUtils;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.UpdateField;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.AWSExceptionManager;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer.CHECKPOINT_OWNER;
import static software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer.LEASE_KEY_KEY;
import static software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer.LEASE_OWNER_KEY;

/**
 * An implementation of {@link LeaseRefresher} that uses DynamoDB.
 */
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLeaseRefresher implements LeaseRefresher {
    static final String LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME = "LeaseOwnerToLeaseKeyIndex";

    protected final String table;
    protected final DynamoDbAsyncClient dynamoDBClient;
    protected final LeaseSerializer serializer;
    protected final boolean consistentReads;
    private final TableCreatorCallback tableCreatorCallback;

    private final Duration dynamoDbRequestTimeout;
    private final DdbTableConfig ddbTableConfig;
    private final boolean leaseTableDeletionProtectionEnabled;
    private final boolean leaseTablePitrEnabled;
    private final Collection<Tag> tags;

    private boolean newTableCreated = false;

    private static final String STREAM_NAME = "streamName";
    private static final String DDB_STREAM_NAME = ":streamName";

    private static final String DDB_LEASE_OWNER = ":" + LEASE_OWNER_KEY;

    private static final String LEASE_OWNER_INDEX_QUERY_CONDITIONAL_EXPRESSION =
            String.format("%s = %s", LEASE_OWNER_KEY, DDB_LEASE_OWNER);

    private static DdbTableConfig createDdbTableConfigFromBillingMode(final BillingMode billingMode) {
        final DdbTableConfig tableConfig = new DdbTableConfig();
        tableConfig.billingMode(billingMode);
        return tableConfig;
    }

    /**
     * Constructor.
     * @param table
     * @param dynamoDBClient
     * @param serializer
     * @param consistentReads
     * @param tableCreatorCallback
     * @param dynamoDbRequestTimeout
     * @param ddbTableConfig
     * @param leaseTableDeletionProtectionEnabled
     * @param leaseTablePitrEnabled
     * @param tags
     */
    public DynamoDBLeaseRefresher(
            final String table,
            final DynamoDbAsyncClient dynamoDBClient,
            final LeaseSerializer serializer,
            final boolean consistentReads,
            @NonNull final TableCreatorCallback tableCreatorCallback,
            Duration dynamoDbRequestTimeout,
            final DdbTableConfig ddbTableConfig,
            final boolean leaseTableDeletionProtectionEnabled,
            final boolean leaseTablePitrEnabled,
            final Collection<Tag> tags) {
        this.table = table;
        this.dynamoDBClient = dynamoDBClient;
        this.serializer = serializer;
        this.consistentReads = consistentReads;
        this.tableCreatorCallback = tableCreatorCallback;
        this.dynamoDbRequestTimeout = dynamoDbRequestTimeout;
        this.ddbTableConfig = ddbTableConfig;
        this.leaseTableDeletionProtectionEnabled = leaseTableDeletionProtectionEnabled;
        this.leaseTablePitrEnabled = leaseTablePitrEnabled;
        this.tags = tags;
    }

    /**
     * {@inheritDoc}
     * This method always creates table in PROVISIONED mode and with RCU and WCU provided as method args
     */
    @Override
    public boolean createLeaseTableIfNotExists(@NonNull final Long readCapacity, @NonNull final Long writeCapacity)
            throws ProvisionedThroughputException, DependencyException {

        final DdbTableConfig overriddenTableConfig = createDdbTableConfigFromBillingMode(BillingMode.PROVISIONED);
        overriddenTableConfig.readCapacity(readCapacity);
        overriddenTableConfig.writeCapacity(writeCapacity);
        final CreateTableRequest.Builder builder = createTableRequestBuilder(overriddenTableConfig);
        return createTableIfNotExists(builder.build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createLeaseTableIfNotExists() throws ProvisionedThroughputException, DependencyException {
        final CreateTableRequest request =
                createTableRequestBuilder(ddbTableConfig).build();

        boolean tableExists = createTableIfNotExists(request);
        if (leaseTablePitrEnabled) {
            enablePitr();
            log.info("Enabled PITR on table {}", table);
        }
        return tableExists;
    }

    private void enablePitr() throws DependencyException {
        final UpdateContinuousBackupsRequest request = UpdateContinuousBackupsRequest.builder()
                .tableName(table)
                .pointInTimeRecoverySpecification(builder -> builder.pointInTimeRecoveryEnabled(true))
                .build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);
        exceptionManager.add(ProvisionedThroughputExceededException.class, t -> t);

        try {
            FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateContinuousBackups(request), dynamoDbRequestTimeout);
        } catch (ExecutionException e) {
            throw exceptionManager.apply(e.getCause());
        } catch (InterruptedException | DynamoDbException | TimeoutException e) {
            throw new DependencyException(e);
        }
    }

    private boolean createTableIfNotExists(CreateTableRequest request)
            throws ProvisionedThroughputException, DependencyException {
        try {
            if (describeLeaseTable() != null) {
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
        final DescribeTableResponse response = describeLeaseTable();
        return nonNull(response) ? response.table().tableStatus() : null;
    }

    private DescribeTableResponse describeLeaseTable() throws DependencyException {
        final DescribeTableRequest request =
                DescribeTableRequest.builder().tableName(table).build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);

        DescribeTableResponse result;
        try {
            try {
                result = FutureUtils.resolveOrCancelFuture(
                        dynamoDBClient.describeTable(request), dynamoDbRequestTimeout);
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

        return result;
    }

    @Override
    public boolean waitUntilLeaseTableExists(long secondsBetweenPolls, long timeoutSeconds) throws DependencyException {
        long sleepTimeRemaining = TimeUnit.SECONDS.toMillis(timeoutSeconds);

        while (!leaseTableExists()) {
            if (sleepTimeRemaining <= 0) {
                return false;
            }
            log.info("Waiting for Lease table creation...");

            long timeToSleepMillis = Math.min(TimeUnit.SECONDS.toMillis(secondsBetweenPolls), sleepTimeRemaining);

            sleepTimeRemaining -= sleep(timeToSleepMillis);
        }

        if (newTableCreated) {
            log.debug("Lease table was recently created, will perform post table creation actions");
            performPostTableCreationAction();
        }

        return true;
    }

    private static boolean isTableInPayPerRequestMode(final DescribeTableResponse describeTableResponse) {
        if (nonNull(describeTableResponse)
                && nonNull(describeTableResponse.table().billingModeSummary())
                && describeTableResponse
                        .table()
                        .billingModeSummary()
                        .billingMode()
                        .equals(BillingMode.PAY_PER_REQUEST)) {
            return true;
        }
        return false;
    }

    @Override
    public String createLeaseOwnerToLeaseKeyIndexIfNotExists() throws DependencyException {
        final DescribeTableResponse describeTableResponse = describeLeaseTable();
        ProvisionedThroughput provisionedThroughput = null;
        if (nonNull(describeTableResponse)) {
            // If table already on PAY_PER_REQUEST then setting null provisionedThroughput creates the GSI in
            // PAY_PER_REQUEST mode
            if (!isTableInPayPerRequestMode(describeTableResponse)) {
                /*
                 * Whatever is configured at the base table use that as WCU and RCU for the GSI. If this is new
                 * application created with provision mode, the set WCU and RCU will be same as that of what application
                 * provided, if this is old application where application provided WCU and RCU is no longer what is set
                 * on base table then we honor the capacity of base table. This is to avoid setting WCU and RCU very
                 * less on GSI and cause issues with base table. Customers are expected to tune in GSI WCU and RCU
                 * themselves after creation as they deem fit.
                 */
                provisionedThroughput = ProvisionedThroughput.builder()
                        .readCapacityUnits(describeTableResponse
                                .table()
                                .provisionedThroughput()
                                .readCapacityUnits())
                        .writeCapacityUnits(describeTableResponse
                                .table()
                                .provisionedThroughput()
                                .writeCapacityUnits())
                        .build();
            }

            final IndexStatus indexStatus = getIndexStatusFromDescribeTableResponse(
                    describeTableResponse.table(), LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME);
            if (nonNull(indexStatus)) {
                log.info(
                        "Lease table GSI {} already exists with status {}",
                        LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME,
                        indexStatus);

                // indexStatus is nonNull that means index already exists, return the status of index.
                return indexStatus.toString();
            }
        }
        final UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
                .tableName(table)
                .attributeDefinitions(serializer.getWorkerIdToLeaseKeyIndexAttributeDefinitions())
                .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                        .create(CreateGlobalSecondaryIndexAction.builder()
                                .indexName(LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME)
                                .keySchema(serializer.getWorkerIdToLeaseKeyIndexKeySchema())
                                .projection(Projection.builder()
                                        .projectionType(ProjectionType.KEYS_ONLY)
                                        .build())
                                .provisionedThroughput(provisionedThroughput)
                                .build())
                        .build())
                .build();

        try {
            log.info("Creating Lease table GSI {}", LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME);
            final UpdateTableResponse response = FutureUtils.resolveOrCancelFuture(
                    dynamoDBClient.updateTable(updateTableRequest), dynamoDbRequestTimeout);
            return getIndexStatusFromDescribeTableResponse(
                            response.tableDescription(), LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME)
                    .toString();
        } catch (ExecutionException e) {
            throw new DependencyException(nonNull(e.getCause()) ? e.getCause() : e);
        } catch (InterruptedException | TimeoutException e) {
            throw new DependencyException(e);
        }
    }

    private IndexStatus getIndexStatusFromDescribeTableResponse(
            final TableDescription tableDescription, final String indexName) {
        if (isNull(tableDescription)) {
            return null;
        }
        return tableDescription.globalSecondaryIndexes().stream()
                .filter(index -> index.indexName().equals(indexName))
                .findFirst()
                .map(GlobalSecondaryIndexDescription::indexStatus)
                .orElse(null);
    }

    @Override
    public boolean waitUntilLeaseOwnerToLeaseKeyIndexExists(final long secondsBetweenPolls, final long timeoutSeconds) {
        final long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime
                < Duration.ofSeconds(timeoutSeconds).toMillis()) {
            try {
                if (isLeaseOwnerToLeaseKeyIndexActive()) {
                    return true;
                }
            } catch (final Exception e) {
                log.warn("Failed to fetch {} status", LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME, e);
            }
            try {
                log.info("GSI status is not active, trying again in {}s", secondsBetweenPolls);
                Thread.sleep(Duration.ofSeconds(secondsBetweenPolls).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("GSI status was not active, after {}s", timeoutSeconds);
        return false;
    }

    @Override
    public boolean isLeaseOwnerToLeaseKeyIndexActive() throws DependencyException {
        final DescribeTableResponse describeTableResponse = describeLeaseTable();
        if (isNull(describeTableResponse)) {
            return false;
        }
        final IndexStatus indexStatus = getIndexStatusFromDescribeTableResponse(
                describeTableResponse.table(), LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME);
        log.debug(
                "Lease table GSI {} status {}",
                LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME,
                indexStatus == null ? "does not exist" : indexStatus);

        return indexStatus == IndexStatus.ACTIVE;
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
    public List<Lease> listLeasesForStream(StreamIdentifier streamIdentifier)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return list(null, streamIdentifier);
    }

    /**
     * {@inheritDoc}
     *
     * This method throws InvalidStateException in case of
     * {@link DynamoDBLeaseRefresher#LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME} does not exists.
     * If index creation is not done and want to listLeases for a worker,
     * use {@link DynamoDBLeaseRefresher#listLeases()} and filter on that to list leases.
     */
    @Override
    public List<String> listLeaseKeysForWorker(final String workerIdentifier)
            throws DependencyException, InvalidStateException {
        QueryRequest queryRequest = QueryRequest.builder()
                .indexName(LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME)
                .keyConditionExpression(LEASE_OWNER_INDEX_QUERY_CONDITIONAL_EXPRESSION)
                .expressionAttributeValues(ImmutableMap.of(
                        DDB_LEASE_OWNER,
                        AttributeValue.builder().s(workerIdentifier).build()))
                .tableName(table)
                .build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);

        try {
            try {
                final List<String> result = new ArrayList<>();

                QueryResponse queryResponse =
                        FutureUtils.resolveOrCancelFuture(dynamoDBClient.query(queryRequest), dynamoDbRequestTimeout);

                while (queryResponse != null) {
                    for (Map<String, AttributeValue> item : queryResponse.items()) {
                        result.add(item.get(LEASE_KEY_KEY).s());
                    }
                    final Map<String, AttributeValue> lastEvaluatedKey = queryResponse.lastEvaluatedKey();
                    if (CollectionUtils.isNullOrEmpty(lastEvaluatedKey)) {
                        // Signify that we're done.
                        queryResponse = null;
                    } else {
                        // Make another request, picking up where we left off.
                        queryRequest = queryRequest.toBuilder()
                                .exclusiveStartKey(lastEvaluatedKey)
                                .build();
                        queryResponse = FutureUtils.resolveOrCancelFuture(
                                dynamoDBClient.query(queryRequest), dynamoDbRequestTimeout);
                    }
                }
                return result;
            } catch (final ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            }
        } catch (final ResourceNotFoundException e) {
            throw new InvalidStateException(LEASE_OWNER_TO_LEASE_KEY_INDEX_NAME + " does not exists.", e);
        } catch (final Exception e) {
            throw new DependencyException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Lease> listLeases() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return list(null, null);
    }

    @Override
    public Map.Entry<List<Lease>, List<String>> listLeasesParallely(
            final ExecutorService parallelScanExecutorService, final int parallelScanTotalSegment)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final List<String> leaseItemFailedDeserialize = new ArrayList<>();
        final List<Lease> response = new ArrayList<>();
        final List<Future<List<Map<String, AttributeValue>>>> futures = new ArrayList<>();
        for (int i = 0; i < parallelScanTotalSegment; ++i) {
            final int segmentNumber = i;
            futures.add(parallelScanExecutorService.submit(() -> scanSegment(segmentNumber, parallelScanTotalSegment)));
        }
        try {
            for (final Future<List<Map<String, AttributeValue>>> future : futures) {
                for (final Map<String, AttributeValue> item : future.get()) {
                    try {
                        response.add(serializer.fromDynamoRecord(item));
                    } catch (final Exception e) {
                        // If one or more leases failed to deserialize for some reason (e.g. corrupted lease etc
                        // do not fail all list call. Capture failed deserialize item and return to caller.
                        log.error("Failed to deserialize lease", e);
                        // If a item exists in DDB then "leaseKey" should be always present as its primaryKey
                        leaseItemFailedDeserialize.add(item.get(LEASE_KEY_KEY).s());
                    }
                }
            }
        } catch (final ExecutionException e) {
            final Throwable throwable = e.getCause() != null ? e.getCause() : e;
            if (throwable instanceof ResourceNotFoundException) {
                throw new InvalidStateException("Cannot scan lease table " + table + " because it does not exist.", e);
            } else if (throwable instanceof ProvisionedThroughputException) {
                throw new ProvisionedThroughputException(e);
            } else {
                throw new DependencyException(e);
            }
        } catch (final InterruptedException e) {
            throw new DependencyException(e);
        }
        return new AbstractMap.SimpleEntry<>(response, leaseItemFailedDeserialize);
    }

    private List<Map<String, AttributeValue>> scanSegment(final int segment, final int parallelScanTotalSegment)
            throws DependencyException {

        final List<Map<String, AttributeValue>> response = new ArrayList<>();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);
        exceptionManager.add(ProvisionedThroughputExceededException.class, t -> t);

        Map<String, AttributeValue> lastEvaluatedKey = null;
        do {
            try {
                final ScanRequest scanRequest = ScanRequest.builder()
                        .tableName(table)
                        .segment(segment)
                        .totalSegments(parallelScanTotalSegment)
                        .exclusiveStartKey(lastEvaluatedKey)
                        .build();

                final ScanResponse scanResult =
                        FutureUtils.resolveOrCancelFuture(dynamoDBClient.scan(scanRequest), dynamoDbRequestTimeout);
                response.addAll(scanResult.items());
                if (scanResult.hasLastEvaluatedKey()) {
                    lastEvaluatedKey = scanResult.lastEvaluatedKey();
                } else {
                    // null signifies that the scan is complete for this segment.
                    lastEvaluatedKey = null;
                }
            } catch (final ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (final InterruptedException | TimeoutException e) {
                throw new DependencyException(e);
            }
        } while (lastEvaluatedKey != null);

        return response;
    }

    /**
     * {@inheritDoc}
     * Current implementation has a fixed 10 parallism
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
    private List<Lease> list(Integer limit, Integer maxPages, StreamIdentifier streamIdentifier)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Listing leases from table {}", table);

        ScanRequest.Builder scanRequestBuilder = ScanRequest.builder().tableName(table);

        if (streamIdentifier != null) {
            final Map<String, AttributeValue> expressionAttributeValues = ImmutableMap.of(
                    DDB_STREAM_NAME,
                    AttributeValue.builder().s(streamIdentifier.serialize()).build());
            scanRequestBuilder = scanRequestBuilder
                    .filterExpression(STREAM_NAME + " = " + DDB_STREAM_NAME)
                    .expressionAttributeValues(expressionAttributeValues);
        }

        if (limit != null) {
            scanRequestBuilder = scanRequestBuilder.limit(limit);
        }
        ScanRequest scanRequest = scanRequestBuilder.build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);
        exceptionManager.add(ProvisionedThroughputExceededException.class, t -> t);

        try {
            try {
                ScanResponse scanResult =
                        FutureUtils.resolveOrCancelFuture(dynamoDBClient.scan(scanRequest), dynamoDbRequestTimeout);
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
                        scanRequest = scanRequest.toBuilder()
                                .exclusiveStartKey(lastEvaluatedKey)
                                .build();
                        log.debug("lastEvaluatedKey was {}, continuing scan.", lastEvaluatedKey);
                        scanResult = FutureUtils.resolveOrCancelFuture(
                                dynamoDBClient.scan(scanRequest), dynamoDbRequestTimeout);
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

        PutItemRequest request = PutItemRequest.builder()
                .tableName(table)
                .item(serializer.toDynamoRecord(lease))
                .expected(serializer.getDynamoNonexistantExpectation())
                .build();

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

        GetItemRequest request = GetItemRequest.builder()
                .tableName(table)
                .key(serializer.getDynamoHashKey(leaseKey))
                .consistentRead(consistentReads)
                .build();
        final AWSExceptionManager exceptionManager = createExceptionManager();
        try {
            try {
                GetItemResponse result =
                        FutureUtils.resolveOrCancelFuture(dynamoDBClient.getItem(request), dynamoDbRequestTimeout);

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

        final Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.putAll(serializer.getDynamoLeaseCounterUpdate(lease));
        if (nonNull(lease.throughputKBps())) {
            attributeUpdates.putAll(serializer.getDynamoLeaseThroughputKbpsUpdate(lease));
        }
        final Map<String, ExpectedAttributeValue> expected = serializer.getDynamoLeaseCounterExpectation(lease);

        // In steady-state execution, the lease is not expected to contain shutdown attributes. If a shutdown
        // is requested, a conditional update failure is triggered. When this happens, we examine the returned
        // lease to determine if the failure resulted from a shutdown request. If so, we update the shutdown
        // attributes in the in-memory lease and retry the renewal without the expectedValue,
        // allowing it to complete successfully
        if (!lease.shutdownRequested()) {
            expected.put(
                    CHECKPOINT_OWNER,
                    ExpectedAttributeValue.builder().exists(false).build());
        }

        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(table)
                .key(serializer.getDynamoHashKey(lease))
                .expected(expected)
                .attributeUpdates(attributeUpdates)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ConditionalCheckFailedException.class, t -> t);

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
            // run this code block only if the in-memory lease doesn't have the shutdown attributes
            if (!lease.shutdownRequested()) {
                final Lease ddbLease;
                if (!e.hasItem()) {
                    // This is a workaround for unit testing and ddblocal since it doesn't return the item
                    // in the error response. Can remove it once the functionality is supported in ddblocal.
                    ddbLease = getLease(lease.leaseKey());
                } else {
                    ddbLease = serializer.fromDynamoRecord(e.item());
                }
                if (ddbLease != null && ddbLease.shutdownRequested()) {
                    return handleGracefulShutdown(lease, ddbLease);
                }
            }
            log.debug(
                    "Lease renewal failed for lease with key {} because the lease counter was not {}",
                    lease.leaseKey(),
                    lease.leaseCounter());
            // If we had a spurious retry during the Dynamo update, then this conditional PUT failure
            // might be incorrect. So, we get the item straight away and check if the lease owner + lease
            // counter are what we expected.
            // We need to use actualOwner because leaseOwner might have been updated to the nextOwner
            // in the previous renewal.
            final String expectedOwner = lease.actualOwner();
            Long expectedCounter = lease.leaseCounter() + 1;
            final Lease updatedLease = getLease(lease.leaseKey());
            if (updatedLease == null
                    || !expectedOwner.equals(updatedLease.leaseOwner())
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

        log.debug(
                "Taking lease with leaseKey {} from {} to {}",
                lease.leaseKey(),
                lease.leaseOwner() == null ? "nobody" : lease.leaseOwner(),
                owner);

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ConditionalCheckFailedException.class, t -> t);

        Map<String, AttributeValueUpdate> updates = serializer.getDynamoLeaseCounterUpdate(lease);
        updates.putAll(serializer.getDynamoTakeLeaseUpdate(lease, owner));

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(table)
                .key(serializer.getDynamoHashKey(lease))
                .expected(serializer.getDynamoLeaseCounterExpectation(lease))
                .attributeUpdates(updates)
                .build();

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
            log.debug(
                    "Lease renewal failed for lease with key {} because the lease counter was not {}",
                    lease.leaseKey(),
                    lease.leaseCounter());
            return false;
        } catch (DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("take", lease.leaseKey(), e);
        }

        lease.leaseCounter(lease.leaseCounter() + 1);
        lease.leaseOwner(owner);
        clearPendingShutdownAttributes(lease);
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
    public boolean initiateGracefulLeaseHandoff(final Lease lease, final String newOwner)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final String oldOwner = lease.leaseOwner();

        log.debug(
                "Initiating graceful lease handoff for leaseKey {} from {} to {}",
                lease.leaseKey(),
                oldOwner,
                newOwner);

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ConditionalCheckFailedException.class, t -> t);

        final Map<String, AttributeValueUpdate> updates = new HashMap<>();
        final Map<String, ExpectedAttributeValue> expectedAttributeValueMap = new HashMap<>();
        // The update doesn't increment the leaseCounter because this can avoid interrupting the lease renewal of the
        // current owner. This is safe because the graceful handoff is being initiated without competing for
        // lease ownership or affecting the lease's existing state such as checkpoints. Additionally, once the lease
        // enters the pendingCheckpoint state, the only remaining state change will be the reassignment,
        // which causes the current owner to relinquish ownership so there will be no rewriting of pendingCheckpoint
        // if there are concurrent LAM assignments somehow.
        updates.put(
                LEASE_OWNER_KEY,
                AttributeValueUpdate.builder()
                        .value(DynamoUtils.createAttributeValue(newOwner))
                        .action(AttributeAction.PUT)
                        .build());
        updates.put(
                CHECKPOINT_OWNER,
                AttributeValueUpdate.builder()
                        .value(DynamoUtils.createAttributeValue(lease.leaseOwner()))
                        .action(AttributeAction.PUT)
                        .build());

        // The conditional checks ensure that the lease is not pending shutdown,
        // so it should have the leaseOwner field, but not the checkpointOwner field.
        expectedAttributeValueMap.put(
                LEASE_OWNER_KEY,
                ExpectedAttributeValue.builder()
                        .value(DynamoUtils.createAttributeValue(lease.leaseOwner()))
                        .build());
        expectedAttributeValueMap.put(
                CHECKPOINT_OWNER, ExpectedAttributeValue.builder().exists(false).build());
        // see assignLease()
        expectedAttributeValueMap.putAll(serializer.getDynamoExistentExpectation(lease.leaseKey()));

        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(table)
                .key(serializer.getDynamoHashKey(lease))
                .expected(expectedAttributeValueMap)
                .attributeUpdates(updates)
                .returnValues(ReturnValue.ALL_NEW)
                .build();

        UpdateItemResponse response = null;
        try {
            try {
                response =
                        FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                throw new DependencyException(e);
            }
        } catch (final ConditionalCheckFailedException e) {
            log.debug(
                    "Initiate graceful lease handoff failed for lease with key {} because the lease owner was not {}"
                            + " or the checkpoint owner was not empty or lease doesn't exist anymore",
                    lease.leaseKey(),
                    lease.leaseOwner());
            return false;
        } catch (final DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("initiate_lease_handoff", lease.leaseKey(), e);
        }

        final Lease updatedLease = serializer.fromDynamoRecord(response.attributes());
        lease.leaseCounter(updatedLease.leaseCounter());
        lease.leaseOwner(updatedLease.leaseOwner());
        lease.checkpointOwner(updatedLease.checkpointOwner());
        lease.ownerSwitchesSinceCheckpoint(updatedLease.ownerSwitchesSinceCheckpoint());

        log.info("Initiated graceful lease handoff for lease {} from {} to {}.", lease.leaseKey(), oldOwner, newOwner);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean assignLease(final Lease lease, final String newOwner)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final String oldOwner = lease.leaseOwner();
        final String checkpointOwner = lease.checkpointOwner() == null ? "nobody" : lease.checkpointOwner();
        log.debug(
                "Assigning lease with leaseKey {} from {} to {} with checkpoint owner {}",
                lease.leaseKey(),
                lease.leaseOwner() == null ? "nobody" : lease.leaseOwner(),
                newOwner,
                checkpointOwner);

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ConditionalCheckFailedException.class, t -> t);

        // Performs the PUT action on leaseOwner and ADD action on the leaseCounter
        // Updating leaseCounter will cause the existing owner to lose the lease.
        // This also clears checkpointOwner attribute to trigger an immediate assignment.
        final Map<String, AttributeValueUpdate> updates = serializer.getDynamoAssignLeaseUpdate(lease, newOwner);

        // Assignment should only happen when leaseOwner match and lease still exists. Lease exists check is required
        // because in case of no leaseOwner, the conditional check no leaseOwner exists is met
        // in case when lease does not exist as well so lease exists check validates that the lease is not deleted
        // during assignLease call.
        final Map<String, ExpectedAttributeValue> expectedAttributeValueMap =
                serializer.getDynamoLeaseOwnerExpectation(lease);

        // Make sure that the lease is always present and not deleted between read and update of assignLease call
        // and when the owner is null on lease as conditional check on owner wont come into play.
        expectedAttributeValueMap.putAll(serializer.getDynamoExistentExpectation(lease.leaseKey()));

        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(table)
                .key(serializer.getDynamoHashKey(lease))
                .expected(expectedAttributeValueMap)
                .attributeUpdates(updates)
                .returnValues(ReturnValue.ALL_NEW)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build();

        UpdateItemResponse response = null;
        try {
            try {
                response =
                        FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                throw new DependencyException(e);
            }
        } catch (final ConditionalCheckFailedException e) {
            String failedCheckpointOwner = "nobody";
            if (e.hasItem()) {
                failedCheckpointOwner = serializer.fromDynamoRecord(e.item()).checkpointOwner();
            }
            log.debug(
                    "Assign lease failed for lease with key {} because the lease owner was not {} or the checkpoint"
                            + " owner was not {} but was {}",
                    lease.leaseKey(),
                    lease.leaseOwner(),
                    checkpointOwner,
                    failedCheckpointOwner);
            return false;
        } catch (final DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("assign", lease.leaseKey(), e);
        }

        final Lease updatedLease = serializer.fromDynamoRecord(response.attributes());
        lease.leaseCounter(updatedLease.leaseCounter());
        lease.leaseOwner(updatedLease.leaseOwner());
        lease.ownerSwitchesSinceCheckpoint(updatedLease.ownerSwitchesSinceCheckpoint());
        clearPendingShutdownAttributes(lease);
        log.info(
                "Assigned lease {} ownership from {} to {} with checkpoint owner {}",
                lease.leaseKey(),
                oldOwner,
                newOwner,
                checkpointOwner);

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

        // AttributeValueUpdate:
        //  - remove either the leaseOwner or the checkpointOwner
        //  - increment leaseCounter
        final Map<String, AttributeValueUpdate> updates = serializer.getDynamoEvictLeaseUpdate(lease);

        // ExpectedAttributeValue:
        // This is similar to the condition we use in assignLease where we do conditional check on the owner fields
        // and ensure lease still exists. This should ensure we are less likely to run into conditional check failure
        // as the leaseCounter is frequently updated in other parts of the process.
        // - ensure owner fields match
        // - ensure lease still exists to ensure we don't end up creating malformed lease
        final Map<String, ExpectedAttributeValue> expectedAttributeValueMap =
                serializer.getDynamoLeaseOwnerExpectation(lease);
        expectedAttributeValueMap.putAll(serializer.getDynamoExistentExpectation(lease.leaseKey()));

        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(table)
                .key(serializer.getDynamoHashKey(lease))
                .expected(expectedAttributeValueMap)
                .attributeUpdates(updates)
                .returnValues(ReturnValue.ALL_NEW)
                .build();

        UpdateItemResponse response = null;
        try {
            try {
                response =
                        FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: check behavior
                throw new DependencyException(e);
            }
        } catch (ConditionalCheckFailedException e) {
            log.debug(
                    "Lease eviction failed for lease with key {} because the lease owner was not {}",
                    lease.leaseKey(),
                    lease.leaseOwner());
            return false;
        } catch (DynamoDbException | TimeoutException e) {
            throw convertAndRethrowExceptions("evict", lease.leaseKey(), e);
        }

        final Lease updatedLease = serializer.fromDynamoRecord(response.attributes());
        lease.leaseCounter(updatedLease.leaseCounter());
        lease.leaseOwner(updatedLease.leaseOwner());
        clearPendingShutdownAttributes(lease);

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
            DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                    .tableName(table)
                    .key(serializer.getDynamoHashKey(lease))
                    .build();

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

        DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                .tableName(table)
                .key(serializer.getDynamoHashKey(lease))
                .build();

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

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(table)
                .key(serializer.getDynamoHashKey(lease))
                .expected(serializer.getDynamoLeaseCounterExpectation(lease))
                .attributeUpdates(updates)
                .build();

        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                throw new DependencyException(e);
            }
        } catch (ConditionalCheckFailedException e) {
            log.debug(
                    "Lease update failed for lease with key {} because the lease counter was not {}",
                    lease.leaseKey(),
                    lease.leaseCounter());
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
        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(table)
                .key(serializer.getDynamoHashKey(lease))
                .expected(serializer.getDynamoExistentExpectation(lease.leaseKey()))
                .attributeUpdates(updates)
                .build();
        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.updateItem(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                throw new DependencyException(e);
            }
        } catch (ConditionalCheckFailedException e) {
            log.warn(
                    "Lease update failed for lease with key {} because the lease did not exist at the time of the update",
                    lease.leaseKey(),
                    e);
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
                    String.format(
                            "Cannot %s lease with key %s because table %s does not exist.", operation, leaseKey, table),
                    e);
        } else {
            return new DependencyException(e);
        }
    }

    private CreateTableRequest.Builder createTableRequestBuilder(final DdbTableConfig tableConfig) {
        final CreateTableRequest.Builder builder = CreateTableRequest.builder()
                .tableName(table)
                .keySchema(serializer.getKeySchema())
                .attributeDefinitions(serializer.getAttributeDefinitions())
                .deletionProtectionEnabled(leaseTableDeletionProtectionEnabled)
                .tags(tags);
        if (BillingMode.PAY_PER_REQUEST.equals(tableConfig.billingMode())) {
            builder.billingMode(BillingMode.PAY_PER_REQUEST);
        } else {
            builder.billingMode(BillingMode.PROVISIONED);
            builder.provisionedThroughput(ProvisionedThroughput.builder()
                    .readCapacityUnits(tableConfig.readCapacity())
                    .writeCapacityUnits(tableConfig.writeCapacity())
                    .build());
        }
        return builder;
    }

    private AWSExceptionManager createExceptionManager() {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(DynamoDbException.class, t -> t);
        return exceptionManager;
    }

    void performPostTableCreationAction() {
        tableCreatorCallback.performAction(TableCreatorCallbackInput.builder()
                .dynamoDbClient(dynamoDBClient)
                .tableName(table)
                .build());
    }

    private boolean handleGracefulShutdown(Lease lease, Lease ddbLease)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        // Drop the lease if lease and updatedLease have different owners. This can happen if lease is taken
        // by someone else.
        if (!lease.actualOwner().equals(ddbLease.actualOwner())) {
            log.warn("Lease and updatedLease have different owners. Lease {}, updatedLease {}", lease, ddbLease);
            return false;
        }
        // This updates the checkpointOwner and leaseOwner of the authoritative lease so the
        // thread handling the lease graceful shutdown can perform the shutdown logic by checking this signal.
        lease.checkpointOwner(ddbLease.checkpointOwner());
        lease.leaseOwner(ddbLease.leaseOwner());
        log.debug(
                "Retry renewing lease with key {} as shutdown requested for leaseOwner {} and " + "checkpointOwner {}",
                lease.leaseKey(),
                lease.leaseOwner(),
                lease.checkpointOwner());
        // Retry lease renewal after updating the in-memory lease with shutdown attributes
        return renewLease(lease);
    }

    // used by takeLease, evictLease and assignLease. These methods result in change in lease ownership so these
    // attribute should be also removed.
    private static void clearPendingShutdownAttributes(Lease lease) {
        lease.checkpointOwner(null);
        lease.checkpointOwnerTimeoutTimestampMillis(null);
    }
}
