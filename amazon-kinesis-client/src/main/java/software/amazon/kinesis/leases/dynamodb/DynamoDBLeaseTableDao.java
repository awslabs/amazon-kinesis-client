/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.coordinator.delegate.CoordinatorStateDAODelegate;
import software.amazon.kinesis.leases.EntityDAO;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

/**
 * DynamoDB implementation of {@link EntityDAO} that performs a parallel scan of the lease table
 * and groups all entities by their {@link EntityType}.
 *
 * <p>Delegates deserialization to the appropriate existing DAO:
 * <ul>
 *   <li>Lease entities → {@link LeaseSerializer#fromDynamoRecord(Map)}</li>
 *   <li>CoordinatorState entities → {@link CoordinatorStateDAODelegate#fromDynamoRecord(Map)}</li>
 *   <li>WorkerMetricStats entities → DynamoDB Enhanced Client {@link TableSchema}</li>
 * </ul>
 *
 * <p>Records without an entityType attribute are treated as {@link EntityType#LEASE}.</p>
 *
 * <p>Uses a parallel scan pattern similar to
 * {@code DynamoDBLeaseRefresher.listLeasesParallely} for throughput.</p>
 */
@Slf4j
@KinesisClientInternalApi
public class DynamoDBLeaseTableDao implements EntityDAO {

    private static final String ENTITY_TYPE_ATTRIBUTE_NAME = "entityType";

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;
    private final LeaseSerializer leaseSerializer;
    private final CoordinatorStateDAODelegate coordinatorStateDAODelegate;
    private final TableSchema<WorkerMetricStats.LeaseTableWorkerMetricStats> workerMetricStatsSchema;
    private final ExecutorService executorService;
    private final int totalSegments;

    public DynamoDBLeaseTableDao(
            final DynamoDbAsyncClient dynamoDbAsyncClient,
            final String tableName,
            final LeaseSerializer leaseSerializer,
            final CoordinatorStateDAODelegate coordinatorStateDAODelegate,
            final ExecutorService executorService,
            final int totalSegments) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.tableName = tableName;
        this.leaseSerializer = leaseSerializer;
        this.coordinatorStateDAODelegate = coordinatorStateDAODelegate;
        this.workerMetricStatsSchema = TableSchema.fromBean(WorkerMetricStats.LeaseTableWorkerMetricStats.class);
        this.executorService = executorService;
        this.totalSegments = totalSegments;
    }

    @Override
    public Map<EntityType, EntityScanList> scanAllEntities()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        log.debug("Scanning all entities from lease table {} with {} segments", tableName, totalSegments);

        // Initialize EntityScanList per type with mutable ArrayLists.
        // EntityScanList is @Value but the underlying List references are mutable ArrayLists,
        // allowing concurrent appends under the synchronized block in scanSegment.
        final Map<EntityType, EntityScanList> result = new EnumMap<>(EntityType.class);
        for (final EntityType type : EntityType.values()) {
            result.put(
                    type,
                    EntityScanList.builder()
                            .entities(new ArrayList<>())
                            .deserializationFailures(new ArrayList<>())
                            .build());
        }

        final List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < totalSegments; i++) {
            final int segment = i;
            futures.add(executorService.submit(() -> {
                scanSegment(segment, totalSegments, result);
                return null;
            }));
        }

        try {
            for (final Future<Void> future : futures) {
                future.get();
            }
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof ResourceNotFoundException) {
                throw new InvalidStateException(
                        "Cannot scan lease table " + tableName + " because it does not exist.", e);
            } else if (cause instanceof ProvisionedThroughputException) {
                throw (ProvisionedThroughputException) cause;
            } else {
                throw new DependencyException(e);
            }
        } catch (final InterruptedException e) {
            throw new DependencyException(e);
        }

        if (log.isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder("Scan complete. Entities by type: {");
            for (final Map.Entry<EntityType, EntityScanList> entry : result.entrySet()) {
                if (!entry.getValue().getEntities().isEmpty()
                        || !entry.getValue().getDeserializationFailures().isEmpty()) {
                    sb.append(entry.getKey())
                            .append("=")
                            .append(entry.getValue().getEntities().size())
                            .append(" (failures=")
                            .append(entry.getValue()
                                    .getDeserializationFailures()
                                    .size())
                            .append("), ");
                }
            }
            sb.append("}");
            log.debug(sb.toString());
        }
        return result;
    }

    @Override
    public Map<EntityType, EntityScanList> scanEntities(final EntityType... entityTypes)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        final Set<EntityType> requested = EnumSet.copyOf(Arrays.asList(entityTypes));

        // Perform the full scan — all records are read, but only requested types are deserialized/collected
        final Map<EntityType, EntityScanList> allEntities = scanAllEntities();

        // Filter to only the requested types
        final Map<EntityType, EntityScanList> filtered = new EnumMap<>(EntityType.class);
        for (final EntityType type : requested) {
            filtered.put(
                    type,
                    allEntities.getOrDefault(type, EntityScanList.builder().build()));
        }
        return filtered;
    }

    /**
     * Scans a single segment of the parallel scan. Successfully deserialized entities are added
     * to the {@link EntityScanList#getEntities()} list; failed records have their partition key
     * added to the {@link EntityScanList#getDeserializationFailures()} list.
     *
     * <p>Thread safety: the result map's EntityScanList objects contain mutable ArrayLists.
     * All appends are guarded by {@code synchronized (result)}.</p>
     */
    private void scanSegment(final int segment, final int totalSegments, final Map<EntityType, EntityScanList> result)
            throws DependencyException, ProvisionedThroughputException {

        Map<String, AttributeValue> lastEvaluatedKey = null;
        do {
            if (Thread.currentThread().isInterrupted()) {
                log.info("Scan segment {} interrupted, stopping early", segment);
                throw new DependencyException(new InterruptedException("Scan segment " + segment + " was interrupted"));
            }
            final ScanRequest scanRequest = ScanRequest.builder()
                    .tableName(tableName)
                    .segment(segment)
                    .totalSegments(totalSegments)
                    .exclusiveStartKey(lastEvaluatedKey)
                    .build();

            try {
                final ScanResponse scanResponse =
                        FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.scan(scanRequest));

                synchronized (result) {
                    for (final Map<String, AttributeValue> record : scanResponse.items()) {
                        final EntityType entityType = resolveEntityType(record);
                        final EntityScanList scanList = result.get(entityType);
                        try {
                            final Entity entity = deserializeRecord(entityType, record);
                            if (entity != null) {
                                scanList.getEntities().add(entity);
                            }
                        } catch (final Exception e) {
                            final String key = extractPartitionKey(record);
                            log.error(
                                    "Failed to deserialize {} record with key '{}' in segment {}",
                                    entityType,
                                    key,
                                    segment,
                                    e);
                            scanList.getDeserializationFailures().add(key);
                        }
                    }
                }

                lastEvaluatedKey = scanResponse.lastEvaluatedKey();
                if (CollectionUtils.isNullOrEmpty(lastEvaluatedKey)) {
                    lastEvaluatedKey = null;
                }
            } catch (final ProvisionedThroughputExceededException e) {
                throw new ProvisionedThroughputException(e);
            } catch (final ResourceNotFoundException e) {
                throw e;
            }
        } while (lastEvaluatedKey != null);
    }

    /**
     * Extracts the partition key value from a DDB record for failure tracking.
     */
    private String extractPartitionKey(final Map<String, AttributeValue> record) {
        final AttributeValue keyAttr = record.get(DynamoDBLeaseSerializer.LEASE_KEY_KEY);
        return keyAttr != null && keyAttr.s() != null ? keyAttr.s() : "UNKNOWN";
    }

    /**
     * Resolve the EntityType from a DDB record. Records without an entityType attribute
     * default to {@link EntityType#LEASE}.
     * All entities written to Lease table should have an Entity type, but since this
     * is a new attribute
     */
    private EntityType resolveEntityType(final Map<String, AttributeValue> record) {
        final AttributeValue entityTypeAttr = record.get(ENTITY_TYPE_ATTRIBUTE_NAME);
        if (entityTypeAttr == null || entityTypeAttr.s() == null) {
            return EntityType.LEASE;
        }
        final EntityType resolved = EntityType.fromDdbValue(entityTypeAttr.s());
        return resolved != null ? resolved : EntityType.LEASE;
    }

    /**
     * Deserialize a DDB record into the appropriate Entity by delegating to the existing DAO
     * responsible for that entity type.
     */
    private Entity deserializeRecord(final EntityType entityType, final Map<String, AttributeValue> record) {
        switch (entityType) {
            case LEASE:
                // Remove entityType attribute before deserializing as a lease because
                // leaseSerializer.fromDynamoRecord rejects records with unrecognized entityType values.
                final Map<String, AttributeValue> leaseRecord = new HashMap<>(record);
                leaseRecord.remove(ENTITY_TYPE_ATTRIBUTE_NAME);
                final Lease lease = leaseSerializer.fromDynamoRecord(leaseRecord);
                return lease;
            case STREAM_INFO:
            case CLIENT_VERSION_MIGRATION:
            case TABLE_MIGRATION:
            case LEADER_LOCK:
                return coordinatorStateDAODelegate.fromDynamoRecord(record);
            case WORKER_METRIC_STATS:
                return workerMetricStatsSchema.mapToItem(record);
            default:
                log.warn("No deserializer for entityType {}. Skipping record.", entityType);
                return null;
        }
    }

    @Override
    public void shutdown() {
        if (executorService.isShutdown()) {
            return;
        }
        log.info("Shutting down EntityDAO executor service");
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                log.info("EntityDAO executor service did not terminate in 5s, forcing shutdown");
                executorService.shutdownNow();
            }
        } catch (final InterruptedException e) {
            log.warn("Interrupted while waiting for EntityDAO executor service shutdown", e);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
