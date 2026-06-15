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
package software.amazon.kinesis.coordinator.delegate;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.EntityType;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

/**
 * Delegate for the legacy (separate) CoordinatorState DDB table.
 *
 * <p>On {@link #initialize()}, checks if the legacy table exists. If it does not exist,
 * the delegate is marked as not enabled and all read operations become no-ops
 * (returning null or empty collections). Write operations on a non-enabled delegate
 * will throw {@link InvalidStateException}.</p>
 *
 * <p>The legacy table should only be deleted after the table migration status moves to
 * COMPLETE, at which point no read/write calls should be routed to this delegate.
 * However, if the table existed at initialization but was deleted later (e.g., by the
 * migration tool after COMPLETE), the underlying DDB calls will throw
 * {@link ResourceNotFoundException} which surfaces as a {@link DependencyException}
 * to the caller.</p>
 */
@Slf4j
@KinesisClientInternalApi
public class LegacyTableCoordinatorStateDAODelegate extends CoordinatorStateDAODelegate {

    /**
     * Key value for the item in the CoordinatorState table
     */
    public static final String COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME = "key";

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;
    private volatile boolean enabled = true;

    public LegacyTableCoordinatorStateDAODelegate(
            final DynamoDbAsyncClient dynamoDbAsyncClient, final CoordinatorConfig.CoordinatorStateTableConfig config) {
        super(dynamoDbAsyncClient, config.tableName(), COORDINATOR_STATE_TABLE_HASH_KEY_ATTRIBUTE_NAME);
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.tableName = config.tableName();
    }

    /**
     * Check if the legacy table exists. If it does not, mark this delegate as not enabled
     * so all read operations return null/empty without making DDB calls.
     *
     * @throws DependencyException if unable to determine table existence due to unexpected errors
     */
    @Override
    public void initialize() throws DependencyException {
        final DescribeTableRequest request =
                DescribeTableRequest.builder().tableName(tableName).build();
        try {
            FutureUtils.unwrappingFuture(() -> dynamoDbAsyncClient.describeTable(request));
            log.info("Legacy coordinator state table {} exists, delegate enabled", tableName);
            enabled = true;
        } catch (final ResourceNotFoundException e) {
            log.info("Legacy coordinator state table {} does not exist, delegate not enabled", tableName);
            enabled = false;
        } catch (final Exception e) {
            throw new DependencyException("Unable to determine if legacy table " + tableName + " exists", e);
        }
    }

    /**
     * @return true if the legacy table exists and this delegate is enabled for operations
     */
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public CoordinatorState getCoordinatorState(final String key)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (!enabled) {
            return null;
        }
        return super.getCoordinatorState(key);
    }

    @Override
    public List<CoordinatorState> listCoordinatorState()
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        if (!enabled) {
            return Collections.emptyList();
        }
        return super.listCoordinatorState();
    }

    @Override
    public List<CoordinatorState> listCoordinatorStateByEntityType(final EntityType.CoordinatorStateType entityType)
            throws ProvisionedThroughputException, DependencyException, InvalidStateException {
        if (!enabled) {
            return Collections.emptyList();
        }
        return super.listCoordinatorStateByEntityType(entityType);
    }

    @Override
    public boolean createCoordinatorStateIfNotExists(final CoordinatorState state)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (!enabled) {
            throw new InvalidStateException("Legacy table does not exist, cannot create coordinator state");
        }
        return super.createCoordinatorStateIfNotExists(state);
    }

    @Override
    public boolean updateCoordinatorStateWithExpectation(
            final CoordinatorState state, final Map<String, ExpectedAttributeValue> expectations)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (!enabled) {
            throw new InvalidStateException("Legacy table does not exist, cannot update coordinator state");
        }
        return super.updateCoordinatorStateWithExpectation(state, expectations);
    }

    @Override
    public boolean deleteCoordinatorState(final String key)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        if (!enabled) {
            throw new InvalidStateException("Legacy table does not exist, cannot delete coordinator state");
        }
        return super.deleteCoordinatorState(key);
    }
}
