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
package software.amazon.kinesis.coordinator;

import java.util.HashMap;
import java.util.Map;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.EntityDAO.Entity;
import software.amazon.kinesis.leases.EntityType;

/**
 * DataModel for CoordinatorState, this data model is used to store various state information required
 * for coordination across the KCL worker fleet. Therefore, the model follows a flexible schema.
 *
 * Subclasses override {@link #serialize()} and {@link #getDynamoUpdate()} to provide their
 * specific serialization logic. The DAO delegate holds a registry of deserializers and uses
 * polymorphic calls for serialize/update to avoid instanceof checks.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@Slf4j
@KinesisClientInternalApi
public class CoordinatorState implements Entity {

    public static final String ENTITY_TYPE_ATTRIBUTE_NAME = "entityType";

    private String key;

    private EntityType.CoordinatorStateType coordinatorStateEntityType;

    private Map<String, AttributeValue> attributes;

    /**
     * Returns the parent {@link EntityType} for this coordinator state entity.
     * Delegates to the {@link EntityType.CoordinatorStateType#getEntityType()} method.
     */
    @Override
    public EntityType getEntityType() {
        return coordinatorStateEntityType != null ? coordinatorStateEntityType.getEntityType() : null;
    }

    /**
     * Serialize this state's attributes into a map of DynamoDB attribute values.
     * Includes the entityType attribute and any generic attributes.
     * Subclasses should call {@code super.serialize()} and then add their own domain-specific fields.
     *
     * @return map of attribute name to AttributeValue for DynamoDB storage
     */
    public Map<String, AttributeValue> serialize() {
        final HashMap<String, AttributeValue> result = new HashMap<>();
        // key will be added by the DAO because it changed based on the table it is being written to
        if (coordinatorStateEntityType != null) {
            result.put(ENTITY_TYPE_ATTRIBUTE_NAME, AttributeValue.fromS(coordinatorStateEntityType.getDdbValue()));
        }
        if (attributes != null) {
            result.putAll(attributes);
        }
        return result;
    }

    /**
     * Produce an update map for DynamoDB UpdateItem operations.
     * Subclasses should override this to provide their specific update logic.
     * The base implementation converts all attributes to PUT updates.
     *
     * @return map of attribute name to AttributeValueUpdate for DynamoDB UpdateItem
     */
    public Map<String, AttributeValueUpdate> getDynamoUpdate() {
        final HashMap<String, AttributeValueUpdate> updates = new HashMap<>();
        if (attributes != null) {
            attributes.forEach((attribute, value) -> updates.put(
                    attribute,
                    AttributeValueUpdate.builder()
                            .value(value)
                            .action(AttributeAction.PUT)
                            .build()));
        }
        return updates;
    }
}
