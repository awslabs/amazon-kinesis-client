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
package software.amazon.kinesis.leases;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;

/**
 * Utility class that manages the mapping of Lease objects/operations to records in DynamoDB.
 */
public interface LeaseSerializer {

    /**
     * Construct a DynamoDB record out of a Lease object
     *
     * @param lease lease object to serialize
     * @return an attribute value map representing the lease object
     */
    Map<String, AttributeValue> toDynamoRecord(Lease lease);

    /**
     * Construct a Lease object out of a DynamoDB record.
     *
     * @param dynamoRecord attribute value map from DynamoDB
     * @return a deserialized lease object representing the attribute value map
     */
    Lease fromDynamoRecord(Map<String, AttributeValue> dynamoRecord);

    default Lease fromDynamoRecord(Map<String, AttributeValue> dynamoRecord, Lease leaseToUpdate) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param lease
     * @return the attribute value map representing a Lease's hash key given a Lease object.
     */
    Map<String, AttributeValue> getDynamoHashKey(Lease lease);

    /**
     * Special getDynamoHashKey implementation used by {@link LeaseRefresher#getLease(String)}.
     *
     * @param leaseKey
     * @return the attribute value map representing a Lease's hash key given a string.
     */
    Map<String, AttributeValue> getDynamoHashKey(String leaseKey);

    /**
     * @param lease
     * @return the attribute value map asserting that a lease counter is what we expect.
     */
    Map<String, ExpectedAttributeValue> getDynamoLeaseCounterExpectation(Lease lease);

    /**
     * @param lease
     * @return the attribute value map asserting that the lease owner is what we expect.
     */
    Map<String, ExpectedAttributeValue> getDynamoLeaseOwnerExpectation(Lease lease);

    /**
     * @return the attribute value map asserting that a lease does not exist.
     */
    Map<String, ExpectedAttributeValue> getDynamoNonexistantExpectation();

    /**
     * @param leaseKey
     * @return the attribute value map asserting that a lease does exist.
     */
    default Map<String, ExpectedAttributeValue> getDynamoExistentExpectation(String leaseKey) {
        throw new UnsupportedOperationException("DynamoExistantExpectation is not implemented");
    }

    /**
     * @param lease
     * @return the attribute value map that increments a lease counter
     */
    Map<String, AttributeValueUpdate> getDynamoLeaseCounterUpdate(Lease lease);

    /**
     * @param lease
     * @param newOwner
     * @return the attribute value map that takes a lease for a new owner
     */
    Map<String, AttributeValueUpdate> getDynamoTakeLeaseUpdate(Lease lease, String newOwner);

    /**
     * @param lease lease that needs to be assigned
     * @param newOwner newLeaseOwner
     * @return the attribute value map that takes a lease for a new owner
     */
    default Map<String, AttributeValueUpdate> getDynamoAssignLeaseUpdate(Lease lease, String newOwner) {
        throw new UnsupportedOperationException("getDynamoAssignLeaseUpdate is not implemented");
    }

    /**
     * @param lease
     * @return the attribute value map that voids a lease
     */
    Map<String, AttributeValueUpdate> getDynamoEvictLeaseUpdate(Lease lease);

    /**
     * @param lease
     * @return the attribute value map that updates application-specific data for a lease and increments the lease
     *         counter
     */
    Map<String, AttributeValueUpdate> getDynamoUpdateLeaseUpdate(Lease lease);

    /**
     * @param lease
     * @param updateField
     * @return the attribute value map that updates application-specific data for a lease
     */
    default Map<String, AttributeValueUpdate> getDynamoUpdateLeaseUpdate(Lease lease, UpdateField updateField) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the key schema for creating a DynamoDB table to store leases
     */
    Collection<KeySchemaElement> getKeySchema();

    default Collection<KeySchemaElement> getWorkerIdToLeaseKeyIndexKeySchema() {
        return Collections.EMPTY_LIST;
    }

    default Collection<AttributeDefinition> getWorkerIdToLeaseKeyIndexAttributeDefinitions() {
        return Collections.EMPTY_LIST;
    }

    /**
     * @return attribute definitions for creating a DynamoDB table to store leases
     */
    Collection<AttributeDefinition> getAttributeDefinitions();

    /**
     * @param lease
     * @return the attribute value map that includes lease throughput
     */
    Map<String, AttributeValueUpdate> getDynamoLeaseThroughputKbpsUpdate(Lease lease);
}
