/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.leases;

import java.util.Collection;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import software.amazon.kinesis.leases.Lease;

/**
 * Utility class that manages the mapping of Lease objects/operations to records in DynamoDB.
 * 
 * @param <T> Lease subclass, possibly Lease itself
 */
public interface LeaseSerializer<T extends Lease> {

    /**
     * Construct a DynamoDB record out of a Lease object
     * 
     * @param lease lease object to serialize
     * @return an attribute value map representing the lease object
     */
    public Map<String, AttributeValue> toDynamoRecord(T lease);

    /**
     * Construct a Lease object out of a DynamoDB record.
     * 
     * @param dynamoRecord attribute value map from DynamoDB
     * @return a deserialized lease object representing the attribute value map
     */
    public T fromDynamoRecord(Map<String, AttributeValue> dynamoRecord);

    /**
     * @param lease
     * @return the attribute value map representing a Lease's hash key given a Lease object.
     */
    public Map<String, AttributeValue> getDynamoHashKey(T lease);

    /**
     * Special getDynamoHashKey implementation used by ILeaseManager.getLease().
     * 
     * @param leaseKey
     * @return the attribute value map representing a Lease's hash key given a string.
     */
    public Map<String, AttributeValue> getDynamoHashKey(String leaseKey);

    /**
     * @param lease
     * @return the attribute value map asserting that a lease counter is what we expect.
     */
    public Map<String, ExpectedAttributeValue> getDynamoLeaseCounterExpectation(T lease);

    /**
     * @param lease
     * @return the attribute value map asserting that the lease owner is what we expect.
     */
    public Map<String, ExpectedAttributeValue> getDynamoLeaseOwnerExpectation(T lease);

    /**
     * @return the attribute value map asserting that a lease does not exist.
     */
    public Map<String, ExpectedAttributeValue> getDynamoNonexistantExpectation();

    /**
     * @param lease
     * @return the attribute value map that increments a lease counter
     */
    public Map<String, AttributeValueUpdate> getDynamoLeaseCounterUpdate(T lease);

    /**
     * @param lease
     * @param newOwner
     * @return the attribute value map that takes a lease for a new owner
     */
    public Map<String, AttributeValueUpdate> getDynamoTakeLeaseUpdate(T lease, String newOwner);

    /**
     * @param lease
     * @return the attribute value map that voids a lease
     */
    public Map<String, AttributeValueUpdate> getDynamoEvictLeaseUpdate(T lease);

    /**
     * @param lease
     * @return the attribute value map that updates application-specific data for a lease and increments the lease
     *         counter
     */
    public Map<String, AttributeValueUpdate> getDynamoUpdateLeaseUpdate(T lease);

    /**
     * @return the key schema for creating a DynamoDB table to store leases
     */
    public Collection<KeySchemaElement> getKeySchema();

    /**
     * @return attribute definitions for creating a DynamoDB table to store leases
     */
    public Collection<AttributeDefinition> getAttributeDefinitions();
}
