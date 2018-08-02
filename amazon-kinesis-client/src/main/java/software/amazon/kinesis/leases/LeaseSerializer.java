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


import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.kinesis.leases.Lease;

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
     * @return the key schema for creating a DynamoDB table to store leases
     */
    Collection<KeySchemaElement> getKeySchema();

    /**
     * @return attribute definitions for creating a DynamoDB table to store leases
     */
    Collection<AttributeDefinition> getAttributeDefinitions();
}
