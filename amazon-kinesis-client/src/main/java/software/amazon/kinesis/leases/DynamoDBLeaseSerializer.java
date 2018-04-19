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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

/**
 * An implementation of ILeaseSerializer for basic Lease objects. Can also instantiate subclasses of Lease so that
 * LeaseSerializer can be decorated by other classes if you need to add fields to leases.
 */
public class DynamoDBLeaseSerializer implements LeaseSerializer<Lease> {

    public final String LEASE_KEY_KEY = "leaseKey";
    public final String LEASE_OWNER_KEY = "leaseOwner";
    public final String LEASE_COUNTER_KEY = "leaseCounter";
    public final Class<? extends Lease> clazz;

    public DynamoDBLeaseSerializer() {
        this.clazz = Lease.class;
    }

    public DynamoDBLeaseSerializer(Class<? extends Lease> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Map<String, AttributeValue> toDynamoRecord(Lease lease) {
        Map<String, AttributeValue> result = new HashMap<String, AttributeValue>();

        result.put(LEASE_KEY_KEY, DynamoUtils.createAttributeValue(lease.getLeaseKey()));
        result.put(LEASE_COUNTER_KEY, DynamoUtils.createAttributeValue(lease.getLeaseCounter()));

        if (lease.getLeaseOwner() != null) {
            result.put(LEASE_OWNER_KEY, DynamoUtils.createAttributeValue(lease.getLeaseOwner()));
        }

        return result;
    }

    @Override
    public Lease fromDynamoRecord(Map<String, AttributeValue> dynamoRecord) {
        Lease result;
        try {
            result = clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        result.setLeaseKey(DynamoUtils.safeGetString(dynamoRecord, LEASE_KEY_KEY));
        result.setLeaseOwner(DynamoUtils.safeGetString(dynamoRecord, LEASE_OWNER_KEY));
        result.setLeaseCounter(DynamoUtils.safeGetLong(dynamoRecord, LEASE_COUNTER_KEY));

        return result;
    }

    @Override
    public Map<String, AttributeValue> getDynamoHashKey(String leaseKey) {
        Map<String, AttributeValue> result = new HashMap<String, AttributeValue>();

        result.put(LEASE_KEY_KEY, DynamoUtils.createAttributeValue(leaseKey));

        return result;
    }

    @Override
    public Map<String, AttributeValue> getDynamoHashKey(Lease lease) {
        return getDynamoHashKey(lease.getLeaseKey());
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoLeaseCounterExpectation(Lease lease) {
        return getDynamoLeaseCounterExpectation(lease.getLeaseCounter());
    }

    public Map<String, ExpectedAttributeValue> getDynamoLeaseCounterExpectation(Long leaseCounter) {
        Map<String, ExpectedAttributeValue> result = new HashMap<String, ExpectedAttributeValue>();

        ExpectedAttributeValue eav = new ExpectedAttributeValue(DynamoUtils.createAttributeValue(leaseCounter));
        result.put(LEASE_COUNTER_KEY, eav);

        return result;
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoLeaseOwnerExpectation(Lease lease) {
        Map<String, ExpectedAttributeValue> result = new HashMap<String, ExpectedAttributeValue>();

        ExpectedAttributeValue eav = null;
        
        if (lease.getLeaseOwner() == null) {
            eav = new ExpectedAttributeValue(false);
        } else {
            eav = new ExpectedAttributeValue(DynamoUtils.createAttributeValue(lease.getLeaseOwner()));
        }
        
        result.put(LEASE_OWNER_KEY, eav);

        return result;
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoNonexistantExpectation() {
        Map<String, ExpectedAttributeValue> result = new HashMap<String, ExpectedAttributeValue>();

        ExpectedAttributeValue expectedAV = new ExpectedAttributeValue(false);
        result.put(LEASE_KEY_KEY, expectedAV);

        return result;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoLeaseCounterUpdate(Lease lease) {
        return getDynamoLeaseCounterUpdate(lease.getLeaseCounter());
    }

    public Map<String, AttributeValueUpdate> getDynamoLeaseCounterUpdate(Long leaseCounter) {
        Map<String, AttributeValueUpdate> result = new HashMap<String, AttributeValueUpdate>();

        AttributeValueUpdate avu =
                new AttributeValueUpdate(DynamoUtils.createAttributeValue(leaseCounter + 1), AttributeAction.PUT);
        result.put(LEASE_COUNTER_KEY, avu);

        return result;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoTakeLeaseUpdate(Lease lease, String owner) {
        Map<String, AttributeValueUpdate> result = new HashMap<String, AttributeValueUpdate>();

        result.put(LEASE_OWNER_KEY, new AttributeValueUpdate(DynamoUtils.createAttributeValue(owner),
                AttributeAction.PUT));

        return result;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoEvictLeaseUpdate(Lease lease) {
        Map<String, AttributeValueUpdate> result = new HashMap<String, AttributeValueUpdate>();

        result.put(LEASE_OWNER_KEY, new AttributeValueUpdate(null, AttributeAction.DELETE));

        return result;
    }

    @Override
    public Map<String, AttributeValueUpdate> getDynamoUpdateLeaseUpdate(Lease lease) {
        // There is no application-specific data in Lease - just return a map that increments the counter.
        return new HashMap<String, AttributeValueUpdate>();
    }

    @Override
    public Collection<KeySchemaElement> getKeySchema() {
        List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName(LEASE_KEY_KEY).withKeyType(KeyType.HASH));

        return keySchema;
    }

    @Override
    public Collection<AttributeDefinition> getAttributeDefinitions() {
        List<AttributeDefinition> definitions = new ArrayList<AttributeDefinition>();
        definitions.add(new AttributeDefinition().withAttributeName(LEASE_KEY_KEY)
                .withAttributeType(ScalarAttributeType.S));

        return definitions;
    }
}
