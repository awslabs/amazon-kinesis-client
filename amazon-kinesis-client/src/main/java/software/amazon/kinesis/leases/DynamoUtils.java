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

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Static utility functions used by our LeaseSerializers.
 */
@KinesisClientInternalApi
public class DynamoUtils {

    public static AttributeValue createAttributeValue(Collection<String> collectionValue) {
        if (collectionValue == null || collectionValue.isEmpty()) {
            throw new IllegalArgumentException("Collection attributeValues cannot be null or empty.");
        }

        return AttributeValue.builder().ss(collectionValue).build();
    }

    public static AttributeValue createAttributeValue(String stringValue) {
        if (stringValue == null || stringValue.isEmpty()) {
            throw new IllegalArgumentException("String attributeValues cannot be null or empty.");
        }

        return AttributeValue.builder().s(stringValue).build();
    }

    public static AttributeValue createAttributeValue(Long longValue) {
        if (longValue == null) {
            throw new IllegalArgumentException("Number AttributeValues cannot be null.");
        }

        return AttributeValue.builder().n(longValue.toString()).build();
    }

    public static Long safeGetLong(Map<String, AttributeValue> dynamoRecord, String key) {
        AttributeValue av = dynamoRecord.get(key);
        if (av == null) {
            return null;
        } else {
            return new Long(av.n());
        }
    }

    public static String safeGetString(Map<String, AttributeValue> dynamoRecord, String key) {
        AttributeValue av = dynamoRecord.get(key);
        if (av == null) {
            return null;
        } else {
            return av.s();
        }
    }

    public static List<String> safeGetSS(Map<String, AttributeValue> dynamoRecord, String key) {
        AttributeValue av = dynamoRecord.get(key);

        if (av == null) {
            return new ArrayList<String>();
        } else {
            return av.ss();
        }
    }

}
