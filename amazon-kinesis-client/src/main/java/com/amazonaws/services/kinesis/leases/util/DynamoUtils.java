/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.leases.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Static utility functions used by our LeaseSerializers.
 */
public class DynamoUtils {

    public static AttributeValue createAttributeValue(Collection<String> collectionValue) {
        if (collectionValue == null || collectionValue.isEmpty()) {
            throw new IllegalArgumentException("Collection attributeValues cannot be null or empty.");
        }

        return new AttributeValue().withSS(collectionValue);
    }

    public static AttributeValue createAttributeValue(String stringValue) {
        if (stringValue == null || stringValue.isEmpty()) {
            throw new IllegalArgumentException("String attributeValues cannot be null or empty.");
        }

        return new AttributeValue().withS(stringValue);
    }

    public static AttributeValue createAttributeValue(Long longValue) {
        if (longValue == null) {
            throw new IllegalArgumentException("Number AttributeValues cannot be null.");
        }

        return new AttributeValue().withN(longValue.toString());
    }

    public static Long safeGetLong(Map<String, AttributeValue> dynamoRecord, String key) {
        AttributeValue av = dynamoRecord.get(key);
        if (av == null) {
            return null;
        } else {
            return new Long(av.getN());
        }
    }

    public static String safeGetString(Map<String, AttributeValue> dynamoRecord, String key) {
        AttributeValue av = dynamoRecord.get(key);
        if (av == null) {
            return null;
        } else {
            return av.getS();
        }
    }

    public static List<String> safeGetSS(Map<String, AttributeValue> dynamoRecord, String key) {
        AttributeValue av = dynamoRecord.get(key);

        if (av == null) {
            return new ArrayList<String>();
        } else {
            return av.getSS();
        }
    }

}
