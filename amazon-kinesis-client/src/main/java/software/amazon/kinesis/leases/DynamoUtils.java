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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

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

    public static AttributeValue createAttributeValue(byte[] byteBufferValue) {
        if (byteBufferValue == null) {
            throw new IllegalArgumentException("Byte buffer attributeValues cannot be null or empty.");
        }

        return AttributeValue.builder()
                .b(SdkBytes.fromByteArray(byteBufferValue))
                .build();
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

    public static byte[] safeGetByteArray(Map<String, AttributeValue> dynamoRecord, String key) {
        AttributeValue av = dynamoRecord.get(key);
        if (av == null) {
            return null;
        } else {
            return av.b().asByteArray();
        }
    }

    public static Long safeGetLong(Map<String, AttributeValue> dynamoRecord, String key) {
        AttributeValue av = dynamoRecord.get(key);
        if (av == null) {
            return null;
        } else {
            return new Long(av.n());
        }
    }

    public static AttributeValue createAttributeValue(Double doubleValue) {
        if (doubleValue == null) {
            throw new IllegalArgumentException("Double attributeValues cannot be null.");
        }

        return AttributeValue.builder().n(doubleValue.toString()).build();
    }

    public static String safeGetString(Map<String, AttributeValue> dynamoRecord, String key) {
        AttributeValue av = dynamoRecord.get(key);
        return safeGetString(av);
    }

    public static String safeGetString(AttributeValue av) {
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

    public static Double safeGetDouble(Map<String, AttributeValue> dynamoRecord, String key) {
        AttributeValue av = dynamoRecord.get(key);
        if (av == null) {
            return null;
        } else {
            return new Double(av.n());
        }
    }
}
