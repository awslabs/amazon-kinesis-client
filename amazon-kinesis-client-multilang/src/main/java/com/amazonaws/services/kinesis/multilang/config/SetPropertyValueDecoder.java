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
package com.amazonaws.services.kinesis.multilang.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Provide {@link Set} property value. Note that since parameterized value cannot be figured out during compile time
 * for setter methods, only {@code Set} of {@code String}s are supported as property value decode.
 */
@SuppressWarnings("rawtypes")
class SetPropertyValueDecoder implements IPropertyValueDecoder<Set> {

    /**
     * Delimiter for the list provided as string.
     */
    private static final String LIST_DELIMITER = ",";

    /**
     * Package constructor for factory use only.
     */
    SetPropertyValueDecoder() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set decodeValue(String propertyValue) {
        String[] values = propertyValue.split(LIST_DELIMITER);
        String value = null;
        Set<String> decodedValue = new HashSet<>();
        for (int i = 0; i < values.length; i++) {
            value = values[i].trim();
            if (!value.isEmpty()) {
                decodedValue.add(value);
            }
        }
        return decodedValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Class<Set>> getSupportedTypes() {
        return Arrays.asList(Set.class);
    }

}
