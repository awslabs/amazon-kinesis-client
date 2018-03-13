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
import java.util.List;

/**
 * Provide boolean property.
 */
class BooleanPropertyValueDecoder implements IPropertyValueDecoder<Boolean> {

    /**
     * Constructor.
     */
    BooleanPropertyValueDecoder() {
    }

    /**
     * @param value property value as String
     * @return corresponding variable in correct type
     */
    @Override
    public Boolean decodeValue(String value) {
        return Boolean.parseBoolean(value);
    }

    /**
     * @return list of supported types
     */
    @Override
    public List<Class<Boolean>> getSupportedTypes() {
        return Arrays.asList(boolean.class, Boolean.class);
    }

}
