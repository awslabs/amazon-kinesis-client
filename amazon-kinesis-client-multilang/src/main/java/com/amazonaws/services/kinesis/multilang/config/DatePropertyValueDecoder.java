/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.multilang.config;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Provide Date property.
 */
public class DatePropertyValueDecoder implements IPropertyValueDecoder<Date> {

    /**
     * Constructor.
     */
    DatePropertyValueDecoder() {
    }

    /**
     * @param value property value as String
     * @return corresponding variable in correct type
     */
    @Override
    public Date decodeValue(String value) {
        try {
            return new Date(Long.parseLong(value) * 1000L);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Date property value must be numeric.");
        }
    }

    /**
     * @return list of supported types
     */
    @Override
    public List<Class<Date>> getSupportedTypes() {
        return Arrays.asList(Date.class);
    }

}
