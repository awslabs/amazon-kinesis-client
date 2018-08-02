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

import java.util.List;

/**
 * This class captures the concept of decoding a property value to a particular Java type.
 * 
 * @param <T>
 */
interface IPropertyValueDecoder<T> {
    /**
     * Get the value that was read from a configuration file and convert it to some type.
     * 
     * @param propertyValue property string value that needs to be decoded.
     * @return property value in type T
     */
    T decodeValue(String propertyValue);

    /**
     * Get a list of supported types for this class.
     * 
     * @return list of supported classes.
     */
    List<Class<T>> getSupportedTypes();
}
