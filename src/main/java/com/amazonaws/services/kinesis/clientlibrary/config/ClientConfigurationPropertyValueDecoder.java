/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.config;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.ClientConfiguration;
/**
 * Get ClientConfiguration property.
 */
class ClientConfigurationPropertyValueDecoder implements IPropertyValueDecoder<ClientConfiguration> {

    /**
     * Constructor.
     */
    ClientConfigurationPropertyValueDecoder() {
    }

    /**
     * @param value property value as String
     * @return corresponding variable in correct type
     */
    @Override
    public ClientConfiguration decodeValue(String value) {
        throw new UnsupportedOperationException("ClientConfiguration is currently not supported");
    }

    /**
     * Get supported types.
     * @return a list of supported class
     */
    @Override
    public List<Class<ClientConfiguration>> getSupportedTypes() {
        return Arrays.asList(ClientConfiguration.class);
    }

}
