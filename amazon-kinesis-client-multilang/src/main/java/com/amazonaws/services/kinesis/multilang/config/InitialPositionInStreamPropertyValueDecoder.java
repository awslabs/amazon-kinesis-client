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

import software.amazon.kinesis.common.InitialPositionInStream;

/**
 * Get an InitialiPosition enum property.
 */
class InitialPositionInStreamPropertyValueDecoder implements IPropertyValueDecoder<InitialPositionInStream> {

    /**
     * Constructor.
     */
    InitialPositionInStreamPropertyValueDecoder() {
    }

    /**
     * @param value property value as String
     * @return corresponding variable in correct type
     */
    @Override
    public InitialPositionInStream decodeValue(String value) {
        return InitialPositionInStream.valueOf(value.toUpperCase());
    }

    /**
     * @return list of supported types
     */
    @Override
    public List<Class<InitialPositionInStream>> getSupportedTypes() {
        return Arrays.asList(InitialPositionInStream.class);
    }
}
