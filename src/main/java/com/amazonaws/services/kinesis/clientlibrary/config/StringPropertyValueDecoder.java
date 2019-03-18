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
package com.amazonaws.services.kinesis.clientlibrary.config;

import java.util.Arrays;
import java.util.List;

/**
 * Get string properties from properties file.
 */
class StringPropertyValueDecoder implements IPropertyValueDecoder<String> {

    /**
     * package constructor for factory use only.
     */
    StringPropertyValueDecoder() {
    }

    /**
     * @param value the property value
     * @return the value as String
     */
    @Override
    public String decodeValue(String value) {
        // How to treat null or empty String should depend on those method who
        // uses the String value. Here just return the string as it is.
        if (value == null) {
            return null;
        }
        return value.trim(); 
    }

    /**
     * @return list of supported types
     */
    @Override
    public List<Class<String>> getSupportedTypes() {
        return Arrays.asList(String.class);
    }

}
