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
package software.amazon.kinesis.multilang.config;

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
