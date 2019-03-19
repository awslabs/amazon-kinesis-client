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

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Repeatable(ConfigurationSettables.class)
public @interface ConfigurationSettable {

    /**
     * Which builder this option applies to
     * 
     * @return the class of the builder to use
     */
    Class<?> configurationClass();

    /**
     * The method name on the builder, defaults to the fieldName
     * 
     * @return the name of the method or null to use the default
     */
    String methodName() default "";

    /**
     * If the type is actually an optional value this will enable conversions
     * 
     * @return true if the value should be wrapped by an optional
     */
    boolean convertToOptional() default false;
}
