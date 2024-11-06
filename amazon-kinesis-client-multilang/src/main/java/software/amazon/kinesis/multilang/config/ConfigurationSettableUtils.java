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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Defaults;
import lombok.NonNull;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;

public class ConfigurationSettableUtils {

    public static <T> T resolveFields(@NonNull Object source, @NonNull T configObject) {
        Map<Class<?>, Object> configObjects = new HashMap<>();
        configObjects.put(configObject.getClass(), configObject);
        resolveFields(source, configObjects, null, null);

        return configObject;
    }

    public static void resolveFields(
            Object source, Map<Class<?>, Object> configObjects, Set<Class<?>> restrictTo, Set<Class<?>> skipIf) {
        for (Field field : source.getClass().getDeclaredFields()) {
            for (ConfigurationSettable b : field.getAnnotationsByType(ConfigurationSettable.class)) {
                if (restrictTo != null && !restrictTo.contains(b.configurationClass())) {
                    continue;
                }
                if (skipIf != null && skipIf.contains(b.configurationClass())) {
                    continue;
                }
                field.setAccessible(true);
                Object configObject = configObjects.get(b.configurationClass());
                if (configObject != null) {
                    String setterName = field.getName();
                    if (!StringUtils.isEmpty(b.methodName())) {
                        setterName = b.methodName();
                    }
                    Object value;
                    try {
                        value = field.get(source);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }

                    if (value != null && !value.equals(Defaults.defaultValue(field.getType()))) {
                        Method setter = null;
                        if (b.convertToOptional()) {
                            value = Optional.of(value);
                        }
                        if (ClassUtils.isPrimitiveOrWrapper(value.getClass())) {
                            Class<?> primitiveType = field.getType().isPrimitive()
                                    ? field.getType()
                                    : ClassUtils.wrapperToPrimitive(field.getType());
                            Class<?> wrapperType = !field.getType().isPrimitive()
                                    ? field.getType()
                                    : ClassUtils.primitiveToWrapper(field.getType());

                            try {
                                setter = b.configurationClass().getMethod(setterName, primitiveType);
                            } catch (NoSuchMethodException e) {
                                //
                                // Ignore this
                                //
                            }
                            if (setter == null) {
                                try {
                                    setter = b.configurationClass().getMethod(setterName, wrapperType);
                                } catch (NoSuchMethodException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        } else {
                            try {
                                setter = b.configurationClass().getMethod(setterName, value.getClass());
                            } catch (NoSuchMethodException e) {
                                // find if there is a setter which is not the exact parameter type
                                // but is assignable from the type
                                for (Method method : b.configurationClass().getMethods()) {
                                    Class<?>[] parameterTypes = method.getParameterTypes();
                                    if (method.getName().equals(setterName)
                                            && parameterTypes.length == 1
                                            && parameterTypes[0].isAssignableFrom(value.getClass())) {
                                        setter = method;
                                        break;
                                    }
                                }
                                if (setter == null) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                        try {
                            setter.invoke(configObject, value);
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }
}
