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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class DynaBeanBuilderUtils {

    static Method getMethod(Class<?> clazz, String name, Class<?>... parameterTypes) {
        try {
            return clazz.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    static Object invokeOrFail(Method method, Object onObject, Object... arguments) {
        try {
            return method.invoke(onObject, arguments);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    static boolean isBuilderOrCreate(Class<?> clazz) {
        Method buildMethod = null;

        try {
            buildMethod = clazz.getMethod("builder");
        } catch (NoSuchMethodException e) {
            //
            // Ignored
            //
        }

        boolean hasCreate = Arrays.stream(clazz.getMethods())
                .anyMatch(m -> "create".equals(m.getName()) && m.getReturnType().isAssignableFrom(clazz));

        return buildMethod != null || hasCreate;
    }
}
