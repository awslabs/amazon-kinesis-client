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
