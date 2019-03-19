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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.lang3.StringUtils;

class DynaBeanCreateSupport {

    private final Class<?> destinedClass;
    private final ConvertUtilsBean convertUtilsBean;
    private final List<String> classPrefixSearchList;
    private final List<TypeTag> createTypes = new ArrayList<>();
    private Object[] createValues = null;

    DynaBeanCreateSupport(Class<?> destinedClass, ConvertUtilsBean convertUtilsBean,
            List<String> classPrefixSearchList) {
        this.destinedClass = destinedClass;
        this.convertUtilsBean = convertUtilsBean;
        this.classPrefixSearchList = classPrefixSearchList;

        readTypes();
    }

    private void readTypes() {
        for (Method method : destinedClass.getMethods()) {
            if ("create".equals(method.getName()) && method.getReturnType().isAssignableFrom(destinedClass)) {
                createValues = new Object[method.getParameterCount()];
                int i = 0;
                for (Class<?> paramType : method.getParameterTypes()) {
                    if (convertUtilsBean.lookup(paramType) != null) {
                        createTypes.add(new TypeTag(paramType, true, null));
                    } else {
                        createTypes.add(new TypeTag(paramType, false, null));
                    }
                    ++i;
                }
            }
        }
    }

    Object build() {

        Method createMethod = DynaBeanBuilderUtils.getMethod(destinedClass, "create",
                createTypes.stream().map(t -> t.type).toArray(i -> new Class<?>[i]));
        Object arguments[] = new Object[createValues.length];
        for (int i = 0; i < createValues.length; ++i) {
            if (createValues[i] instanceof BuilderDynaBean) {
                arguments[i] = ((BuilderDynaBean) createValues[i]).build(createTypes.get(i).type);
            } else {
                arguments[i] = createValues[i];
            }
        }
        return DynaBeanBuilderUtils.invokeOrFail(createMethod, null, arguments);
    }

    public Object get(String name, int index) {
        if (index < createValues.length) {
            if (createTypes.get(index).hasConverter) {
                return createValues[index];
            } else {
                if (createValues[index] == null) {
                    createValues[index] = new BuilderDynaBean(createTypes.get(index).type, convertUtilsBean, null,
                            classPrefixSearchList);
                }
                return createValues[index];
            }
        }
        return null;
    }

    public void set(String name, int index, Object value) {
        if (StringUtils.isEmpty(name)) {
            if (index >= createValues.length) {
                throw new IllegalArgumentException(
                        String.format("%d exceeds the maximum number of arguments (%d) for %s", index,
                                createValues.length, destinedClass.getName()));
            }
            createValues[index] = value;
        }

    }

}
