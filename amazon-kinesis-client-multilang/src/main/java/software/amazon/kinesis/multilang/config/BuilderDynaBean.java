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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import lombok.Getter;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.DynaClass;
import org.apache.commons.beanutils.DynaProperty;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;

public class BuilderDynaBean implements DynaBean {

    private static final String[] CLASS_NAME_JOINERS = { ClassUtils.PACKAGE_SEPARATOR, ClassUtils.INNER_CLASS_SEPARATOR };
    static final String NO_MAP_ACCESS_SUPPORT = "Map access isn't supported";

    private Class<?> destinedClass;
    private final ConvertUtilsBean convertUtilsBean;
    private final List<String> classPrefixSearchList;

    private DynaBeanCreateSupport dynaBeanCreateSupport;
    private DynaBeanBuilderSupport dynaBeanBuilderSupport;

    @Getter
    private boolean isDirty = false;

    private final Function<String, ?> emptyPropertyHandler;
    private Object emptyPropertyResolved = null;

    public BuilderDynaBean(Class<?> destinedClass, ConvertUtilsBean convertUtilsBean, String... classPrefixSearchList) {
        this(destinedClass, convertUtilsBean, null, Arrays.asList(classPrefixSearchList));
    }

    public BuilderDynaBean(Class<?> destinedClass, ConvertUtilsBean convertUtilsBean,
            Function<String, ?> emptyPropertyHandler, String... classPrefixSearchList) {
        this(destinedClass, convertUtilsBean, emptyPropertyHandler, Arrays.asList(classPrefixSearchList));
    }

    public BuilderDynaBean(Class<?> destinedClass, ConvertUtilsBean convertUtilsBean,
            Function<String, ?> emptyPropertyHandler, List<String> classPrefixSearchList) {
        this.convertUtilsBean = convertUtilsBean;
        this.classPrefixSearchList = classPrefixSearchList;
        this.emptyPropertyHandler = emptyPropertyHandler;
        initialize(destinedClass);
    }

    private void initialize(Class<?> destinedClass) {
        this.destinedClass = destinedClass;

        if (DynaBeanBuilderUtils.isBuilderOrCreate(destinedClass)) {
            dynaBeanBuilderSupport = new DynaBeanBuilderSupport(destinedClass, convertUtilsBean, classPrefixSearchList);
            dynaBeanCreateSupport = new DynaBeanCreateSupport(destinedClass, convertUtilsBean, classPrefixSearchList);
        }
    }

    private void reinitializeFrom(String newClass) {
        Class<?> newClazz = null;
        List<String> attempts = new ArrayList<>();
        attempts.add(newClass);
        try {
            newClazz = Class.forName(newClass);
        } catch (ClassNotFoundException e) {
            //
            // Ignored
            //
        }
        if (newClazz == null) {
            for (String prefix : classPrefixSearchList) {
                for (String joiner : CLASS_NAME_JOINERS) {
                    String possibleClass;
                    if (prefix.endsWith(joiner)) {
                        possibleClass = prefix + newClass;
                    } else {
                        possibleClass = prefix + joiner + newClass;
                    }
                    attempts.add(possibleClass);
                    try {
                        newClazz = Class.forName(possibleClass);
                        break;
                    } catch (ClassNotFoundException e) {
                        //
                        // Ignored
                        //
                    }

                }
            }
        }

        if (newClazz == null) {
            throw new IllegalArgumentException(
                    "Unable to load class " + newClass + ". Attempted: (" + String.join(", ", attempts) + ")");
        }
        initialize(newClazz);
    }

    private void validatedExpectedClass(Class<?> source, Class<?> expected) {
        if (!ClassUtils.isAssignable(source, expected)) {
            throw new IllegalArgumentException(
                    String.format("%s cannot be assigned to %s.", source.getName(), expected.getName()));
        }
    }

    public boolean canBuildOrCreate() {
        return dynaBeanBuilderSupport != null || dynaBeanCreateSupport != null;
    }

    private void validateCanBuildOrCreate() {
        if (!canBuildOrCreate()) {
            throw new IllegalStateException("Unable to to introspect or handle " + destinedClass.getName()
                    + " as it doesn't have a builder or create method.");
        }
    }

    @SafeVarargs
    public final <T> T build(Class<T> expected, Function<Object, Object>... additionalMutators) {
        if (emptyPropertyResolved != null) {
            validatedExpectedClass(emptyPropertyResolved.getClass(), expected);
            return expected.cast(emptyPropertyResolved);
        }

        if (dynaBeanBuilderSupport == null && dynaBeanCreateSupport == null) {
            return null;
        }

        validatedExpectedClass(destinedClass, expected);
        if (dynaBeanBuilderSupport.isValid()) {
            return expected.cast(dynaBeanBuilderSupport.build(additionalMutators));
        } else {
            return expected.cast(dynaBeanCreateSupport.build());
        }
    }

    private void validateResolvedEmptyHandler() {
        if (emptyPropertyResolved != null) {
            throw new IllegalStateException("When a property handler is resolved further properties may not be set.");
        }
    }

    boolean hasValue(String name) {
        if (dynaBeanBuilderSupport != null) {
            return dynaBeanBuilderSupport.hasValue(name);
        }
        return false;
    }

    @Override
    public boolean contains(String name, String key) {
        throw new UnsupportedOperationException(NO_MAP_ACCESS_SUPPORT);
    }

    @Override
    public Object get(String name) {
        validateResolvedEmptyHandler();
        isDirty = true;
        return dynaBeanBuilderSupport.get(name);
    }

    @Override
    public Object get(String name, int index) {
        validateResolvedEmptyHandler();
        isDirty = true;
        if (StringUtils.isEmpty(name)) {
            return dynaBeanCreateSupport.get(name, index);
        }
        return dynaBeanBuilderSupport.get(name, index);
    }

    @Override
    public Object get(String name, String key) {
        throw new UnsupportedOperationException(NO_MAP_ACCESS_SUPPORT);
    }

    @Override
    public DynaClass getDynaClass() {
        return new DynaClass() {
            @Override
            public String getName() {
                return destinedClass.getName();
            }

            @Override
            public DynaProperty getDynaProperty(String name) {
                if (StringUtils.isEmpty(name)) {
                    return new DynaProperty(name);
                }
                if ("class".equals(name)) {
                    return new DynaProperty(name, String.class);
                }
                //
                // We delay validation until after the class check to allow for re-initialization for a specific class.
                // The check for isEmpty is allowed ahead of this check to allow for raw string support.
                //
                validateCanBuildOrCreate();
                List<TypeTag> types = dynaBeanBuilderSupport.getProperty(name);
                if (types.size() > 1) {
                    Optional<TypeTag> arrayType = types.stream().filter(t -> t.type.isArray()).findFirst();
                    return arrayType.map(t -> new DynaProperty(name, t.type, t.type.getComponentType()))
                            .orElseGet(() -> new DynaProperty(name));
                } else {
                    TypeTag type = types.get(0);
                    if (type.hasConverter) {
                        return new DynaProperty(name, type.type);
                    }
                    if (type.type.isEnum()) {
                        return new DynaProperty(name, String.class);
                    }
                    return new DynaProperty(name, BuilderDynaBean.class);
                }
            }

            @Override
            public DynaProperty[] getDynaProperties() {
                validateCanBuildOrCreate();
                return dynaBeanBuilderSupport.getPropertyNames().stream().map(this::getDynaProperty)
                        .toArray(DynaProperty[]::new);
            }

            @Override
            public DynaBean newInstance() {
                return null;
            }
        };
    }

    @Override
    public void remove(String name, String key) {
        throw new UnsupportedOperationException(NO_MAP_ACCESS_SUPPORT);
    }

    @Override
    public void set(String name, Object value) {
        validateResolvedEmptyHandler();
        isDirty = true;
        if (emptyPropertyHandler != null && StringUtils.isEmpty(name) && value instanceof String) {
            emptyPropertyResolved = emptyPropertyHandler.apply((String) value);
            return;
        }

        if ("class".equals(name)) {
            reinitializeFrom(value.toString());
        } else {
            validateResolvedEmptyHandler();
            dynaBeanBuilderSupport.set(name, value);
        }
    }

    @Override
    public void set(String name, int index, Object value) {
        validateResolvedEmptyHandler();
        validateCanBuildOrCreate();
        isDirty = true;
        if (StringUtils.isEmpty(name)) {
            dynaBeanCreateSupport.set(name, index, value);
        } else {
            dynaBeanBuilderSupport.set(name, index, value);
        }
    }

    @Override
    public void set(String name, String key, Object value) {
        throw new UnsupportedOperationException(NO_MAP_ACCESS_SUPPORT);
    }
}
