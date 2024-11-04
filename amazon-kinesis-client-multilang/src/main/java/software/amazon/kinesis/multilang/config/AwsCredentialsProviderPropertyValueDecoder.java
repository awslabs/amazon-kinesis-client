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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.kinesis.multilang.auth.KclStsAssumeRoleCredentialsProvider;

/**
 * Get AwsCredentialsProvider property.
 */
@Slf4j
class AwsCredentialsProviderPropertyValueDecoder implements IPropertyValueDecoder<AwsCredentialsProvider> {
    private static final String LIST_DELIMITER = ",";
    private static final String ARG_DELIMITER = "|";

    /**
     * Constructor.
     */
    AwsCredentialsProviderPropertyValueDecoder() {}

    /**
     * Get AwsCredentialsProvider property.
     *
     * @param value
     *            property value as String
     * @return corresponding variable in correct type
     */
    @Override
    public AwsCredentialsProvider decodeValue(String value) {
        if (value != null) {
            List<String> providerNames = getProviderNames(value);
            List<AwsCredentialsProvider> providers = getValidCredentialsProviders(providerNames);
            AwsCredentialsProvider[] ps = new AwsCredentialsProvider[providers.size()];
            providers.toArray(ps);
            if (providers.isEmpty()) {
                log.warn("Unable to construct any provider with name {}", value);
                log.warn("Please verify that all AwsCredentialsProvider properties are passed correctly");
            }
            return AwsCredentialsProviderChain.builder()
                    .credentialsProviders(providers)
                    .build();
        } else {
            throw new IllegalArgumentException("Property AwsCredentialsProvider is missing.");
        }
    }

    /**
     * @return list of supported types
     */
    @Override
    public List<Class<AwsCredentialsProvider>> getSupportedTypes() {
        return Collections.singletonList(AwsCredentialsProvider.class);
    }

    /**
     * Convert string list to a list of valid credentials providers.
     */
    private static List<AwsCredentialsProvider> getValidCredentialsProviders(List<String> providerNames) {
        List<AwsCredentialsProvider> credentialsProviders = new ArrayList<>();

        for (String providerName : providerNames) {
            final String[] nameAndArgs = providerName.split("\\" + ARG_DELIMITER);
            final Class<? extends AwsCredentialsProvider> clazz = getClass(nameAndArgs[0]);
            if (clazz == null) {
                continue;
            }
            log.info("Attempting to construct {}", clazz);
            final String[] varargs =
                    nameAndArgs.length > 1 ? Arrays.copyOfRange(nameAndArgs, 1, nameAndArgs.length) : new String[0];
            AwsCredentialsProvider provider = tryConstructor(providerName, clazz, varargs);
            if (provider == null) {
                provider = tryCreate(providerName, clazz, varargs);
            }
            if (provider != null) {
                log.info("Provider constructed successfully: {}", provider);
                credentialsProviders.add(provider);
            }
        }
        return credentialsProviders;
    }

    private static AwsCredentialsProvider tryConstructor(
            String providerName, Class<? extends AwsCredentialsProvider> clazz, String[] varargs) {
        AwsCredentialsProvider provider =
                constructProvider(providerName, () -> getConstructorWithVarArgs(clazz, varargs));
        if (provider == null) {
            provider = constructProvider(providerName, () -> getConstructorWithArgs(clazz, varargs));
        }
        if (provider == null) {
            provider = constructProvider(providerName, clazz::newInstance);
        }
        return provider;
    }

    private static AwsCredentialsProvider tryCreate(
            String providerName, Class<? extends AwsCredentialsProvider> clazz, String[] varargs) {
        AwsCredentialsProvider provider =
                constructProvider(providerName, () -> getCreateMethod(clazz, (Object) varargs));
        if (provider == null) {
            provider = constructProvider(providerName, () -> getCreateMethod(clazz, varargs));
        }
        if (provider == null) {
            provider = constructProvider(providerName, () -> getCreateMethod(clazz));
        }
        return provider;
    }

    private static AwsCredentialsProvider getConstructorWithVarArgs(
            Class<? extends AwsCredentialsProvider> clazz, String[] varargs) {
        try {
            return clazz.getConstructor(String[].class).newInstance((Object) varargs);
        } catch (Exception e) {
            return null;
        }
    }

    private static AwsCredentialsProvider getConstructorWithArgs(
            Class<? extends AwsCredentialsProvider> clazz, String[] varargs) {
        try {
            Class<?>[] argTypes = new Class<?>[varargs.length];
            Arrays.fill(argTypes, String.class);
            return clazz.getConstructor(argTypes).newInstance((Object[]) varargs);
        } catch (Exception e) {
            return null;
        }
    }

    private static AwsCredentialsProvider getCreateMethod(
            Class<? extends AwsCredentialsProvider> clazz, Object... args) {
        try {
            Class<?>[] argTypes = new Class<?>[args.length];
            for (int i = 0; i < args.length; i++) {
                argTypes[i] = args[i].getClass();
            }
            Method createMethod = clazz.getDeclaredMethod("create", argTypes);
            if (Modifier.isStatic(createMethod.getModifiers())) {
                return clazz.cast(createMethod.invoke(null, args));
            } else {
                log.warn("Found non-static create() method in {}", clazz.getName());
            }
        } catch (NoSuchMethodException e) {
            // No matching create method found for class
        } catch (Exception e) {
            log.warn("Failed to invoke create() method in {}", clazz.getName(), e);
        }
        return null;
    }

    /**
     * Resolves the class for the given provider name.
     *
     * @param providerName A string containing the provider name.
     *
     * @return The Class object representing the resolved AwsCredentialsProvider implementation,
     * or null if the class cannot be resolved or does not extend AwsCredentialsProvider.
     */
    private static Class<? extends AwsCredentialsProvider> getClass(String providerName) {
        // Convert any form of StsAssumeRoleCredentialsProvider string to KclStsAssumeRoleCredentialsProvider
        if (providerName.equals(StsAssumeRoleCredentialsProvider.class.getSimpleName())
                || providerName.equals(StsAssumeRoleCredentialsProvider.class.getName())) {
            providerName = KclStsAssumeRoleCredentialsProvider.class.getName();
        }
        try {
            final Class<?> c = Class.forName(providerName);
            if (!AwsCredentialsProvider.class.isAssignableFrom(c)) {
                return null;
            }
            return (Class<? extends AwsCredentialsProvider>) c;
        } catch (ClassNotFoundException cnfe) {
            // Providers are a product of prefixed Strings to cover multiple
            // namespaces (e.g., "Foo" -> { "some.auth.Foo", "kcl.auth.Foo" }).
            // It's expected that many class names will not resolve.
            return null;
        }
    }

    private static List<String> getProviderNames(String property) {
        // assume list delimiter is ","
        String[] elements = property.split(LIST_DELIMITER);
        List<String> result = new ArrayList<>();
        for (int i = 0; i < elements.length; i++) {
            String string = elements[i].trim();
            if (!string.isEmpty()) {
                // find all possible names and add them to name list
                result.addAll(getPossibleFullClassNames(string));
            }
        }
        return result;
    }

    private static List<String> getPossibleFullClassNames(final String provider) {
        return Stream.of(
                        // Customer provides a short name of a provider offered by this multi-lang package
                        "software.amazon.kinesis.multilang.auth.",
                        // Customer provides a short name of common providers in software.amazon.awssdk.auth.credentials
                        // package (e.g., any classes implementing the AwsCredentialsProvider interface)
                        // @see
                        // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html
                        "software.amazon.awssdk.auth.credentials.",
                        // Customer provides a fully-qualified provider name, or a custom credentials provider
                        // (e.g., org.mycompany.FooProvider)
                        "")
                .map(prefix -> prefix + provider)
                .collect(Collectors.toList());
    }

    @FunctionalInterface
    private interface CredentialsProviderConstructor<T extends AwsCredentialsProvider> {
        T construct()
                throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException;
    }

    /**
     * Attempts to construct an {@link AwsCredentialsProvider}.
     *
     * @param providerName Raw, unmodified provider name. Should there be an
     *      Exception during construction, this parameter will be logged.
     * @param constructor supplier-like function that will perform the construction
     * @return the constructed provider, if successful; otherwise, null
     *
     * @param <T> type of the CredentialsProvider to construct
     */
    private static <T extends AwsCredentialsProvider> T constructProvider(
            final String providerName, final CredentialsProviderConstructor<T> constructor) {
        try {
            return constructor.construct();
        } catch (NoSuchMethodException
                | IllegalAccessException
                | InstantiationException
                | InvocationTargetException
                | RuntimeException ignored) {
            // ignore
        }
        return null;
    }
}
