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

/**
 * Get AWSCredentialsProvider property.
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
     * Get AWSCredentialsProvider property.
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
            return AwsCredentialsProviderChain.builder()
                    .credentialsProviders(providers)
                    .build();
        } else {
            throw new IllegalArgumentException("Property AWSCredentialsProvider is missing.");
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
            final Class<? extends AwsCredentialsProvider> clazz;
            try {
                final Class<?> c = Class.forName(nameAndArgs[0]);
                if (!AwsCredentialsProvider.class.isAssignableFrom(c)) {
                    continue;
                }
                clazz = (Class<? extends AwsCredentialsProvider>) c;
            } catch (ClassNotFoundException cnfe) {
                // Providers are a product of prefixed Strings to cover multiple
                // namespaces (e.g., "Foo" -> { "some.auth.Foo", "kcl.auth.Foo" }).
                // It's expected that many class names will not resolve.
                continue;
            }
            log.info("Attempting to construct {}", clazz);

            AwsCredentialsProvider provider = null;
            if (nameAndArgs.length > 1) {
                final String[] varargs = Arrays.copyOfRange(nameAndArgs, 1, nameAndArgs.length);

                // attempt to invoke an explicit N-arg constructor of FooClass(String, String, ...)
                provider = constructProvider(providerName, () -> {
                    Class<?>[] argTypes = new Class<?>[nameAndArgs.length - 1];
                    Arrays.fill(argTypes, String.class);
                    return clazz.getConstructor(argTypes).newInstance(varargs);
                });

                if (provider == null) {
                    // attempt to invoke a public varargs/array constructor of FooClass(String[])
                    provider = constructProvider(providerName, () -> clazz.getConstructor(String[].class)
                            .newInstance((Object) varargs));
                }
            }

            if (provider == null) {
                // regardless of parameters, fallback to invoke a public no-arg constructor
                provider = constructProvider(providerName, clazz::newInstance);
            }

            if (provider == null) {
                // if still not found, try empty create() method
                try {
                    Method createMethod = clazz.getDeclaredMethod("create");
                    if (Modifier.isStatic(createMethod.getModifiers())) {
                        provider = constructProvider(providerName, () -> clazz.cast(createMethod.invoke(null)));
                    } else {
                        log.warn("Found non-static create() method in {}", providerName);
                    }
                } catch (NoSuchMethodException e) {
                    // No create() method found for class
                }
            }

            if (provider != null) {
                credentialsProviders.add(provider);
            }
        }
        return credentialsProviders;
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
                        // package (e.g., any classes implementing the AWSCredentialsProvider interface)
                        // @see
                        // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html
                        "software.amazon.awssdk.auth.credentials.",
                        // Customer provides a fully-qualified provider name, or a custom credentials provider
                        // (e.g., software.amazon.awssdk.auth.credentials.AwsCredentialsProvider)
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
        } catch (NoSuchMethodException ignored) {
            // ignore
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException | RuntimeException e) {
            log.warn("Failed to construct {}", providerName, e);
        }
        return null;
    }
}
