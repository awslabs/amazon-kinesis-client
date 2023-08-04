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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import lombok.extern.slf4j.Slf4j;

/**
 * Get AWSCredentialsProvider property.
 */
@Slf4j
class AWSCredentialsProviderPropertyValueDecoder implements IPropertyValueDecoder<AWSCredentialsProvider> {
    private static final String LIST_DELIMITER = ",";
    private static final String ARG_DELIMITER = "|";

    /**
     * Constructor.
     */
    AWSCredentialsProviderPropertyValueDecoder() {
    }

    /**
     * Get AWSCredentialsProvider property.
     *
     * @param value
     *            property value as String
     * @return corresponding variable in correct type
     */
    @Override
    public AWSCredentialsProvider decodeValue(String value) {
        if (value != null) {
            List<String> providerNames = getProviderNames(value);
            List<AWSCredentialsProvider> providers = getValidCredentialsProviders(providerNames);
            AWSCredentialsProvider[] ps = new AWSCredentialsProvider[providers.size()];
            providers.toArray(ps);
            return new AWSCredentialsProviderChain(providers);
        } else {
            throw new IllegalArgumentException("Property AWSCredentialsProvider is missing.");
        }
    }

    /**
     * @return list of supported types
     */
    @Override
    public List<Class<AWSCredentialsProvider>> getSupportedTypes() {
        return Collections.singletonList(AWSCredentialsProvider.class);
    }

    /**
     * Convert string list to a list of valid credentials providers.
     */
    private static List<AWSCredentialsProvider> getValidCredentialsProviders(List<String> providerNames) {
        List<AWSCredentialsProvider> credentialsProviders = new ArrayList<>();

        for (String providerName : providerNames) {
            final String[] nameAndArgs = providerName.split("\\" + ARG_DELIMITER);
            final Class<? extends AWSCredentialsProvider> clazz;
            try {
                final Class<?> c = Class.forName(nameAndArgs[0]);
                if (!AWSCredentialsProvider.class.isAssignableFrom(c)) {
                    continue;
                }
                clazz = (Class<? extends AWSCredentialsProvider>) c;
            } catch (ClassNotFoundException cnfe) {
                // Providers are a product of prefixed Strings to cover multiple
                // namespaces (e.g., "Foo" -> { "some.auth.Foo", "kcl.auth.Foo" }).
                // It's expected that many class names will not resolve.
                continue;
            }
            log.info("Attempting to construct {}", clazz);

            AWSCredentialsProvider provider = null;
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
                    provider = constructProvider(providerName, () ->
                        clazz.getConstructor(String[].class).newInstance((Object) varargs)
                    );
                }
            }

            if (provider == null) {
                // regardless of parameters, fallback to invoke a public no-arg constructor
                provider = constructProvider(providerName, clazz::newInstance);
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
                // Customer provides a short name of common providers in com.amazonaws.auth package
                // (e.g., any classes implementing the AWSCredentialsProvider interface)
                // @see http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html
                "com.amazonaws.auth.",

                // Customer provides a short name of a provider offered by this multi-lang package
                "software.amazon.kinesis.multilang.auth.",

                // Customer provides a fully-qualified provider name, or a custom credentials provider
                // (e.g., com.amazonaws.auth.ClasspathFileCredentialsProvider, org.mycompany.FooProvider)
                ""
        ).map(prefix -> prefix + provider).collect(Collectors.toList());
    }

    @FunctionalInterface
    private interface CredentialsProviderConstructor<T extends AWSCredentialsProvider> {
        T construct() throws IllegalAccessException, InstantiationException,
                InvocationTargetException, NoSuchMethodException;
    }

    /**
     * Attempts to construct an {@link AWSCredentialsProvider}.
     *
     * @param providerName Raw, unmodified provider name. Should there be an
     *      Exeception during construction, this parameter will be logged.
     * @param constructor supplier-like function that will perform the construction
     * @return the constructed provider, if successful; otherwise, null
     *
     * @param <T> type of the CredentialsProvider to construct
     */
    private static <T extends AWSCredentialsProvider> T constructProvider(
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
