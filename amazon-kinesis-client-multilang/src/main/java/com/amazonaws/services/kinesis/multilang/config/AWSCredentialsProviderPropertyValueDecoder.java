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
package com.amazonaws.services.kinesis.multilang.config;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;

/**
 * Get AWSCredentialsProvider property.
 */
@Slf4j
class AWSCredentialsProviderPropertyValueDecoder implements IPropertyValueDecoder<AwsCredentialsProvider> {
    private static final String AUTH_PREFIX = "com.amazonaws.auth.";
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
    public AwsCredentialsProvider decodeValue(String value) {
        if (value != null) {
            List<String> providerNames = getProviderNames(value);
            List<AwsCredentialsProvider> providers = getValidCredentialsProviders(providerNames);
            AwsCredentialsProvider[] ps = new AwsCredentialsProvider[providers.size()];
            providers.toArray(ps);
            return AwsCredentialsProviderChain.builder().credentialsProviders(ps).build();
        } else {
            throw new IllegalArgumentException("Property AWSCredentialsProvider is missing.");
        }
    }

    /**
     * @return list of supported types
     */
    @Override
    public List<Class<AwsCredentialsProvider>> getSupportedTypes() {
        return Arrays.asList(AwsCredentialsProvider.class);
    }

    /*
     * Convert string list to a list of valid credentials providers.
     */
    private static List<AwsCredentialsProvider> getValidCredentialsProviders(List<String> providerNames) {
        List<AwsCredentialsProvider> credentialsProviders = new ArrayList<AwsCredentialsProvider>();
        for (String providerName : providerNames) {
            if (providerName.contains(ARG_DELIMITER)) {
                String[] nameAndArgs = providerName.split("\\" + ARG_DELIMITER);
                Class<?>[] argTypes = new Class<?>[nameAndArgs.length - 1];
                Arrays.fill(argTypes, String.class);
                try {
                    Class<?> className = Class.forName(nameAndArgs[0]);
                    Constructor<?> c = className.getConstructor(argTypes);
                    credentialsProviders.add((AwsCredentialsProvider) c
                            .newInstance(Arrays.copyOfRange(nameAndArgs, 1, nameAndArgs.length)));
                } catch (Exception e) {
                    log.debug("Can't find any credentials provider matching {}.", providerName);
                }
            } else {
                try {
                    Class<?> className = Class.forName(providerName);
                    credentialsProviders.add((AwsCredentialsProvider) className.newInstance());
                } catch (Exception e) {
                    log.debug("Can't find any credentials provider matching {}.", providerName);
                }
            }
        }
        return credentialsProviders;
    }

    private static List<String> getProviderNames(String property) {
        // assume list delimiter is ","
        String[] elements = property.split(LIST_DELIMITER);
        List<String> result = new ArrayList<String>();
        for (int i = 0; i < elements.length; i++) {
            String string = elements[i].trim();
            if (!string.isEmpty()) {
                // find all possible names and add them to name list
                result.addAll(getPossibleFullClassNames(string));
            }
        }
        return result;
    }

    private static List<String> getPossibleFullClassNames(String s) {
        /*
         * We take care of three cases :
         *
         * 1. Customer provides a short name of common providers in com.amazonaws.auth package i.e. any classes
         * implementing the AWSCredentialsProvider interface:
         * http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html
         *
         * 2. Customer provides a full name of common providers e.g. com.amazonaws.auth.ClasspathFileCredentialsProvider
         *
         * 3. Customer provides a custom credentials provider with full name of provider
         */

        return Arrays.asList(s, AUTH_PREFIX + s);
    }

}
