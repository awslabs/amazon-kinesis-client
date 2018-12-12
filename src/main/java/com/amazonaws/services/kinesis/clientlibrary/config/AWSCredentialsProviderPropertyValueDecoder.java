/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;

/**
 * Get AWSCredentialsProvider property.
 */
class AWSCredentialsProviderPropertyValueDecoder implements IPropertyValueDecoder<AWSCredentialsProvider> {
    private static final Log LOG = LogFactory.getLog(AWSCredentialsProviderPropertyValueDecoder.class);
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
     * @param value property value as String
     * @return corresponding variable in correct type
     */
    @Override
    public AWSCredentialsProvider decodeValue(String value) {
        if (value != null) {
            List<String> providerNames = getProviderNames(value);
            List<AWSCredentialsProvider> providers = getValidCredentialsProviders(providerNames);
            AWSCredentialsProvider[] ps = new AWSCredentialsProvider[providers.size()];
            providers.toArray(ps);
            return new AWSCredentialsProviderChain(ps);
        } else {
            throw new IllegalArgumentException("Property AWSCredentialsProvider is missing.");
        }
    }

    /**
     * @return list of supported types
     */
    @Override
    public List<Class<AWSCredentialsProvider>> getSupportedTypes() {
        return Arrays.asList(AWSCredentialsProvider.class);
    }

    /*
     * Convert string list to a list of valid credentials providers.
     */
    private static List<AWSCredentialsProvider> getValidCredentialsProviders(List<String> providerNames) {
        List<AWSCredentialsProvider> credentialsProviders = new ArrayList<AWSCredentialsProvider>();
        for (String providerName : providerNames) {
            if (providerName.contains(ARG_DELIMITER)) {
              String[] nameAndArgs = providerName.split("\\" + ARG_DELIMITER);
              Class<?>[] argTypes = new Class<?>[nameAndArgs.length - 1];
              Arrays.fill(argTypes, String.class);
              try {
                  Class<?> className = Class.forName(nameAndArgs[0]);
                  Constructor<?> c = className.getConstructor(argTypes);
                  credentialsProviders.add((AWSCredentialsProvider) c.newInstance(
                        Arrays.copyOfRange(nameAndArgs, 1, nameAndArgs.length)));
              } catch (Exception e) {
                  LOG.debug("Can't find any credentials provider matching " + providerName + ".");
              }
            } else {
              try {
                  Class<?> className = Class.forName(providerName);
                  credentialsProviders.add((AWSCredentialsProvider) className.newInstance());
              } catch (Exception e) {
                  LOG.debug("Can't find any credentials provider matching " + providerName + ".");
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
