/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.multilang.config;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * KinesisClientLibConfigurator constructs a KinesisClientLibConfiguration from java properties file. The following
 * three properties must be provided. 1) "applicationName" 2) "streamName" 3) "AWSCredentialsProvider"
 * KinesisClientLibConfigurator will help to automatically assign the value of "workerId" if this property is not
 * provided. In the specified properties file, any properties, which matches the variable name in
 * KinesisClientLibConfiguration and has a corresponding "with{variableName}" setter method, will be read in, and its
 * value in properties file will be assigned to corresponding variable in KinesisClientLibConfiguration.
 */
@Slf4j
public class KinesisClientLibConfigurator {
    private static final String PREFIX = "with";

    // Required properties
    private static final String PROP_APP_NAME = "applicationName";
    private static final String PROP_STREAM_NAME = "streamName";
    private static final String PROP_CREDENTIALS_PROVIDER_KINESIS = "AWSCredentialsProvider";
    private static final String PROP_CREDENTIALS_PROVIDER_DYNAMODB = "AWSCredentialsProviderDynamoDB";
    private static final String PROP_CREDENTIALS_PROVIDER_CLOUDWATCH = "AWSCredentialsProviderCloudWatch";
    private static final String PROP_WORKER_ID = "workerId";

    private Map<Class<?>, IPropertyValueDecoder<?>> classToDecoder;
    private Map<String, List<Method>> nameToMethods;

    /**
     * Constructor.
     */
    public KinesisClientLibConfigurator() {
        List<IPropertyValueDecoder<? extends Object>> getters =
                Arrays.asList(new IntegerPropertyValueDecoder(),
                        new LongPropertyValueDecoder(),
                        new BooleanPropertyValueDecoder(),
                        new DatePropertyValueDecoder(),
                        new AWSCredentialsProviderPropertyValueDecoder(),
                        new StringPropertyValueDecoder(),
                        new InitialPositionInStreamPropertyValueDecoder(),
                        new SetPropertyValueDecoder());

        classToDecoder = new Hashtable<>();
        for (IPropertyValueDecoder<?> getter : getters) {
            for (Class<?> clazz : getter.getSupportedTypes()) {
                /*
                 * We could validate that we never overwrite a getter but we can also do this by manual inspection of
                 * the getters.
                 */
                classToDecoder.put(clazz, getter);
            }
        }
        nameToMethods = new Hashtable<>();
        for (Method method : KinesisClientLibConfiguration.class.getMethods()) {
            if (!nameToMethods.containsKey(method.getName())) {
                nameToMethods.put(method.getName(), new ArrayList<>());
            }
            nameToMethods.get(method.getName()).add(method);
        }
    }

    /**
     * Return a KinesisClientLibConfiguration with variables configured as specified by the properties in config stream.
     * Program will fail immediately, if customer provide: 1) invalid variable value. Program will log it as warning and
     * continue, if customer provide: 1) variable with unsupported variable type. 2) a variable with name which does not
     * match any of the variables in KinesisClientLibConfigration.
     * 
     * @param properties a Properties object containing the configuration information
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration getConfiguration(Properties properties) {
        // The three minimum required arguments for constructor are obtained first. They are all mandatory, all of them
        // should be provided. If any of these three failed to be set, program will fail.
        IPropertyValueDecoder<String> stringValueDecoder = new StringPropertyValueDecoder();
        IPropertyValueDecoder<AwsCredentialsProvider> awsCPPropGetter =
                new AWSCredentialsProviderPropertyValueDecoder();
        String applicationName = stringValueDecoder.decodeValue(properties.getProperty(PROP_APP_NAME));
        String streamName = stringValueDecoder.decodeValue(properties.getProperty(PROP_STREAM_NAME));
        AwsCredentialsProvider provider =
                awsCPPropGetter.decodeValue(properties.getProperty(PROP_CREDENTIALS_PROVIDER_KINESIS));

        if (applicationName == null || applicationName.isEmpty()) {
            throw new IllegalArgumentException("Value of applicationName should be explicitly provided.");
        }
        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("Value of streamName should be explicitly provided.");
        }

        // Decode the DynamoDB credentials provider if it exists.  If not use the Kinesis credentials provider.
        AwsCredentialsProvider providerDynamoDB;
        String propCredentialsProviderDynamoDBValue = properties.getProperty(PROP_CREDENTIALS_PROVIDER_DYNAMODB);
        if (propCredentialsProviderDynamoDBValue == null) {
            providerDynamoDB = provider;
        } else {
            providerDynamoDB = awsCPPropGetter.decodeValue(propCredentialsProviderDynamoDBValue);
        }

        // Decode the CloudWatch credentials provider if it exists.  If not use the Kinesis credentials provider.
        AwsCredentialsProvider providerCloudWatch;
        String propCredentialsProviderCloudWatchValue = properties.getProperty(PROP_CREDENTIALS_PROVIDER_CLOUDWATCH);
        if (propCredentialsProviderCloudWatchValue == null) {
            providerCloudWatch = provider;
        } else {
            providerCloudWatch = awsCPPropGetter.decodeValue(propCredentialsProviderCloudWatchValue);
        }

        // Allow customer not to provide workerId or to provide empty worker id.
        String workerId = stringValueDecoder.decodeValue(properties.getProperty(PROP_WORKER_ID));
        if (workerId == null || workerId.isEmpty()) {
            workerId = UUID.randomUUID().toString();
            log.info("Value of workerId is not provided in the properties. WorkerId is automatically assigned as: {}",
                    workerId);
        }

        KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration(applicationName, streamName, provider, providerDynamoDB, providerCloudWatch, workerId);

        Set<String> requiredNames =
                new HashSet<String>(Arrays.asList(PROP_STREAM_NAME,
                        PROP_APP_NAME,
                        PROP_WORKER_ID,
                        PROP_CREDENTIALS_PROVIDER_KINESIS));

        // Set all the variables that are not used for constructor.
        for (Object keyObject : properties.keySet()) {
            String key = keyObject.toString();
            if (!requiredNames.contains(key)) {
                withProperty(key, properties, config);
            }
        }

        return config;
    }

    /**
     * @param configStream the input stream containing the configuration information
     * @return KinesisClientLibConfiguration
     */
    public KinesisClientLibConfiguration getConfiguration(InputStream configStream) {
        Properties properties = new Properties();
        try {
            properties.load(configStream);
        } catch (IOException e) {
            String msg = "Could not load properties from the stream provided";
            throw new IllegalStateException(msg, e);
        } finally {
            try {
                configStream.close();
            } catch (IOException e) {
                String msg = "Encountered error while trying to close properties file.";
                throw new IllegalStateException(msg, e);
            }
        }
        return getConfiguration(properties);
    }

    private void withProperty(String propertyKey, Properties properties, KinesisClientLibConfiguration config) {
        if (propertyKey.isEmpty()) {
            throw new IllegalArgumentException("The property can't be empty string");
        }
        // Assume that all the setters in KinesisClientLibConfiguration are in the following format
        // They all start with "with" followed by the variable name with first letter capitalized
        String targetMethodName = PREFIX + Character.toUpperCase(propertyKey.charAt(0)) + propertyKey.substring(1);
        String propertyValue = properties.getProperty(propertyKey);
        if (nameToMethods.containsKey(targetMethodName)) {
            for (Method method : nameToMethods.get(targetMethodName)) {
                if (method.getParameterTypes().length == 1 && method.getName().equals(targetMethodName)) {
                    Class<?> paramType = method.getParameterTypes()[0];
                    if (classToDecoder.containsKey(paramType)) {
                        IPropertyValueDecoder<?> decoder = classToDecoder.get(paramType);
                        try {
                            method.invoke(config, decoder.decodeValue(propertyValue));
                            log.info("Successfully set property {} with value {}", propertyKey, propertyValue);
                            return;
                        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                            // At this point, we really thought that we could call this method.
                            log.warn("Encountered an error while invoking method %s with value {}. Exception was {}",
                                    method, propertyValue, e);
                        } catch (UnsupportedOperationException e) {
                            log.warn("The property {} is not supported as type {} at this time.", propertyKey,
                                    paramType);
                        }
                    } else {
                        log.debug("No method for decoding parameters of type {} so method {} could not be invoked.",
                                paramType, method);
                    }
                } else {
                    log.debug("Method {} doesn't look like it is appropriate for setting property {}. Looking for"
                                    + " something called {}.", method, propertyKey, targetMethodName);
                }
            }
        } else {
            log.debug(String.format("There was no appropriately named method for setting property %s.", propertyKey));
        }
    }
}
