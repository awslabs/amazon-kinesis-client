/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.multilang;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfigurator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

/**
 * This class captures the configuration needed to run the MultiLangDaemon.
 */
public class MultiLangDaemonConfig {

    private static final Log LOG = LogFactory.getLog(MultiLangDaemonConfig.class);

    private static final String USER_AGENT = "amazon-kinesis-multi-lang-daemon";
    private static final String VERSION = "1.0.1";

    private static final String PROP_EXECUTABLE_NAME = "executableName";
    private static final String PROP_PROCESSING_LANGUAGE = "processingLanguage";
    private static final String PROP_MAX_ACTIVE_THREADS = "maxActiveThreads";

    private KinesisClientLibConfiguration kinesisClientLibConfig;

    private ExecutorService executorService;

    private MultiLangRecordProcessorFactory recordProcessorFactory;

    /**
     * Constructor.
     * 
     * @param propertiesFile The location of the properties file.
     * @throws IOException Thrown when the properties file can't be accessed.
     * @throws IllegalArgumentException Thrown when the contents of the properties file are not as expected.
     */
    public MultiLangDaemonConfig(String propertiesFile) throws IOException, IllegalArgumentException {
        this(propertiesFile, Thread.currentThread().getContextClassLoader());
    }

    /**
     * 
     * @param propertiesFile The location of the properties file.
     * @param classLoader A classloader, useful if trying to programmatically configure with the daemon, such as in a
     *        unit test.
     * @throws IOException Thrown when the properties file can't be accessed.
     * @throws IllegalArgumentException Thrown when the contents of the properties file are not as expected.
     */
    public MultiLangDaemonConfig(String propertiesFile, ClassLoader classLoader) throws IOException,
            IllegalArgumentException {
        this(propertiesFile, classLoader, new KinesisClientLibConfigurator());
    }

    /**
     * 
     * @param propertiesFile The location of the properties file.
     * @param classLoader A classloader, useful if trying to programmatically configure with the daemon, such as in a
     *        unit test.
     * @param configurator A configurator to use.
     * @throws IOException Thrown when the properties file can't be accessed.
     * @throws IllegalArgumentException Thrown when the contents of the properties file are not as expected.
     */
    public MultiLangDaemonConfig(String propertiesFile,
            ClassLoader classLoader,
            KinesisClientLibConfigurator configurator) throws IOException, IllegalArgumentException {
        Properties properties = loadProperties(classLoader, propertiesFile);
        if (!validateProperties(properties)) {
            throw new IllegalArgumentException("Must provide an executable name in the properties file, "
                    + "e.g. executableName = sampleapp.py");
        }

        String executableName = properties.getProperty(PROP_EXECUTABLE_NAME);
        String processingLanguage = properties.getProperty(PROP_PROCESSING_LANGUAGE);

        kinesisClientLibConfig = configurator.getConfiguration(properties);
        executorService = buildExecutorService(properties);
        recordProcessorFactory = new MultiLangRecordProcessorFactory(executableName, executorService);

        LOG.info("Running " + kinesisClientLibConfig.getApplicationName() + " to process stream "
                + kinesisClientLibConfig.getStreamName() + " with executable " + executableName);
        prepare(processingLanguage);
    }

    private void prepare(String processingLanguage) {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        LOG.info("Using workerId: " + kinesisClientLibConfig.getWorkerIdentifier());
        LOG.info("Using credentials with access key id: "
                + kinesisClientLibConfig.getKinesisCredentialsProvider().getCredentials().getAWSAccessKeyId());

        StringBuilder userAgent = new StringBuilder(KinesisClientLibConfiguration.KINESIS_CLIENT_LIB_USER_AGENT);
        userAgent.append(" ");
        userAgent.append(USER_AGENT);
        userAgent.append("/");
        userAgent.append(VERSION);

        if (processingLanguage != null) {
            userAgent.append(" ");
            userAgent.append(processingLanguage);
        }

        if (recordProcessorFactory.getCommandArray().length > 0) {
            userAgent.append(" ");
            userAgent.append(recordProcessorFactory.getCommandArray()[0]);
        }

        LOG.info(String.format("MultiLangDaemon is adding the following fields to the User Agent: %s",
                userAgent.toString()));
        kinesisClientLibConfig.withUserAgent(userAgent.toString());
    }

    private static Properties loadProperties(ClassLoader classLoader, String propertiesFileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream propertiesStream = classLoader.getResourceAsStream(propertiesFileName)) {
            properties.load(propertiesStream);
            return properties;
        }
    }

    private static boolean validateProperties(Properties properties) {
        return properties != null && properties.getProperty(PROP_EXECUTABLE_NAME) != null;
    }

    private static int getMaxActiveThreads(Properties properties) {
        return Integer.parseInt(properties.getProperty(PROP_MAX_ACTIVE_THREADS, "0"));
    }

    private static ExecutorService buildExecutorService(Properties properties) {
        int maxActiveThreads = getMaxActiveThreads(properties);
        LOG.debug(String.format("Value for %s property is %d", PROP_MAX_ACTIVE_THREADS, maxActiveThreads));
        if (maxActiveThreads <= 0) {
            LOG.info("Using a cached thread pool.");
            return Executors.newCachedThreadPool();
        } else {
            LOG.info(String.format("Using a fixed thread pool with %d max active threads.", maxActiveThreads));
            return Executors.newFixedThreadPool(maxActiveThreads);
        }
    }

    /**
     * 
     * @return A KinesisClientLibConfiguration object based on the properties file provided.
     */
    public KinesisClientLibConfiguration getKinesisClientLibConfiguration() {
        return kinesisClientLibConfig;
    }

    /**
     * 
     * @return An executor service based on the properties file provided.
     */
    public ExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * 
     * @return A MultiLangRecordProcessorFactory based on the properties file provided.
     */
    public MultiLangRecordProcessorFactory getRecordProcessorFactory() {
        return recordProcessorFactory;
    }
}
