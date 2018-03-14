/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
 * http://aws.amazon.com/asl/ or in the "license" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.multilang;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.multilang.config.KinesisClientLibConfigurator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.extern.slf4j.Slf4j;

/**
 * This class captures the configuration needed to run the MultiLangDaemon.
 */
@Slf4j
public class MultiLangDaemonConfig {
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
     * @param propertiesFile
     *            The location of the properties file.
     * @throws IOException
     *             Thrown when the properties file can't be accessed.
     * @throws IllegalArgumentException
     *             Thrown when the contents of the properties file are not as expected.
     */
    public MultiLangDaemonConfig(String propertiesFile) throws IOException, IllegalArgumentException {
        this(propertiesFile, Thread.currentThread().getContextClassLoader());
    }

    /**
     * 
     * @param propertiesFile
     *            The location of the properties file.
     * @param classLoader
     *            A classloader, useful if trying to programmatically configure with the daemon, such as in a unit test.
     * @throws IOException
     *             Thrown when the properties file can't be accessed.
     * @throws IllegalArgumentException
     *             Thrown when the contents of the properties file are not as expected.
     */
    public MultiLangDaemonConfig(String propertiesFile, ClassLoader classLoader)
            throws IOException, IllegalArgumentException {
        this(propertiesFile, classLoader, new KinesisClientLibConfigurator());
    }

    /**
     * 
     * @param propertiesFile
     *            The location of the properties file.
     * @param classLoader
     *            A classloader, useful if trying to programmatically configure with the daemon, such as in a unit test.
     * @param configurator
     *            A configurator to use.
     * @throws IOException
     *             Thrown when the properties file can't be accessed.
     * @throws IllegalArgumentException
     *             Thrown when the contents of the properties file are not as expected.
     */
    public MultiLangDaemonConfig(String propertiesFile, ClassLoader classLoader,
            KinesisClientLibConfigurator configurator) throws IOException, IllegalArgumentException {
        Properties properties = loadProperties(classLoader, propertiesFile);
        if (!validateProperties(properties)) {
            throw new IllegalArgumentException(
                    "Must provide an executable name in the properties file, " + "e.g. executableName = sampleapp.py");
        }

        String executableName = properties.getProperty(PROP_EXECUTABLE_NAME);
        String processingLanguage = properties.getProperty(PROP_PROCESSING_LANGUAGE);

        kinesisClientLibConfig = configurator.getConfiguration(properties);
        executorService = buildExecutorService(properties);
        recordProcessorFactory = new MultiLangRecordProcessorFactory(executableName, executorService,
                kinesisClientLibConfig);

        log.info("Running {} to process stream {} with executable {}", kinesisClientLibConfig.getApplicationName(),
                kinesisClientLibConfig.getStreamName(), executableName);
        prepare(processingLanguage);
    }

    private void prepare(String processingLanguage) {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        log.info("Using workerId: {}", kinesisClientLibConfig.getWorkerIdentifier());
        log.info("Using credentials with access key id: {}",
                kinesisClientLibConfig.getKinesisCredentialsProvider().getCredentials().getAWSAccessKeyId());

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

        log.info("MultiLangDaemon is adding the following fields to the User Agent: {}", userAgent.toString());
        kinesisClientLibConfig.withUserAgent(userAgent.toString());
    }

    private static Properties loadProperties(ClassLoader classLoader, String propertiesFileName) throws IOException {
        Properties properties = new Properties();
        InputStream propertyStream = null;
        try {
            propertyStream = classLoader.getResourceAsStream(propertiesFileName);
            if (propertyStream == null) {
                File propertyFile = new File(propertiesFileName);
                if (propertyFile.exists()) {
                    propertyStream = new FileInputStream(propertyFile);
                }
            }

            if (propertyStream == null) {
                throw new FileNotFoundException(
                        "Unable to find property file in classpath, or file system: '" + propertiesFileName + "'");
            }

            properties.load(propertyStream);
            return properties;
        } finally {
            if (propertyStream != null) {
                propertyStream.close();
            }
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
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder().setNameFormat("multi-lang-daemon-%04d");
        log.debug("Value for {} property is {}", PROP_MAX_ACTIVE_THREADS, maxActiveThreads);
        if (maxActiveThreads <= 0) {
            log.info("Using a cached thread pool.");
            return new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                    builder.build());
        } else {
            log.info("Using a fixed thread pool with {} max active threads.", maxActiveThreads);
            return new ThreadPoolExecutor(maxActiveThreads, maxActiveThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(), builder.build());
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
