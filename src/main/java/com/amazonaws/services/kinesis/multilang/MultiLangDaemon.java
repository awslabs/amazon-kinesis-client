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
package com.amazonaws.services.kinesis.multilang;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfigurator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * Main app that launches the worker that runs the multi-language record processor.
 * 
 * Requires a properties file containing configuration for this daemon and the KCL. A properties file should at minimum
 * define these properties:
 * 
 * <pre>
 * # The script that abides by the multi-language protocol. This script will
 * # be executed by the MultiLangDaemon, which will communicate with this script
 * # over STDIN and STDOUT according to the multi-language protocol.
 * executableName = sampleapp.py
 * 
 * # The name of an Amazon Kinesis stream to process.
 * streamName = words
 * 
 * # Used by the KCL as the name of this application. Will be used as the name
 * # of a Amazon DynamoDB table which will store the lease and checkpoint
 * # information for workers with this application name.
 * applicationName = PythonKCLSample
 * 
 * # Users can change the credentials provider the KCL will use to retrieve credentials.
 * # The DefaultAWSCredentialsProviderChain checks several other providers, which is
 * # described here:
 * # http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html
 * AWSCredentialsProvider = DefaultAWSCredentialsProviderChain
 * </pre>
 */
public class MultiLangDaemon implements Callable<Integer> {

    private static final Log LOG = LogFactory.getLog(MultiLangDaemon.class);

    private static final String USER_AGENT = "amazon-kinesis-multi-lang-daemon";
    private static final String VERSION = "1.0.0";

    private static final String PROP_EXECUTABLE_NAME = "executableName";
    private static final String PROP_PROCESSING_LANGUAGE = "processingLanguage";
    private static final String PROP_MAX_ACTIVE_THREADS = "maxActiveThreads";

    private KinesisClientLibConfiguration configuration;

    private MultiLangRecordProcessorFactory recordProcessorFactory;

    private ExecutorService workerThreadPool;

    private String processingLanguage;

    /**
     * Constructor.
     */
    MultiLangDaemon(String processingLanguage,
            KinesisClientLibConfiguration configuration,
            MultiLangRecordProcessorFactory recordProcessorFactory,
            ExecutorService workerThreadPool) {
        this.processingLanguage = processingLanguage;
        this.configuration = configuration;
        this.recordProcessorFactory = recordProcessorFactory;
        this.workerThreadPool = workerThreadPool;
    }

    static void printUsage(PrintStream stream, String message) {
        StringBuilder builder = new StringBuilder();
        if (message != null) {
            builder.append(message);
        }
        builder.append(String.format("java %s <properties file>", MultiLangDaemon.class.getCanonicalName()));
        stream.println(builder.toString());
    }

    static Properties loadProperties(ClassLoader classLoader, String propertiesFileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream propertiesStream = classLoader.getResourceAsStream(propertiesFileName)) {
            properties.load(propertiesStream);
            return properties;
        }
    }

    static boolean validateProperties(Properties properties) {
        return properties != null && properties.getProperty(PROP_EXECUTABLE_NAME) != null;
    }

    /**
     * This method will cause the MultiLangDaemon to read its configuration and build a worker with a
     * MultiLangRecordProcessorFactory for the executable specified in the provided properties.
     */
    void prepare() {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        LOG.info("Using workerId: " + configuration.getWorkerIdentifier());
        LOG.info("Using credentials with access key id: "
                + configuration.getKinesisCredentialsProvider().getCredentials().getAWSAccessKeyId());

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

        LOG.debug(String.format("User Agent string is: %s", userAgent.toString()));
        configuration.withUserAgent(userAgent.toString());
    }

    @Override
    public Integer call() throws Exception {
        prepare();
        Worker worker = new Worker(recordProcessorFactory, configuration, workerThreadPool);
        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        return exitCode;
    }

    private static int getMaxActiveThreads(Properties properties) {
        return Integer.parseInt(properties.getProperty(PROP_MAX_ACTIVE_THREADS, "0"));
    }

    private static ExecutorService getExecutorService(Properties properties) {
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
     * @param args Accepts a single argument, that argument is a properties file which provides KCL configuration as
     *        well as the name of an executable.
     */
    public static void main(String[] args) {

        if (args.length == 0) {
            printUsage(System.err, "You must provide a properties file");
            System.exit(1);
        }
        Properties properties = null;
        try {
            properties = loadProperties(Thread.currentThread().getContextClassLoader(), args[0]);
        } catch (IOException e) {
            printUsage(System.err, "You must provide a properties file");
            System.exit(1);
        }

        if (validateProperties(properties)) {

            // Configuration
            KinesisClientLibConfiguration kinesisClientLibConfiguration =
                    new KinesisClientLibConfigurator().getConfiguration(properties);
            String executableName = properties.getProperty(PROP_EXECUTABLE_NAME);

            ExecutorService executorService = getExecutorService(properties);

            // Factory
            MultiLangRecordProcessorFactory recordProcessorFactory =
                    new MultiLangRecordProcessorFactory(executableName, executorService);

            // Daemon
            MultiLangDaemon daemon =
                    new MultiLangDaemon(properties.getProperty(PROP_PROCESSING_LANGUAGE),
                            kinesisClientLibConfiguration, recordProcessorFactory, executorService);

            LOG.info("Running " + kinesisClientLibConfiguration.getApplicationName() + " to process stream "
                    + kinesisClientLibConfiguration.getStreamName() + " with executable " + executableName);

            Future<Integer> future = executorService.submit(daemon);
            try {
                System.exit(future.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Encountered an error while running daemon", e);
            }
        } else {
            printUsage(System.err, "Must provide an executable name in the properties file, "
                    + "e.g. executableName = sampleapp.py");
        }
        System.exit(1);
    }
}
