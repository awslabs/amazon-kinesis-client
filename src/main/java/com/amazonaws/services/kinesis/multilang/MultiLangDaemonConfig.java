/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.multilang;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.config.KinesisClientLibConfigurator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    public static final String PROXY_HOST_PROP = "http.proxyHost";
    public static final String PROXY_PORT_PROP = "http.proxyPort";
    public static final String HTTP_PROXY_ENV_VAR = "HTTP_PROXY";

    private KinesisClientLibConfiguration kinesisClientLibConfig;

    private ExecutorService executorService;

    private MultiLangRecordProcessorFactory recordProcessorFactory;

    /**
     * Constructor.
     *
     * @param propertiesFile The location of the properties file.
     * @throws IOException              Thrown when the properties file can't be accessed.
     * @throws IllegalArgumentException Thrown when the contents of the properties file are not as expected.
     */
    public MultiLangDaemonConfig(String propertiesFile) throws IOException, IllegalArgumentException {
        this(propertiesFile, Thread.currentThread().getContextClassLoader());
    }

    /**
     * @param propertiesFile The location of the properties file.
     * @param classLoader    A classloader, useful if trying to programmatically configure with the daemon, such as in a
     *                       unit test.
     * @throws IOException              Thrown when the properties file can't be accessed.
     * @throws IllegalArgumentException Thrown when the contents of the properties file are not as expected.
     */
    public MultiLangDaemonConfig(String propertiesFile, ClassLoader classLoader)
            throws IOException, IllegalArgumentException {
        this(propertiesFile, classLoader, new KinesisClientLibConfigurator());
    }

    /**
     * @param propertiesFile The location of the properties file.
     * @param classLoader    A classloader, useful if trying to programmatically configure with the daemon, such as in a
     *                       unit test.
     * @param configurator   A configurator to use.
     * @throws IOException              Thrown when the properties file can't be accessed.
     * @throws IllegalArgumentException Thrown when the contents of the properties file are not as expected.
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
        ClientConfiguration clientConfig = buildClientConfig(properties);

        kinesisClientLibConfig = configurator.getConfiguration(properties).withKinesisClientConfig(clientConfig)
                .withCloudWatchClientConfig(clientConfig).withDynamoDBClientConfig(clientConfig);

        executorService = buildExecutorService(properties);
        recordProcessorFactory = new MultiLangRecordProcessorFactory(executableName, executorService,
                kinesisClientLibConfig);

        LOG.info("Running " + kinesisClientLibConfig.getApplicationName() + " to process stream "
                + kinesisClientLibConfig.getStreamName() + " with executable " + executableName);
        prepare(processingLanguage);
    }

    private ClientConfiguration buildClientConfig(Properties properties) {
        ClientConfiguration clientConfig = new ClientConfiguration();
        String proxyHost = null;
        int proxyPort = 0;

        if (properties.getProperty(PROXY_HOST_PROP) != null) {
            LOG.debug("Getting proxy info from properties file.");

            proxyHost = properties.getProperty(PROXY_HOST_PROP);
            proxyPort = Integer.parseInt(properties.getProperty(PROXY_PORT_PROP));
        } else if (System.getProperty(PROXY_HOST_PROP) != null) {
            LOG.debug("Getting proxy info from java system properties");

            proxyHost = System.getProperty(PROXY_HOST_PROP);
            proxyPort = Integer.parseInt(System.getProperty(PROXY_PORT_PROP));
        } else if (System.getenv(HTTP_PROXY_ENV_VAR) != null) {
            LOG.debug("Getting proxy info environment settings");

            try {
                URI proxyAddr = new URI(System.getenv(HTTP_PROXY_ENV_VAR));

                proxyHost = proxyAddr.getHost();
                proxyPort = proxyAddr.getPort();
            } catch (URISyntaxException e) {
                LOG.error("System proxy not set correctly", e);
            }
        }

        if (StringUtils.isNotEmpty(proxyHost) && proxyPort > 0) {
            clientConfig = clientConfig.withProxyHost(proxyHost).withProxyPort(proxyPort);
        } else {
            LOG.debug("Not configuring proxy as none specified");
        }

        return clientConfig;
    }

    private void prepare(String processingLanguage) {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        LOG.info("Using workerId: " + kinesisClientLibConfig.getWorkerIdentifier());
        LOG.info("Using credentials with access key id: " + kinesisClientLibConfig.getKinesisCredentialsProvider()
                .getCredentials().getAWSAccessKeyId());

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
        LOG.debug(String.format("Value for %s property is %d", PROP_MAX_ACTIVE_THREADS, maxActiveThreads));
        if (maxActiveThreads <= 0) {
            LOG.info("Using a cached thread pool.");
            return new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                    builder.build());
        } else {
            LOG.info(String.format("Using a fixed thread pool with %d max active threads.", maxActiveThreads));
            return new ThreadPoolExecutor(maxActiveThreads, maxActiveThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(), builder.build());
        }
    }

    /**
     * @return A KinesisClientLibConfiguration object based on the properties file provided.
     */
    public KinesisClientLibConfiguration getKinesisClientLibConfiguration() {
        return kinesisClientLibConfig;
    }

    /**
     * @return An executor service based on the properties file provided.
     */
    public ExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * @return A MultiLangRecordProcessorFactory based on the properties file provided.
     */
    public MultiLangRecordProcessorFactory getRecordProcessorFactory() {
        return recordProcessorFactory;
    }
}
