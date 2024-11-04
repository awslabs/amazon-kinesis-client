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
package software.amazon.kinesis.multilang;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.coordinator.Scheduler;

/**
 * Main app that launches the scheduler that runs the multi-language record processor.
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
 * # The DefaultCredentialsProvider checks several other providers, which is
 * # described here:
 * # https://sdk.amazonaws.com/java/api/2.0.0-preview-11/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html
 * AwsCredentialsProvider = DefaultCredentialsProvider
 * </pre>
 */
@Slf4j
public class MultiLangDaemon {
    static class MultiLangDaemonArguments {
        @Parameter
        List<String> parameters = new ArrayList<>();

        @Parameter(
                names = {"-p", "--properties-file"},
                description = "Properties file to be used with the KCL")
        String propertiesFile;

        @Parameter(
                names = {"-l", "--log-configuration"},
                description = "File location of logback.xml to be override the default")
        String logConfiguration;
    }

    @Data
    @Accessors(fluent = true)
    static class MultiLangRunner implements Callable<Integer> {
        private final Scheduler scheduler;

        @Override
        public Integer call() throws Exception {
            int exitCode = 0;
            try {
                scheduler().run();
            } catch (Throwable t) {
                log.error("Caught throwable while processing data", t);
                exitCode = 1;
            }
            return exitCode;
        }
    }

    JCommander buildJCommanderAndParseArgs(final MultiLangDaemonArguments arguments, final String[] args) {
        JCommander jCommander = JCommander.newBuilder()
                .programName("amazon-kinesis-client MultiLangDaemon")
                .addObject(arguments)
                .build();
        jCommander.parse(args);
        return jCommander;
    }

    void printUsage(final JCommander jCommander, final String message) {
        if (StringUtils.isNotEmpty(message)) {
            System.err.println(message);
        }
        jCommander.usage();
    }

    Scheduler buildScheduler(final MultiLangDaemonConfig config) {
        return config.getMultiLangDaemonConfiguration().build(config.getRecordProcessorFactory());
    }

    void configureLogging(final String logConfiguration) {
        if (StringUtils.isNotEmpty(logConfiguration)) {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configureLogging(logConfiguration, loggerContext, configurator);
        }
    }

    void configureLogging(
            final String logConfiguration, final LoggerContext loggerContext, final JoranConfigurator configurator) {
        loggerContext.reset();
        try (InputStream inputStream = FileUtils.openInputStream(new File(logConfiguration))) {
            configurator.setContext(loggerContext);
            configurator.doConfigure(inputStream);
        } catch (IOException | JoranException e) {
            throw new RuntimeException("Error while loading log configuration: " + e.getMessage());
        }
    }

    String validateAndGetPropertiesFileName(final MultiLangDaemonArguments arguments) {
        String propertiesFile = "";

        if (CollectionUtils.isNotEmpty(arguments.parameters)) {
            if (arguments.parameters.size() == 1) {
                propertiesFile = arguments.parameters.get(0);
            } else {
                throw new RuntimeException("Expected a single argument, but found multiple arguments.  Arguments: "
                        + String.join(", ", arguments.parameters));
            }
        }

        if (StringUtils.isNotEmpty(arguments.propertiesFile)) {
            if (StringUtils.isNotEmpty(propertiesFile)) {
                log.warn("Overriding the properties file with the --properties-file option");
            }
            propertiesFile = arguments.propertiesFile;
        }

        if (StringUtils.isEmpty(propertiesFile)) {
            throw new RuntimeException("Properties file missing, please provide a properties file");
        }

        return propertiesFile;
    }

    MultiLangDaemonConfig buildMultiLangDaemonConfig(final String propertiesFile) {
        try {
            return new MultiLangDaemonConfig(propertiesFile);
        } catch (IOException e) {
            throw new RuntimeException("Error while reading properties file: " + e.getMessage());
        }
    }

    void setupShutdownHook(final Runtime runtime, final MultiLangRunner runner, final MultiLangDaemonConfig config) {
        long shutdownGraceMillis = config.getMultiLangDaemonConfiguration().getShutdownGraceMillis();
        runtime.addShutdownHook(new Thread(() -> {
            log.info("Process terminated, will initiate shutdown.");
            try {
                Future<Boolean> runnerFuture = runner.scheduler().startGracefulShutdown();
                runnerFuture.get(shutdownGraceMillis, TimeUnit.MILLISECONDS);
                log.info("Process shutdown is complete.");
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error("Encountered an error during shutdown.", e);
            }
        }));
    }

    int submitRunnerAndWait(final MultiLangDaemonConfig config, final MultiLangRunner runner) {
        ExecutorService executorService = config.getExecutorService();
        Future<Integer> future = executorService.submit(runner);

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Encountered an error while running daemon", e);
        }
        return 1;
    }

    void exit(final int exitCode) {
        System.exit(exitCode);
    }

    /**
     * @param args
     *            Accepts a single argument, that argument is a properties file which provides KCL configuration as
     *            well as the name of an executable.
     */
    public static void main(final String[] args) {
        int exitCode = 1;
        MultiLangDaemon daemon = new MultiLangDaemon();
        MultiLangDaemonArguments arguments = new MultiLangDaemonArguments();
        JCommander jCommander = daemon.buildJCommanderAndParseArgs(arguments, args);
        try {
            String propertiesFileName = daemon.validateAndGetPropertiesFileName(arguments);
            daemon.configureLogging(arguments.logConfiguration);
            MultiLangDaemonConfig config = daemon.buildMultiLangDaemonConfig(propertiesFileName);

            Scheduler scheduler = daemon.buildScheduler(config);
            MultiLangRunner runner = new MultiLangRunner(scheduler);

            daemon.setupShutdownHook(Runtime.getRuntime(), runner, config);
            exitCode = daemon.submitRunnerAndWait(config, runner);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            daemon.printUsage(jCommander, t.getMessage());
            System.err.println("For more information, visit: https://github.com/awslabs/amazon-kinesis-client");
        }
        daemon.exit(exitCode);
    }
}
