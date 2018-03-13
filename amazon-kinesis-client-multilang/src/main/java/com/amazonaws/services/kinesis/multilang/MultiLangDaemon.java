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

import java.io.IOException;
import java.io.PrintStream;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
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

    private Worker worker;

    /**
     * Constructor.
     * 
     * @param configuration The KCL config to use.
     * @param recordProcessorFactory A record processor factory to create record processors that abide by the multi-lang
     *        protocol.
     * @param workerThreadPool The executor service to run the daemon in.
     */
    public MultiLangDaemon(KinesisClientLibConfiguration configuration,
            MultiLangRecordProcessorFactory recordProcessorFactory,
            ExecutorService workerThreadPool) {
        this(buildWorker(recordProcessorFactory, configuration, workerThreadPool));
    }

    private static Worker buildWorker(IRecordProcessorFactory recordProcessorFactory,
            KinesisClientLibConfiguration configuration, ExecutorService workerThreadPool) {
        return new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(configuration)
                .execService(workerThreadPool).build();
    }

    /**
     * 
     * @param worker A worker to use instead of the default worker.
     */
    public MultiLangDaemon(Worker worker) {
        this.worker = worker;
    }

    /**
     * Utility for describing how to run this app.
     * 
     * @param stream Where to output the usage info.
     * @param messageToPrepend An optional error message to describe why the usage is being printed.
     */
    public static void printUsage(PrintStream stream, String messageToPrepend) {
        StringBuilder builder = new StringBuilder();
        if (messageToPrepend != null) {
            builder.append(messageToPrepend);
        }
        builder.append(String.format("java %s <properties file>", MultiLangDaemon.class.getCanonicalName()));
        stream.println(builder.toString());
    }

    @Override
    public Integer call() throws Exception {
        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        return exitCode;
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
        MultiLangDaemonConfig config = null;
        try {
            config = new MultiLangDaemonConfig(args[0]);
        } catch (IOException e) {
            printUsage(System.err, "You must provide a properties file");
            System.exit(1);
        } catch (IllegalArgumentException e) {
            printUsage(System.err, e.getMessage());
            System.exit(1);
        }

        ExecutorService executorService = config.getExecutorService();

        // Daemon
        final MultiLangDaemon daemon = new MultiLangDaemon(
                config.getKinesisClientLibConfiguration(),
                config.getRecordProcessorFactory(),
                executorService);

        final long shutdownGraceMillis = config.getKinesisClientLibConfiguration().getShutdownGraceMillis();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.info("Process terminanted, will initiate shutdown.");
                try {
                    Future<Void> fut = daemon.worker.requestShutdown();
                    fut.get(shutdownGraceMillis, TimeUnit.MILLISECONDS);
                    LOG.info("Process shutdown is complete.");
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    LOG.error("Encountered an error during shutdown.", e);
                }
            }
        });

        Future<Integer> future = executorService.submit(daemon);
        try {
            System.exit(future.get());
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Encountered an error while running daemon", e);
        }
        System.exit(1);
    }
}
