/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.metrics.impl;

import java.util.Set;

import com.amazonaws.AbortedException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.google.common.collect.ImmutableSet;

/**
 * An IMetricsFactory that creates IMetricsScopes that output themselves via CloudWatch. Batches IMetricsScopes together
 * to reduce API calls.
 */
public class CWMetricsFactory implements IMetricsFactory {

    /**
     * Default metrics level to enable. By default, all metrics levels are emitted.
     */
    public static final MetricsLevel DEFAULT_METRICS_LEVEL = MetricsLevel.DETAILED;
    /**
     * Default metrics dimensions. By default, all dimensions are enabled.
     */
    public static final Set<String> DEFAULT_METRICS_ENABLED_DIMENSIONS = ImmutableSet.of(
            IMetricsScope.METRICS_DIMENSIONS_ALL);

    /**
     * If the CWPublisherRunnable accumulates more than FLUSH_SIZE distinct metrics, it will call CloudWatch
     * immediately instead of waiting for the next scheduled call.
     */
    private static final int FLUSH_SIZE = 200;

    private final CWPublisherRunnable<CWMetricKey> runnable;
    private final Thread publicationThread;

    /**
     * Enabled metrics level. All metrics below this level will be dropped.
     */
    private final MetricsLevel metricsLevel;
    /**
     * List of enabled dimensions for metrics.
     */
    private final Set<String> metricsEnabledDimensions;

    /**
     * Constructor.
     * 
     * @param credentialsProvider client credentials for CloudWatch
     * @param namespace the namespace under which the metrics will appear in the CloudWatch console
     * @param bufferTimeMillis time to buffer metrics before publishing to CloudWatch
     * @param maxQueueSize maximum number of metrics that we can have in a queue
     */
    public CWMetricsFactory(AWSCredentialsProvider credentialsProvider,
            String namespace,
            long bufferTimeMillis,
            int maxQueueSize) {
        this(new AmazonCloudWatchClient(credentialsProvider), namespace, bufferTimeMillis, maxQueueSize);
    }

    /**
     * Constructor.
     * 
     * @param credentialsProvider client credentials for CloudWatch
     * @param clientConfig Configuration to use with the AmazonCloudWatchClient
     * @param namespace the namespace under which the metrics will appear in the CloudWatch console
     * @param bufferTimeMillis time to buffer metrics before publishing to CloudWatch
     * @param maxQueueSize maximum number of metrics that we can have in a queue
     */
    public CWMetricsFactory(AWSCredentialsProvider credentialsProvider,
            ClientConfiguration clientConfig,
            String namespace,
            long bufferTimeMillis,
            int maxQueueSize) {
        this(new AmazonCloudWatchClient(credentialsProvider, clientConfig), namespace, bufferTimeMillis, maxQueueSize);
    }

    /**
     * Constructor.
     * 
     * @param cloudWatchClient Client used to make CloudWatch requests
     * @param namespace the namespace under which the metrics will appear in the CloudWatch console
     * @param bufferTimeMillis time to buffer metrics before publishing to CloudWatch
     * @param maxQueueSize maximum number of metrics that we can have in a queue
     */
    public CWMetricsFactory(AmazonCloudWatch cloudWatchClient,
            String namespace,
            long bufferTimeMillis,
            int maxQueueSize) {
        this(cloudWatchClient, namespace, bufferTimeMillis, maxQueueSize,
                DEFAULT_METRICS_LEVEL, DEFAULT_METRICS_ENABLED_DIMENSIONS);
    }

    /**
     * Constructor.
     * 
     * @param cloudWatchClient Client used to make CloudWatch requests
     * @param namespace the namespace under which the metrics will appear in the CloudWatch console
     * @param bufferTimeMillis time to buffer metrics before publishing to CloudWatch
     * @param maxQueueSize maximum number of metrics that we can have in a queue
     * @param metricsLevel metrics level to enable
     * @param metricsEnabledDimensions metrics dimensions to allow
     */
    public CWMetricsFactory(AmazonCloudWatch cloudWatchClient,
            String namespace,
            long bufferTimeMillis,
            int maxQueueSize,
            MetricsLevel metricsLevel,
            Set<String> metricsEnabledDimensions) {
        this.metricsLevel = (metricsLevel == null ? DEFAULT_METRICS_LEVEL : metricsLevel);
        this.metricsEnabledDimensions = (metricsEnabledDimensions == null
                ? ImmutableSet.<String>of() : ImmutableSet.copyOf(metricsEnabledDimensions));

        runnable = new CWPublisherRunnable<CWMetricKey>(
                new DefaultCWMetricsPublisher(cloudWatchClient, namespace),
                bufferTimeMillis, maxQueueSize, FLUSH_SIZE);
        publicationThread = new Thread(runnable);
        publicationThread.setName("cw-metrics-publisher");
        publicationThread.start();
    }

    @Override
    public IMetricsScope createMetrics() {
        return new CWMetricsScope(runnable, metricsLevel, metricsEnabledDimensions);
    }

    public void shutdown() {
        runnable.shutdown();
        try {
            publicationThread.join();
        } catch (InterruptedException e) {
            throw new AbortedException(e.getMessage(), e);
        }
    }

}
