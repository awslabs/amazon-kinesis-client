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

import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A CWPublisherRunnable contains the logic of when to publish metrics.
 * 
 * @param <KeyType>
 */

public class CWPublisherRunnable<KeyType> implements Runnable {

    private static final Log LOG = LogFactory.getLog(CWPublisherRunnable.class);

    private final ICWMetricsPublisher<KeyType> metricsPublisher;
    private final MetricAccumulatingQueue<KeyType> queue;
    private final long bufferTimeMillis;

    /*
     * Number of metrics that will cause us to flush.
     */
    private int flushSize;
    private boolean shuttingDown = false;
    private boolean shutdown = false;
    private long lastFlushTime = Long.MAX_VALUE;
    private int maxJitter;

    private Random rand = new Random();
    private int nextJitterValueToUse = 0;

    /**
     * Constructor.
     * 
     * @param metricsPublisher publishes metrics
     * @param bufferTimeMillis time between publishing metrics
     * @param maxQueueSize max size of metrics to publish
     * @param batchSize size of batch that can be published at a time
     */

    public CWPublisherRunnable(ICWMetricsPublisher<KeyType> metricsPublisher,
            long bufferTimeMillis,
            int maxQueueSize,
            int batchSize) {
        this(metricsPublisher, bufferTimeMillis, maxQueueSize, batchSize, 0);
    }

    public CWPublisherRunnable(ICWMetricsPublisher<KeyType> metricsPublisher,
            long bufferTimeMillis,
            int maxQueueSize,
            int batchSize,
            int maxJitter) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Constructing CWPublisherRunnable with maxBufferTimeMillis %d maxQueueSize %d batchSize %d maxJitter %d",
                    bufferTimeMillis,
                    maxQueueSize,
                    batchSize,
                    maxJitter));
        }

        this.metricsPublisher = metricsPublisher;
        this.bufferTimeMillis = bufferTimeMillis;
        this.queue = new MetricAccumulatingQueue<KeyType>(maxQueueSize);
        this.flushSize = batchSize;
        this.maxJitter = maxJitter;
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                runOnce();
            } catch (Throwable t) {
                LOG.error("Encountered throwable in CWPublisherRunable", t);
            }
        }

        LOG.info("CWPublication thread finished.");
    }

    /**
     * Exposed for testing purposes.
     */
    public void runOnce() {
        List<MetricDatumWithKey<KeyType>> dataToPublish = null;
        synchronized (queue) {
            /*
             * We should send if:
             * 
             * it's been maxBufferTimeMillis since our last send
             * or if the queue contains > batchSize elements
             * or if we're shutting down
             */
            long timeSinceFlush = Math.max(0, getTime() - lastFlushTime);
            if (timeSinceFlush >= bufferTimeMillis || queue.size() >= flushSize || shuttingDown) {
                dataToPublish = queue.drain(flushSize);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Drained %d datums from queue", dataToPublish.size()));
                }

                if (shuttingDown) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Shutting down with %d datums left on the queue", queue.size()));
                    }

                    // If we're shutting down, we successfully shut down only when the queue is empty.
                    shutdown = queue.isEmpty();
                }
            } else {
                long waitTime = bufferTimeMillis - timeSinceFlush;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Waiting up to %dms for %d more datums to appear.", waitTime, flushSize
                            - queue.size()));
                }

                try {
                    // Wait for enqueues for up to maxBufferTimeMillis.
                    queue.wait(waitTime);
                } catch (InterruptedException e) {
                }
            }
        }

        if (dataToPublish != null) {
            try {
                metricsPublisher.publishMetrics(dataToPublish);
            } catch (Throwable t) {
                LOG.error("Caught exception thrown by metrics Publisher in CWPublisherRunnable", t);
            }
            // Changing the value of lastFlushTime will change the time when metrics are flushed next.
            lastFlushTime = getTime() + nextJitterValueToUse;
            if (maxJitter != 0) {
                // nextJittervalueToUse will be a value between (-maxJitter,+maxJitter)
                nextJitterValueToUse = maxJitter - rand.nextInt(2 * maxJitter);
            }
        }
    }

    /**
     * Overrideable for testing purposes.
     */
    protected long getTime() {
        return System.currentTimeMillis();
    }

    public void shutdown() {
        LOG.info("Shutting down CWPublication thread.");
        synchronized (queue) {
            shuttingDown = true;
            queue.notify();
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Enqueues metric data for publication.
     * 
     * @param data collection of MetricDatum to enqueue
     */
    public void enqueue(Collection<MetricDatumWithKey<KeyType>> data) {
        synchronized (queue) {
            if (shuttingDown) {
                LOG.warn(String.format("Dropping metrics %s because CWPublisherRunnable is shutting down.", data));
                return;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Enqueueing %d datums for publication", data.size()));
            }

            for (MetricDatumWithKey<KeyType> datumWithKey : data) {
                if (!queue.offer(datumWithKey.key, datumWithKey.datum)) {
                    LOG.warn("Metrics queue full - dropping metric " + datumWithKey.datum);
                }
            }

            // If this is the first enqueue, start buffering from now.
            if (lastFlushTime == Long.MAX_VALUE) {
                lastFlushTime = getTime();
            }

            queue.notify();
        }
    }

}
