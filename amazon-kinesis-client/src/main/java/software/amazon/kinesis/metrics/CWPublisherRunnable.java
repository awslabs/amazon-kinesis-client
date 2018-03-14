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
package software.amazon.kinesis.metrics;

import java.util.Collection;
import java.util.List;
import java.util.Random;

import lombok.extern.slf4j.Slf4j;

/**
 * A CWPublisherRunnable contains the logic of when to publish metrics.
 * 
 * @param <KeyType>
 */
@Slf4j
public class CWPublisherRunnable<KeyType> implements Runnable {
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
        if (log.isDebugEnabled()) {
            log.debug("Constructing CWPublisherRunnable with maxBufferTimeMillis {} maxQueueSize {} batchSize {} maxJitter {}",
                    bufferTimeMillis,
                    maxQueueSize,
                    batchSize,
                    maxJitter);
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
                log.error("Encountered throwable in CWPublisherRunable", t);
            }
        }

        log.info("CWPublication thread finished.");
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
                if (log.isDebugEnabled()) {
                    log.debug("Drained {} datums from queue", dataToPublish.size());
                }

                if (shuttingDown) {
                    if (log.isDebugEnabled()) {
                        log.debug("Shutting down with {} datums left on the queue", queue.size());
                    }

                    // If we're shutting down, we successfully shut down only when the queue is empty.
                    shutdown = queue.isEmpty();
                }
            } else {
                long waitTime = bufferTimeMillis - timeSinceFlush;
                if (log.isDebugEnabled()) {
                    log.debug("Waiting up to {} ms for {} more datums to appear.", waitTime, flushSize
                            - queue.size());
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
                log.error("Caught exception thrown by metrics Publisher in CWPublisherRunnable", t);
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
        log.info("Shutting down CWPublication thread.");
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
                log.warn("Dropping metrics {} because CWPublisherRunnable is shutting down.", data);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Enqueueing {} datums for publication", data.size());
            }

            for (MetricDatumWithKey<KeyType> datumWithKey : data) {
                if (!queue.offer(datumWithKey.key, datumWithKey.datum)) {
                    log.warn("Metrics queue full - dropping metric {}", datumWithKey.datum);
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
