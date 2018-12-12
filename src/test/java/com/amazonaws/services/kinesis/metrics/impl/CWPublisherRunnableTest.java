/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

public class CWPublisherRunnableTest {

    private static final int MAX_QUEUE_SIZE = 5;
    private static final long MAX_BUFFER_TIME_MILLIS = 1;

    /*
     * For tests to run properly, FLUSH_SIZE should be > 1 and < MAX_QUEUE_SIZE / 2
     */
    private static final int FLUSH_SIZE = 2;

    private static class TestHarness {
        private List<MetricDatumWithKey<CWMetricKey>> data = new ArrayList<MetricDatumWithKey<CWMetricKey>>();
        private int counter = 0;
        private ICWMetricsPublisher<CWMetricKey> publisher;
        private CWPublisherRunnable<CWMetricKey> runnable;
        private long time = 0L;

        @SuppressWarnings("unchecked")
        public TestHarness() {
            publisher = Mockito.mock(ICWMetricsPublisher.class);
            runnable = new CWPublisherRunnable<CWMetricKey>(publisher,
                    MAX_BUFFER_TIME_MILLIS,
                    MAX_QUEUE_SIZE,
                    FLUSH_SIZE) {

                @Override
                protected long getTime() {
                    return time;
                }

            };
        }

        public void enqueueRandom(int count) {
            for (int i = 0; i < count; i++) {
                int value = counter++;
                data.add(constructDatum(value));
            }

            runnable.enqueue(data.subList(data.size() - count, data.size()));
        }

        private MetricDatumWithKey<CWMetricKey> constructDatum(int value) {
            MetricDatum datum = TestHelper.constructDatum("datum-" + Integer.toString(value),
                    StandardUnit.Count,
                    value,
                    value,
                    value,
                    1);

            return new MetricDatumWithKey<CWMetricKey>(new CWMetricKey(datum), datum);
        }

        /**
         * Run one iteration of the runnable and assert that it called CloudWatch with count records beginning with
         * record startIndex, and no more than that.
         * 
         * @param startIndex
         * @param count
         */
        public void runAndAssert(int startIndex, int count) {
            runnable.runOnce();

            if (count > 0) {
                Mockito.verify(publisher).publishMetrics(data.subList(startIndex, startIndex + count));
            }

            Mockito.verifyNoMoreInteractions(publisher);
        }

        /**
         * Run one iteration of the runnable and assert that it called CloudWatch with all data.
         */
        public void runAndAssertAllData() {
            runAndAssert(0, data.size());
        }

        public void passTime(long time) {
            this.time += time;
        }

        public CWPublisherRunnable<CWMetricKey> getRunnable() {
            return runnable;
        }
    }

    private TestHarness harness;

    @Before
    public void setup() {
        harness = new TestHarness();
    }

    /**
     * Enqueue a full batch of data. Without allowing time to pass, assert that the runnable sends all data.
     */
    @Test
    public void testPublishOnFlushSize() {
        harness.enqueueRandom(FLUSH_SIZE);
        harness.runAndAssertAllData();
    }

    /**
     * Enqueue 1 message. Without allowing time to pass, assert that the runnable sends nothing.
     * Pass MAX_BUFFER_TIME_MILLIS of time, then assert that the runnable sends all data. Enqueue another message.
     * Repeat timing/assertion pattern.
     */
    @Test
    public void testWaitForBatchTimeout() {
        harness.enqueueRandom(1);
        harness.runAndAssert(0, 0);
        harness.passTime(MAX_BUFFER_TIME_MILLIS);
        harness.runAndAssertAllData();

        harness.enqueueRandom(1);
        harness.runAndAssert(0, 0);
        harness.passTime(MAX_BUFFER_TIME_MILLIS);
        harness.runAndAssert(1, 1);
    }

    /**
     * Enqueue two batches + 1 datum. Without allowing time to pass, assert that the runnable sends all but the last
     * datum. Pass MAX_BUFFER_TIME_MILLIS of time, then assert that the runnable sends the last datum.
     */
    @Test
    public void testDrainQueue() {
        int numBatches = 2;
        harness.enqueueRandom(FLUSH_SIZE * numBatches);
        harness.enqueueRandom(1);
        for (int i = 0; i < numBatches; i++) {
            harness.runAndAssert(i * FLUSH_SIZE, FLUSH_SIZE);
        }
        harness.runAndAssert(0, 0);
        harness.passTime(MAX_BUFFER_TIME_MILLIS);
        harness.runAndAssert(numBatches * FLUSH_SIZE, 1);
    }

    /**
     * Enqueue BATCH_SIZE + 1 messages. Shutdown the runnable. Without passing time, assert that the runnable sends all
     * data and isShutdown() returns false until all data is sent.
     */
    @Test
    public void testShutdown() {
        harness.enqueueRandom(FLUSH_SIZE + 1);
        harness.getRunnable().shutdown();

        harness.runAndAssert(0, FLUSH_SIZE);
        Assert.assertFalse(harness.getRunnable().isShutdown());

        harness.runAndAssert(FLUSH_SIZE, 1);
        Assert.assertTrue(harness.getRunnable().isShutdown());
    }

    /**
     * Enqueue MAX_QUEUE_SIZE + 1 messages. Shutdown the runnable. Assert that the runnable sends all but the last
     * datum and is shut down afterwards.
     */
    @Test
    public void testQueueFullDropData() {
        int numRecords = MAX_QUEUE_SIZE + 1;
        harness.enqueueRandom(numRecords);
        harness.getRunnable().shutdown();
        for (int i = 0; i < MAX_QUEUE_SIZE; i += FLUSH_SIZE) {
            harness.runAndAssert(i, Math.min(MAX_QUEUE_SIZE - i, FLUSH_SIZE));
        }

        Assert.assertTrue(harness.getRunnable().isShutdown());
    }
}
