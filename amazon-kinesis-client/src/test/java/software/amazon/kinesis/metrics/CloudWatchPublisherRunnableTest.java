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
package software.amazon.kinesis.metrics;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

public class CloudWatchPublisherRunnableTest {

    private static final int MAX_QUEUE_SIZE = 5;
    private static final long MAX_BUFFER_TIME_MILLIS = 1;

    /*
     * For tests to run properly, FLUSH_SIZE should be > 1 and < MAX_QUEUE_SIZE / 2
     */
    private static final int FLUSH_SIZE = 2;

    private static class TestHarness {
        private List<MetricDatumWithKey<CloudWatchMetricKey>> data =
                new ArrayList<MetricDatumWithKey<CloudWatchMetricKey>>();
        private int counter = 0;
        private CloudWatchMetricsPublisher publisher;
        private CloudWatchPublisherRunnable runnable;
        private long time = 0L;

        TestHarness() {
            publisher = Mockito.mock(CloudWatchMetricsPublisher.class);
            runnable = new CloudWatchPublisherRunnable(publisher, MAX_BUFFER_TIME_MILLIS, MAX_QUEUE_SIZE, FLUSH_SIZE) {

                @Override
                protected long getTime() {
                    return time;
                }
            };
        }

        void enqueueRandom(int count) {
            for (int i = 0; i < count; i++) {
                int value = counter++;
                data.add(constructDatum(value));
            }

            runnable.enqueue(data.subList(data.size() - count, data.size()));
        }

        private MetricDatumWithKey<CloudWatchMetricKey> constructDatum(int value) {
            MetricDatum datum = TestHelper.constructDatum(
                    "datum-" + Integer.toString(value), StandardUnit.COUNT, value, value, value, 1);

            return new MetricDatumWithKey<CloudWatchMetricKey>(new CloudWatchMetricKey(datum), datum);
        }

        /**
         * Run one iteration of the runnable and assert that it called CloudWatch with count records beginning with
         * record startIndex, and no more than that.
         *
         * @param startIndex
         * @param count
         */
        void runAndAssert(int startIndex, int count) {
            runnable.runOnce();

            if (count > 0) {
                Mockito.verify(publisher).publishMetrics(data.subList(startIndex, startIndex + count));
            }

            Mockito.verifyNoMoreInteractions(publisher);
        }

        /**
         * Run one iteration of the runnable and assert that it called CloudWatch with all data.
         */
        void runAndAssertAllData() {
            runAndAssert(0, data.size());
        }

        void passTime(long time) {
            this.time += time;
        }

        CloudWatchPublisherRunnable getRunnable() {
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
