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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;


public class MetricAccumulatingQueueTest {

    private static final int MAX_QUEUE_SIZE = 5;
    private MetricAccumulatingQueue<CloudWatchMetricKey> queue;

    @Before
    public void setup() {
        this.queue = new MetricAccumulatingQueue<>(MAX_QUEUE_SIZE);
    }

    private Dimension dim(String name, String value) {
        return Dimension.builder().name(name).value(value).build();
    }
    
    /*
     * Test whether the MetricDatums offered into the queue will accumulate data based on the same metricName and 
     * output those datums with the correctly accumulated output.
     */
    @Test
    public void testAccumulation() {
        Collection<Dimension> dimensionsA = Collections.singleton(dim("name", "a"));
        Collection<Dimension> dimensionsB = Collections.singleton(dim("name", "b"));
        String keyA = "a";
        String keyB = "b";

        MetricDatum datum1 =
                TestHelper.constructDatum(keyA, StandardUnit.COUNT, 10, 5, 15, 2).toBuilder().dimensions(dimensionsA).build();
        queue.offer(new CloudWatchMetricKey(datum1), datum1);
        MetricDatum datum2 =
                TestHelper.constructDatum(keyA, StandardUnit.COUNT, 1, 1, 2, 2).toBuilder().dimensions(dimensionsA).build();
        queue.offer(new CloudWatchMetricKey(datum2), datum2);

        MetricDatum datum3 =
                TestHelper.constructDatum(keyA, StandardUnit.COUNT, 1, 1, 2, 2).toBuilder().dimensions(dimensionsB).build();
        queue.offer(new CloudWatchMetricKey(datum3), datum3);

        MetricDatum datum4 = TestHelper.constructDatum(keyA, StandardUnit.COUNT, 1, 1, 2, 2);
        queue.offer(new CloudWatchMetricKey(datum4), datum4);
        queue.offer(new CloudWatchMetricKey(datum4), datum4);

        MetricDatum datum5 =
                TestHelper.constructDatum(keyB, StandardUnit.COUNT, 100, 10, 110, 2).toBuilder().dimensions(dimensionsA).build();
        queue.offer(new CloudWatchMetricKey(datum5), datum5);

        Assert.assertEquals(4, queue.size());
        List<MetricDatumWithKey<CloudWatchMetricKey>> items = queue.drain(4);

        Assert.assertEquals(items.get(0).datum, TestHelper.constructDatum(keyA, StandardUnit.COUNT, 10, 1, 17, 4)
                .toBuilder().dimensions(dimensionsA).build());
        Assert.assertEquals(items.get(1).datum, datum3);
        Assert.assertEquals(items.get(2).datum, TestHelper.constructDatum(keyA, StandardUnit.COUNT, 1, 1, 4, 4));
        Assert.assertEquals(items.get(3).datum, TestHelper.constructDatum(keyB, StandardUnit.COUNT, 100, 10, 110, 2)
                .toBuilder().dimensions(dimensionsA).build());
    }
    
    /*
     * Test that the number of MetricDatum that can be added to our queue is capped at the MAX_QUEUE_SIZE.
     * Therefore, any datums added to the queue that is greater than the capacity of our queue will be dropped.
     */
    @Test
    public void testDrop() {
        for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
            MetricDatum datum = TestHelper.constructDatum(Integer.toString(i), StandardUnit.COUNT, 1, 1, 2, 2);
            CloudWatchMetricKey key = new CloudWatchMetricKey(datum);
            Assert.assertTrue(queue.offer(key, datum));
        }

        MetricDatum datum = TestHelper.constructDatum("foo", StandardUnit.COUNT, 1, 1, 2, 2);
        Assert.assertFalse(queue.offer(new CloudWatchMetricKey(datum), datum));
        Assert.assertEquals(MAX_QUEUE_SIZE, queue.size());
    }
}
