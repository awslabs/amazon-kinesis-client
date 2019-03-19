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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;


/**
 * Helper class for accumulating MetricDatums with the same name and dimensions.
 * 
 * @param <KeyType> can be a class or object defined by the user that stores information about a MetricDatum needed
 *        by the user.
 * 
 *        The following is a example of what a KeyType class might look like:
 *        class SampleKeyType {
 *              private long timeKeyCreated;
 *              private MetricDatum datum;
 *              public SampleKeyType(long timeKeyCreated, MetricDatum datum){
 *                  this.timeKeyCreated = timeKeyCreated;
 *                  this.datum = datum;
 *              }
 *        }
 */
public class MetricAccumulatingQueue<KeyType> {

    // Queue is for first in first out behavior
    private BlockingQueue<MetricDatumWithKey<KeyType>> queue;
    // Map is for constant time lookup by key
    private Map<KeyType, MetricDatumWithKey<KeyType>> map;

    public MetricAccumulatingQueue(int maxQueueSize) {
        queue = new LinkedBlockingQueue<>(maxQueueSize);
        map = new HashMap<>();
    }

    /**
     * @param maxItems number of items to remove from the queue.
     * @return a list of MetricDatums that are no longer contained within the queue or map.
     */
    public synchronized List<MetricDatumWithKey<KeyType>> drain(int maxItems) {
        List<MetricDatumWithKey<KeyType>> drainedItems = new ArrayList<>(maxItems);
        queue.drainTo(drainedItems, maxItems);
        drainedItems.forEach(datumWithKey -> map.remove(datumWithKey.key));
        return drainedItems;
    }

    public synchronized boolean isEmpty() {
        return queue.isEmpty();
    }

    public synchronized int size() {
        return queue.size();
    }

    /**
     * We use a queue and a map in this method. The reason for this is because, the queue will keep our metrics in
     * FIFO order and the map will provide us with constant time lookup to get the appropriate MetricDatum.
     * 
     * @param key metric key to be inserted into queue
     * @param datum metric to be inserted into queue
     * @return a boolean depending on whether the datum was inserted into the queue
     */
    public synchronized boolean offer(KeyType key, MetricDatum datum) {
        MetricDatumWithKey<KeyType> metricDatumWithKey = map.get(key);

        if (metricDatumWithKey == null) {
            metricDatumWithKey = new MetricDatumWithKey<>(key, datum);
            boolean offered = queue.offer(metricDatumWithKey);
            if (offered) {
                map.put(key, metricDatumWithKey);
            }

            return offered;
        } else {
            accumulate(metricDatumWithKey, datum);
            return true;
        }
    }

    private void accumulate(MetricDatumWithKey<KeyType> metricDatumWithKey, MetricDatum newDatum) {
        MetricDatum oldDatum = metricDatumWithKey.datum;
        if (!oldDatum.unit().equals(newDatum.unit())) {
            throw new IllegalArgumentException("Unit mismatch for datum named " + oldDatum.metricName());
        }

        StatisticSet oldStats = oldDatum.statisticValues();
        StatisticSet newStats = newDatum.statisticValues();

        StatisticSet statisticSet = oldStats.toBuilder().sum(oldStats.sum() + newStats.sum())
                .minimum(Math.min(oldStats.minimum(), newStats.minimum()))
                .maximum(Math.max(oldStats.maximum(), newStats.maximum()))
                .sampleCount(oldStats.sampleCount() + newStats.sampleCount()).build();

        MetricDatum datum = oldDatum.toBuilder().statisticValues(statisticSet).build();
        metricDatumWithKey.datum(datum);
    }
}
