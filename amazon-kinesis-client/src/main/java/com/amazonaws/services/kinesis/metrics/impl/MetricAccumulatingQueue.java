/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StatisticSet;

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
    private Map<KeyType, MetricDatum> map;

    public MetricAccumulatingQueue(int maxQueueSize) {
        queue = new LinkedBlockingQueue<MetricDatumWithKey<KeyType>>(maxQueueSize);
        map = new HashMap<KeyType, MetricDatum>();
    }

    /**
     * @param maxItems number of items to remove from the queue.
     * @return a list of MetricDatums that are no longer contained within the queue or map.
     */
    public synchronized List<MetricDatumWithKey<KeyType>> drain(int maxItems) {
        List<MetricDatumWithKey<KeyType>> drainedItems = new ArrayList<MetricDatumWithKey<KeyType>>(maxItems);

        queue.drainTo(drainedItems, maxItems);

        for (MetricDatumWithKey<KeyType> datumWithKey : drainedItems) {
            map.remove(datumWithKey.key);
        }

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
        MetricDatum old = map.get(key);
        if (old == null) {
            boolean offered = queue.offer(new MetricDatumWithKey<KeyType>(key, datum));
            if (offered) {
                map.put(key, datum);
            }

            return offered;
        } else {
            accumulate(old, datum);
            return true;
        }
    }

    private void accumulate(MetricDatum oldDatum, MetricDatum newDatum) {
        if (!oldDatum.getUnit().equals(newDatum.getUnit())) {
            throw new IllegalArgumentException("Unit mismatch for datum named " + oldDatum.getMetricName());
        }

        StatisticSet oldStats = oldDatum.getStatisticValues();
        StatisticSet newStats = newDatum.getStatisticValues();

        oldStats.setSampleCount(oldStats.getSampleCount() + newStats.getSampleCount());
        oldStats.setMaximum(Math.max(oldStats.getMaximum(), newStats.getMaximum()));
        oldStats.setMinimum(Math.min(oldStats.getMinimum(), newStats.getMinimum()));
        oldStats.setSum(oldStats.getSum() + newStats.getSum());
    }
}
