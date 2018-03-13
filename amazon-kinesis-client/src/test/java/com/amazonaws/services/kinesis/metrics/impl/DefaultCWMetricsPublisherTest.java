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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

public class DefaultCWMetricsPublisherTest {

    private final String NAMESPACE = "fakeNamespace";
    private final AmazonCloudWatch cloudWatchClient = Mockito.mock(AmazonCloudWatch.class);
    private DefaultCWMetricsPublisher publisher = new DefaultCWMetricsPublisher(cloudWatchClient, NAMESPACE);

    /*
     * Test whether the data input into metrics publisher is the equal to the data which will be published to CW
     */

    @Test
    public void testMetricsPublisher() {
        List<MetricDatumWithKey<CWMetricKey>> dataToPublish = constructMetricDatumWithKeyList(25);
        List<Map<String, MetricDatum>> expectedData = constructMetricDatumListMap(dataToPublish);
        publisher.publishMetrics(dataToPublish);
        
        ArgumentCaptor<PutMetricDataRequest> argument = ArgumentCaptor.forClass(PutMetricDataRequest.class);
        Mockito.verify(cloudWatchClient, Mockito.atLeastOnce()).putMetricData(argument.capture());

        List<PutMetricDataRequest> requests = argument.getAllValues();
        Assert.assertEquals(expectedData.size(), requests.size());

        for (int i = 0; i < requests.size(); i++) {
            assertMetricData(expectedData.get(i), requests.get(i));
        }

    }

    public static List<MetricDatumWithKey<CWMetricKey>> constructMetricDatumWithKeyList(int value) {
        List<MetricDatumWithKey<CWMetricKey>> data = new ArrayList<MetricDatumWithKey<CWMetricKey>>();
        for (int i = 1; i <= value; i++) {
            MetricDatum datum =
                    TestHelper.constructDatum("datum" + Integer.toString(i), StandardUnit.Count, i, i, i, 1);
            data.add(new MetricDatumWithKey<CWMetricKey>(new CWMetricKey(datum), datum));
        }

        return data;
    }

    // batchSize is the number of metrics sent in a single request.
    // In DefaultCWMetricsPublisher this number is set to 20.
    public List<Map<String, MetricDatum>> constructMetricDatumListMap(List<MetricDatumWithKey<CWMetricKey>> data) {
        int batchSize = 20;
        List<Map<String, MetricDatum>> dataList = new ArrayList<Map<String, MetricDatum>>();

        int expectedRequestcount = (int) Math.ceil(data.size() / 20.0);

        for (int i = 0; i < expectedRequestcount; i++) {
            dataList.add(i, new HashMap<String, MetricDatum>());
        }

        int batchIndex = 1;
        int listIndex = 0;
        for (MetricDatumWithKey<CWMetricKey> metricDatumWithKey : data) {
            if (batchIndex > batchSize) {
                batchIndex = 1;
                listIndex++;
            }
            batchIndex++;
            dataList.get(listIndex).put(metricDatumWithKey.datum.getMetricName(), metricDatumWithKey.datum);
        }
        return dataList;
    }

    public static void assertMetricData(Map<String, MetricDatum> expected, PutMetricDataRequest actual) {
        List<MetricDatum> actualData = actual.getMetricData();
        for (MetricDatum actualDatum : actualData) {
            String metricName = actualDatum.getMetricName();
            Assert.assertNotNull(expected.get(metricName));
            Assert.assertTrue(expected.get(metricName).equals(actualDatum));
            expected.remove(metricName);
        }

        Assert.assertTrue(expected.isEmpty());
    }
}
