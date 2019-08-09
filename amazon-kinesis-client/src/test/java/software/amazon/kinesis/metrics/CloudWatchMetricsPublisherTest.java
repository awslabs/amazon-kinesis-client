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
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CloudWatchMetricsPublisherTest {
    private static final String NAMESPACE = "fakeNamespace";
    private CloudWatchMetricsPublisher publisher;

    @Mock
    private CloudWatchAsyncClient cloudWatchClient;

    @Before
    public void setup() {
        publisher = new CloudWatchMetricsPublisher(cloudWatchClient, NAMESPACE);
    }

    /*
     * Test whether the data input into metrics publisher is the equal to the data which will be published to CW
     */
    @Test
    public void testMetricsPublisher() {
        final CompletableFuture<PutMetricDataResponse> putResponseFuture = new CompletableFuture<>();
        putResponseFuture.complete(PutMetricDataResponse.builder().build());
        when(cloudWatchClient.putMetricData(any(PutMetricDataRequest.class))).thenReturn(putResponseFuture);

        List<MetricDatumWithKey<CloudWatchMetricKey>> dataToPublish = constructMetricDatumWithKeyList(25);
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

    public static List<MetricDatumWithKey<CloudWatchMetricKey>> constructMetricDatumWithKeyList(int value) {
        List<MetricDatumWithKey<CloudWatchMetricKey>> data = new ArrayList<MetricDatumWithKey<CloudWatchMetricKey>>();
        for (int i = 1; i <= value; i++) {
            MetricDatum datum =
                    TestHelper.constructDatum("datum" + Integer.toString(i), StandardUnit.COUNT, i, i, i, 1);
            data.add(new MetricDatumWithKey<CloudWatchMetricKey>(new CloudWatchMetricKey(datum), datum));
        }

        return data;
    }

    // batchSize is the number of metrics sent in a single request.
    // In CloudWatchMetricsPublisher this number is set to 20.
    public List<Map<String, MetricDatum>> constructMetricDatumListMap(List<MetricDatumWithKey<CloudWatchMetricKey>> data) {
        int batchSize = 20;
        List<Map<String, MetricDatum>> dataList = new ArrayList<Map<String, MetricDatum>>();

        int expectedRequestcount = (int) Math.ceil(data.size() / 20.0);

        for (int i = 0; i < expectedRequestcount; i++) {
            dataList.add(i, new HashMap<>());
        }

        int batchIndex = 1;
        int listIndex = 0;
        for (MetricDatumWithKey<CloudWatchMetricKey> metricDatumWithKey : data) {
            if (batchIndex > batchSize) {
                batchIndex = 1;
                listIndex++;
            }
            batchIndex++;
            dataList.get(listIndex).put(metricDatumWithKey.datum.metricName(), metricDatumWithKey.datum);
        }
        return dataList;
    }

    public static void assertMetricData(Map<String, MetricDatum> expected, PutMetricDataRequest actual) {
        List<MetricDatum> actualData = actual.metricData();
        for (MetricDatum actualDatum : actualData) {
            String metricName = actualDatum.metricName();
            Assert.assertNotNull(expected.get(metricName));
            Assert.assertTrue(expected.get(metricName).equals(actualDatum));
            expected.remove(metricName);
        }

        Assert.assertTrue(expected.isEmpty());
    }
}
