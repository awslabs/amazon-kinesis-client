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
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

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

    @Test
    public void testCountBasedBatchSplitting() {
        final CompletableFuture<PutMetricDataResponse> putResponseFuture = new CompletableFuture<>();
        putResponseFuture.complete(PutMetricDataResponse.builder().build());
        when(cloudWatchClient.putMetricData(any(PutMetricDataRequest.class))).thenReturn(putResponseFuture);

        // 2500 datums with the 1000-per-request limit should produce ceil(2500/1000) = 3 requests.
        int datumCount = 2 * 1000 + 500;
        List<MetricDatumWithKey<CloudWatchMetricKey>> dataToPublish = constructMetricDatumWithKeyList(datumCount);
        publisher.publishMetrics(dataToPublish);

        ArgumentCaptor<PutMetricDataRequest> argument = ArgumentCaptor.forClass(PutMetricDataRequest.class);
        Mockito.verify(cloudWatchClient, Mockito.times(3)).putMetricData(argument.capture());

        int totalDatums = 0;
        for (PutMetricDataRequest request : argument.getAllValues()) {
            Assert.assertTrue(request.metricData().size() <= 1000);
            totalDatums += request.metricData().size();
        }
        Assert.assertEquals(datumCount, totalDatums);
    }

    @Test
    public void testSizeBasedBatchSplitting() {
        final CompletableFuture<PutMetricDataResponse> putResponseFuture = new CompletableFuture<>();
        putResponseFuture.complete(PutMetricDataResponse.builder().build());
        when(cloudWatchClient.putMetricData(any(PutMetricDataRequest.class))).thenReturn(putResponseFuture);

        // Build datums large enough (via long dimension values) that far fewer than 1000 fit within the
        // ~900 KB payload budget, forcing a size-based split even though the count is below 1000.
        List<MetricDatumWithKey<CloudWatchMetricKey>> dataToPublish = new ArrayList<>();
        String bigValue = new String(new char[900]).replace('\0', 'x');
        for (int i = 1; i <= 500; i++) {
            MetricDatum datum = MetricDatum.builder()
                    .metricName("datum" + i)
                    .unit(StandardUnit.COUNT)
                    .value((double) i)
                    .dimensions(
                            TestHelper.constructDimension("d1", bigValue),
                            TestHelper.constructDimension("d2", bigValue))
                    .build();
            dataToPublish.add(new MetricDatumWithKey<>(new CloudWatchMetricKey(datum), datum));
        }

        publisher.publishMetrics(dataToPublish);

        ArgumentCaptor<PutMetricDataRequest> argument = ArgumentCaptor.forClass(PutMetricDataRequest.class);
        // Each datum is ~1.9 KB, so far fewer than 1000 fit -> more than one request for 500 datums.
        Mockito.verify(cloudWatchClient, Mockito.atLeast(2)).putMetricData(argument.capture());

        int totalDatums = 0;
        for (PutMetricDataRequest request : argument.getAllValues()) {
            Assert.assertTrue(request.metricData().size() <= 1000);
            // The invariant this change exists to protect: no request may exceed the ~900 KB budget. Assert it
            // directly against the same estimator the publisher uses, rather than trusting a hard-coded count.
            // Multi-datum requests must be within budget; a lone datum is the only allowed budget escape hatch.
            int estimatedRequestBytes = estimateRequestBytes(request);
            if (request.metricData().size() > 1) {
                Assert.assertTrue(
                        "multi-datum request estimated at " + estimatedRequestBytes
                                + " bytes should be within the 900 KB budget",
                        estimatedRequestBytes <= 900_000);
            }
            totalDatums += request.metricData().size();
        }
        Assert.assertEquals(500, totalDatums);
    }

    /**
     * A single datum whose estimated size already exceeds the payload budget must still be sent — as a lone-datum
     * request — rather than dropped or looped on forever. This exercises the "always include at least one" escape
     * hatch in {@link CloudWatchMetricsPublisher#publishMetrics}.
     */
    @Test
    public void testSingleOversizedDatumIsStillSentAlone() {
        final CompletableFuture<PutMetricDataResponse> putResponseFuture = new CompletableFuture<>();
        putResponseFuture.complete(PutMetricDataResponse.builder().build());
        when(cloudWatchClient.putMetricData(any(PutMetricDataRequest.class))).thenReturn(putResponseFuture);

        // One dimension value large enough that a single datum's estimate exceeds the ~900 KB budget on its own.
        String hugeValue = new String(new char[1_000_000]).replace('\0', 'x');
        MetricDatum oversized = MetricDatum.builder()
                .metricName("oversizedDatum")
                .unit(StandardUnit.COUNT)
                .value(1.0)
                .dimensions(TestHelper.constructDimension("big", hugeValue))
                .build();
        Assert.assertTrue(
                "test precondition: datum must exceed the budget",
                CloudWatchMetricsPublisher.estimateDatumBytes(oversized) > 900_000);

        // Pair it with a normal datum to confirm the oversized one is isolated into its own request.
        MetricDatum normal = TestHelper.constructDatum("normalDatum", StandardUnit.COUNT, 1, 1, 1, 1);

        List<MetricDatumWithKey<CloudWatchMetricKey>> dataToPublish = new ArrayList<>();
        dataToPublish.add(new MetricDatumWithKey<>(new CloudWatchMetricKey(oversized), oversized));
        dataToPublish.add(new MetricDatumWithKey<>(new CloudWatchMetricKey(normal), normal));

        publisher.publishMetrics(dataToPublish);

        ArgumentCaptor<PutMetricDataRequest> argument = ArgumentCaptor.forClass(PutMetricDataRequest.class);
        // Two requests: the oversized datum alone, then the normal datum.
        Mockito.verify(cloudWatchClient, Mockito.times(2)).putMetricData(argument.capture());

        List<PutMetricDataRequest> requests = argument.getAllValues();
        Assert.assertEquals(1, requests.get(0).metricData().size());
        Assert.assertEquals(
                "oversizedDatum", requests.get(0).metricData().get(0).metricName());
        Assert.assertEquals(1, requests.get(1).metricData().size());
        Assert.assertEquals("normalDatum", requests.get(1).metricData().get(0).metricName());
    }

    @Test
    public void testEstimateDatumBytesAccountsForStatisticsAndValues() {
        MetricDatum scalar = MetricDatum.builder()
                .metricName("m")
                .unit(StandardUnit.COUNT)
                .value(1.0)
                .build();

        MetricDatum statistic = MetricDatum.builder()
                .metricName("m")
                .unit(StandardUnit.COUNT)
                .statisticValues(StatisticSet.builder()
                        .maximum(10.0)
                        .minimum(1.0)
                        .sum(55.0)
                        .sampleCount(10.0)
                        .build())
                .build();

        List<Double> values = new ArrayList<>();
        List<Double> counts = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            values.add((double) i);
            counts.add(1.0);
        }
        MetricDatum distribution = MetricDatum.builder()
                .metricName("m")
                .unit(StandardUnit.COUNT)
                .values(values)
                .counts(counts)
                .build();

        int scalarBytes = CloudWatchMetricsPublisher.estimateDatumBytes(scalar);
        int statisticBytes = CloudWatchMetricsPublisher.estimateDatumBytes(statistic);
        int distributionBytes = CloudWatchMetricsPublisher.estimateDatumBytes(distribution);

        Assert.assertTrue(
                "statistic-set datum should estimate larger than a scalar datum", statisticBytes > scalarBytes);
        Assert.assertTrue(
                "150-entry distribution should estimate much larger than a scalar datum",
                distributionBytes > scalarBytes + 150 * 50);
    }

    /**
     * Mirrors the publisher's own estimator so tests can assert the per-request byte-budget invariant without
     * duplicating the field-by-field logic.
     */
    private static int estimateRequestBytes(PutMetricDataRequest request) {
        int bytes = 64 + NAMESPACE.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        for (MetricDatum datum : request.metricData()) {
            bytes += CloudWatchMetricsPublisher.estimateDatumBytes(datum);
        }
        return bytes;
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

    // The CloudWatch PutMetricData API allows up to 1000 datums (and 1 MB) per request, so the publisher packs
    // as many datums as possible into each request. A small number of datums therefore fits in a single request.
    public List<Map<String, MetricDatum>> constructMetricDatumListMap(
            List<MetricDatumWithKey<CloudWatchMetricKey>> data) {
        int batchSize = 1000;
        List<Map<String, MetricDatum>> dataList = new ArrayList<Map<String, MetricDatum>>();

        int expectedRequestcount = (int) Math.ceil(data.size() / (double) batchSize);

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
