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

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.kinesis.retrieval.AWSExceptionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Publisher that contains the logic to publish metrics.
 */
@Slf4j
public class CloudWatchMetricsPublisher {
    // CloudWatch API has a limit of 20 MetricDatums per request
    private static final int BATCH_SIZE = 20;
    private static final int PUT_TIMEOUT_MILLIS = 5000;
    private static final AWSExceptionManager CW_EXCEPTION_MANAGER = new AWSExceptionManager();
    static {
        CW_EXCEPTION_MANAGER.add(CloudWatchException.class, t -> t);
    }

    private final String namespace;
    private final CloudWatchAsyncClient cloudWatchAsyncClient;

    public CloudWatchMetricsPublisher(CloudWatchAsyncClient cloudWatchClient, String namespace) {
        this.cloudWatchAsyncClient = cloudWatchClient;
        this.namespace = namespace;
    }

    /**
     * Given a list of MetricDatumWithKey, this method extracts the MetricDatum from each
     * MetricDatumWithKey and publishes those datums.
     *
     * @param dataToPublish a list containing all the MetricDatums to publish
     */
    public void publishMetrics(List<MetricDatumWithKey<CloudWatchMetricKey>> dataToPublish) {
        for (int startIndex = 0; startIndex < dataToPublish.size(); startIndex += BATCH_SIZE) {
            int endIndex = Math.min(dataToPublish.size(), startIndex + BATCH_SIZE);

            PutMetricDataRequest.Builder request = PutMetricDataRequest.builder();
            request = request.namespace(namespace);

            List<MetricDatum> metricData = new ArrayList<>();
            for (int i = startIndex; i < endIndex; i++) {
                metricData.add(dataToPublish.get(i).datum);
            }
            request = request.metricData(metricData);
            try {
                PutMetricDataRequest.Builder finalRequest = request;
                // This needs to be blocking. Making it asynchronous leads to increased throttling.
                blockingExecute(cloudWatchAsyncClient.putMetricData(finalRequest.build()), PUT_TIMEOUT_MILLIS,
                        CW_EXCEPTION_MANAGER);
            } catch(CloudWatchException | TimeoutException e) {
                log.warn("Could not publish {} datums to CloudWatch", endIndex - startIndex, e);
            } catch (Exception e) {
                log.error("Unknown exception while publishing {} datums to CloudWatch", endIndex - startIndex, e);
            }
        }
    }

    private static <T> void blockingExecute(CompletableFuture<T> future, long timeOutMillis,
            AWSExceptionManager exceptionManager) throws TimeoutException {
        try {
            future.get(timeOutMillis, MILLISECONDS);
        } catch (ExecutionException e) {
            throw exceptionManager.apply(e.getCause());
        } catch (InterruptedException e) {
            log.info("Thread interrupted.");
        }
    }
}
