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

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;

/**
 * Publisher that contains the logic to publish metrics.
 */
@Slf4j
public class CloudWatchMetricsPublisher {
    // CloudWatch API has a limit of 20 MetricDatums per request
    private static final int BATCH_SIZE = 20;

    private final String namespace;
    private final CloudWatchAsyncClient cloudWatchClient;

    public CloudWatchMetricsPublisher(CloudWatchAsyncClient cloudWatchClient, String namespace) {
        this.cloudWatchClient = cloudWatchClient;
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
                cloudWatchClient.putMetricData(request.build());

                log.debug("Successfully published {} datums.", endIndex - startIndex);
            } catch (CloudWatchException e) {
                log.warn("Could not publish {} datums to CloudWatch", endIndex - startIndex, e);
            }
        }
    }
}
