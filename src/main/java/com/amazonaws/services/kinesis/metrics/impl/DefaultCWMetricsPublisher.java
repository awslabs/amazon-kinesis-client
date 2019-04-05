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
package com.amazonaws.services.kinesis.metrics.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;

/**
 * Default implementation for publishing metrics to CloudWatch.
 */

public class DefaultCWMetricsPublisher implements ICWMetricsPublisher<CWMetricKey> {

    private static final Log LOG = LogFactory.getLog(CWPublisherRunnable.class);

    // CloudWatch API has a limit of 20 MetricDatums per request
    private static final int BATCH_SIZE = 20;

    private final String namespace;
    private final AmazonCloudWatch cloudWatchClient;

    public DefaultCWMetricsPublisher(AmazonCloudWatch cloudWatchClient, String namespace) {
        this.cloudWatchClient = cloudWatchClient;
        this.namespace = namespace;
    }

    @Override
    public void publishMetrics(List<MetricDatumWithKey<CWMetricKey>> dataToPublish) {
        for (int startIndex = 0; startIndex < dataToPublish.size(); startIndex += BATCH_SIZE) {
            int endIndex = Math.min(dataToPublish.size(), startIndex + BATCH_SIZE);

            PutMetricDataRequest request = new PutMetricDataRequest();
            request.setNamespace(namespace);

            List<MetricDatum> metricData = new ArrayList<MetricDatum>();
            for (int i = startIndex; i < endIndex; i++) {
                metricData.add(dataToPublish.get(i).datum);
            }

            request.setMetricData(metricData);

            try {
                cloudWatchClient.putMetricData(request);

                LOG.debug(String.format("Successfully published %d datums.", endIndex - startIndex));
            } catch (AmazonClientException e) {
                LOG.warn(String.format("Could not publish %d datums to CloudWatch", endIndex - startIndex), e);
            }
        }
    }
}
