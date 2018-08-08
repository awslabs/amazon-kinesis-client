/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.kinesis.common;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.retrieval.RetrievalConfig;

/**
 *
 */
@KinesisClientInternalApi
public class KinesisRequestsBuilder {
    public static ListShardsRequest.Builder listShardsRequestBuilder() {
        return appendUserAgent(ListShardsRequest.builder());
    }

    public static SubscribeToShardRequest.Builder subscribeToShardRequestBuilder() {
        return appendUserAgent(SubscribeToShardRequest.builder());
    }

    public static GetRecordsRequest.Builder getRecordsRequestBuilder() {
        return appendUserAgent(GetRecordsRequest.builder());
    }

    public static GetShardIteratorRequest.Builder getShardIteratorRequestBuilder() {
        return appendUserAgent(GetShardIteratorRequest.builder());
    }

    public static DescribeStreamSummaryRequest.Builder describeStreamSummaryRequestBuilder() {
        return appendUserAgent(DescribeStreamSummaryRequest.builder());
    }

    public static RegisterStreamConsumerRequest.Builder registerStreamConsumerRequestBuilder() {
        return appendUserAgent(RegisterStreamConsumerRequest.builder());
    }

    public static DescribeStreamConsumerRequest.Builder describeStreamConsumerRequestBuilder() {
        return appendUserAgent(DescribeStreamConsumerRequest.builder());
    }

    @SuppressWarnings("unchecked")
    private static <T extends AwsRequest.Builder> T appendUserAgent(final T builder) {
        return (T) builder
                .overrideConfiguration(
                        AwsRequestOverrideConfiguration.builder()
                .addApiName(ApiName.builder().name(RetrievalConfig.KINESIS_CLIENT_LIB_USER_AGENT)
                        .version(RetrievalConfig.KINESIS_CLIENT_LIB_USER_AGENT_VERSION).build())
                .build());
    }

}
