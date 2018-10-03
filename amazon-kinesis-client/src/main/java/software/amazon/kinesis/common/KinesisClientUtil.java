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

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;

/**
 * Utility to setup KinesisAsyncClient to be used with KCL.
 */
public class KinesisClientUtil {

    /**
     * Creates a client from a builder.
     *
     * @param clientBuilder
     * @return
     */
    public static KinesisAsyncClient createKinesisAsyncClient(KinesisAsyncClientBuilder clientBuilder) {
        return clientBuilder.httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(Integer.MAX_VALUE))
                .build();
    }
}
