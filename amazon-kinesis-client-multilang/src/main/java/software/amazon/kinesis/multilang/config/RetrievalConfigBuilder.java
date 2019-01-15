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

package software.amazon.kinesis.multilang.config;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.retrieval.RetrievalSpecificConfig;

public interface RetrievalConfigBuilder {
    /**
     * Creates a retrieval specific configuration using the supplied parameters, and internal class parameters
     * 
     * @param kinesisAsyncClient
     *            the client that will be provided to the RetrievalSpecificConfig constructor
     * @param parent
     *            configuration parameters that this builder can access to configure it self
     * @return a RetrievalSpecificConfig configured according to the customer's configuration.
     */
    public RetrievalSpecificConfig build(KinesisAsyncClient kinesisAsyncClient, MultiLangDaemonConfiguration parent);
}
