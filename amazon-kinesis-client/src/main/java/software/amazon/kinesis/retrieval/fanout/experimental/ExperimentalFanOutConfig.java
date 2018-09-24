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
package software.amazon.kinesis.retrieval.fanout.experimental;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.annotations.KinesisClientExperimental;
import software.amazon.kinesis.retrieval.RetrievalFactory;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;

/**
 * Enables validation of sequence number for every received record.
 *
 * <h2><strong>This is an experimental class and may be removed at any time</strong></h2>
 */
@KinesisClientExperimental
public class ExperimentalFanOutConfig extends FanOutConfig {

    public ExperimentalFanOutConfig(KinesisAsyncClient kinesisClient) {
        super(kinesisClient);
    }

    @Override
    public RetrievalFactory retrievalFactory() {
        return new ExperimentalFanOutRetrievalFactory(kinesisClient(), getOrCreateConsumerArn());
    }
}
