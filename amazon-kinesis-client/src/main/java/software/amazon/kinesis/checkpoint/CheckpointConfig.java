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

package software.amazon.kinesis.checkpoint;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;

/**
 * Used by the KCL to manage checkpointing.
 */
@Data
@Accessors(fluent = true)
public class CheckpointConfig {
    @NonNull
    private final String tableName;

    @NonNull
    private final AmazonDynamoDB amazonDynamoDB;

    @NonNull
    private final String workerIdentifier;

    /**
     * KCL will validate client provided sequence numbers with a call to Amazon Kinesis before checkpointing for calls
     * to {@link RecordProcessorCheckpointer#checkpoint(String)} by default.
     *
     * <p>Default value: true</p>
     */
    private boolean validateSequenceNumberBeforeCheckpointing = true;

    private boolean consistentReads = false;

    private long failoverTimeMillis = 10000L;

    private int maxLeasesForWorker = Integer.MAX_VALUE;

    private int maxLeasesToStealAtOneTime = 1;

    private int maxLeaseRenewalThreads = 20;

    private IMetricsFactory metricsFactory = new NullMetricsFactory();

    private CheckpointFactory checkpointFactory;

    private long epsilonMillis = 25L;

    public CheckpointFactory checkpointFactory() {
        if (checkpointFactory == null) {
            checkpointFactory = new DynamoDBCheckpointFactory(metricsFactory());
        }
        return checkpointFactory;
    }
}
