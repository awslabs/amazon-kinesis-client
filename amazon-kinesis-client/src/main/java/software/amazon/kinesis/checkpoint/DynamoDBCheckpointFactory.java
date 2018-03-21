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

import lombok.Data;
import lombok.NonNull;
import software.amazon.kinesis.leases.ILeaseManager;
import software.amazon.kinesis.leases.KinesisClientLibLeaseCoordinator;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.processor.ICheckpoint;

/**
 *
 */
@Data
public class DynamoDBCheckpointFactory implements CheckpointFactory {
    @NonNull
    private final ILeaseManager leaseManager;
    @NonNull
    private final String workerIdentifier;
    private final long failoverTimeMillis;
    private final long epsilonMillis;
    private final int maxLeasesForWorker;
    private final int maxLeasesToStealAtOneTime;
    private final int maxLeaseRenewalThreads;
    @NonNull
    private final IMetricsFactory metricsFactory;

    @Override
    public ICheckpoint createCheckpoint() {
        return new KinesisClientLibLeaseCoordinator(leaseManager,
                workerIdentifier,
                failoverTimeMillis,
                epsilonMillis,
                maxLeasesForWorker,
                maxLeasesToStealAtOneTime,
                maxLeaseRenewalThreads,
                metricsFactory);
    }
}
