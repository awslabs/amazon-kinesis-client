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

package software.amazon.kinesis.leases;

import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.metrics.MetricsFactory;

/**
 *
 */
public interface LeaseManagementFactory {
    LeaseCoordinator createLeaseCoordinator(MetricsFactory metricsFactory);

    ShardSyncTaskManager createShardSyncTaskManager(MetricsFactory metricsFactory);

    DynamoDBLeaseRefresher createLeaseRefresher();

    ShardDetector createShardDetector();
}
