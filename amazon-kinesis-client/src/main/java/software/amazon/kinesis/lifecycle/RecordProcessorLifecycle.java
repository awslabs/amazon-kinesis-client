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
package software.amazon.kinesis.lifecycle;

import software.amazon.kinesis.lifecycle.events.LeaseLost;
import software.amazon.kinesis.lifecycle.events.RecordsReceived;
import software.amazon.kinesis.lifecycle.events.ShardCompleted;
import software.amazon.kinesis.lifecycle.events.ShutdownRequested;
import software.amazon.kinesis.lifecycle.events.Started;

public interface RecordProcessorLifecycle {

    void started(Started started);
    void recordsReceived(RecordsReceived records);
    void leaseLost(LeaseLost leaseLost);
    void shardCompleted(ShardCompleted shardCompleted);
    void shutdownRequested(ShutdownRequested shutdownRequested);


}
