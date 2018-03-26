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

import lombok.AllArgsConstructor;
import software.amazon.kinesis.lifecycle.events.LeaseLost;
import software.amazon.kinesis.lifecycle.events.ShardCompleted;
import software.amazon.kinesis.lifecycle.events.ShutdownRequested;
import software.amazon.kinesis.lifecycle.events.Started;
import software.amazon.kinesis.processor.IRecordProcessor;

@AllArgsConstructor
public class RecordProcessorShim implements RecordProcessorLifecycle {

    private final IRecordProcessor delegate;

    @Override
    public void started(Started started) {
        InitializationInput initializationInput = started.toInitializationInput();
        delegate.initialize(initializationInput);
    }

    @Override
    public void recordsReceived(ProcessRecordsInput records) {

    }

    @Override
    public void leaseLost(LeaseLost leaseLost) {

    }

    @Override
    public void shardCompleted(ShardCompleted shardCompletedInput) {

    }

    @Override
    public void shutdownRequested(ShutdownRequested shutdownRequested) {

    }
}
