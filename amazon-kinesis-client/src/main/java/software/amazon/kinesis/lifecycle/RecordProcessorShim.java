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

import lombok.RequiredArgsConstructor;
import software.amazon.kinesis.lifecycle.events.LeaseLost;
import software.amazon.kinesis.lifecycle.events.RecordsReceived;
import software.amazon.kinesis.lifecycle.events.ShardCompleted;
import software.amazon.kinesis.lifecycle.events.ShutdownRequested;
import software.amazon.kinesis.lifecycle.events.Started;
import software.amazon.kinesis.processor.RecordProcessor;
import software.amazon.kinesis.processor.IRecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShutdownNotificationAware;

@RequiredArgsConstructor
public class RecordProcessorShim implements RecordProcessorLifecycle {

    private final RecordProcessor delegate;

    @Override
    public void started(Started started) {
        delegate.initialize(started.toInitializationInput());
    }

    @Override
    public void recordsReceived(RecordsReceived records) {
        delegate.processRecords(records.toProcessRecordsInput());
    }

    @Override
    public void leaseLost(LeaseLost leaseLost) {
        ShutdownInput shutdownInput = new ShutdownInput() {
            @Override
            public IRecordProcessorCheckpointer checkpointer() {
                throw new UnsupportedOperationException("Cannot checkpoint when the lease is lost");
            }
        }.shutdownReason(ShutdownReason.ZOMBIE);

        delegate.shutdown(shutdownInput);
    }

    @Override
    public void shardCompleted(ShardCompleted shardCompleted) {
        ShutdownInput shutdownInput = new ShutdownInput().checkpointer(shardCompleted.getCheckpointer())
                .shutdownReason(ShutdownReason.TERMINATE);
        delegate.shutdown(shutdownInput);
    }

    @Override
    public void shutdownRequested(ShutdownRequested shutdownRequested) {
        if (delegate instanceof ShutdownNotificationAware) {
            ShutdownNotificationAware aware = (ShutdownNotificationAware)delegate;
            aware.shutdownRequested(shutdownRequested.getCheckpointer());
        }
    }
}
