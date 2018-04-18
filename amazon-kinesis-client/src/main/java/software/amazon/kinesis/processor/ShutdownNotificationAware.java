/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.processor;

import software.amazon.kinesis.processor.IRecordProcessorCheckpointer;

/**
 * Allows a record processor to indicate it's aware of requested shutdowns, and handle the request.
 */
public interface ShutdownNotificationAware {

    /**
     * Called when the worker has been requested to shutdown, and gives the record processor a chance to checkpoint.
     *
     * The record processor will still have shutdown called.
     * 
     * @param checkpointer the checkpointer that can be used to save progress.
     */
    void shutdownRequested(IRecordProcessorCheckpointer checkpointer);

}
