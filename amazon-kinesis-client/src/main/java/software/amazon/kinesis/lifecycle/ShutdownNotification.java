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
package software.amazon.kinesis.lifecycle;

import software.amazon.kinesis.processor.RecordProcessor;

/**
 * A shutdown request to the ShardConsumer
 */
public interface ShutdownNotification {
    /**
     * Used to indicate that the record processor has been notified of a requested shutdown, and given the chance to
     * checkpoint.
     *
     */
    void shutdownNotificationComplete();

    /**
     * Used to indicate that the record processor has completed the call to
     * {@link RecordProcessor#shutdown(ShutdownInput)} has
     * completed.
     */
    void shutdownComplete();
}
