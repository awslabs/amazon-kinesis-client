/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.kinesis.processor;

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
    void shutdownRequested(RecordProcessorCheckpointer checkpointer);

}
