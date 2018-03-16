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
import lombok.experimental.Accessors;
import software.amazon.kinesis.coordinator.RecordProcessorCheckpointer;

/**
 * Used by the KCL to manage checkpointing.
 */
@Data
@Accessors(fluent = true)
public class CheckpointConfig {
    /**
     * KCL will validate client provided sequence numbers with a call to Amazon Kinesis before checkpointing for calls
     * to {@link RecordProcessorCheckpointer#checkpoint(String)} by default.
     *
     * <p>Default value: true</p>
     */
    private boolean validateSequenceNumberBeforeCheckpointing = true;
}
