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
package com.amazonaws.services.kinesis.clientlibrary.types;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;

/**
 * Container for the parameters to the IRecordProcessor's
 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#shutdown(ShutdownInput
 * shutdownInput) shutdown} method.
 */
public class ShutdownInput {
    
    private ShutdownReason shutdownReason;
    private IRecordProcessorCheckpointer checkpointer;

    /**
     * Default constructor.
     */
    public ShutdownInput() {
    }

    /**
     * Get shutdown reason.
     *
     * @return Reason for the shutdown (ShutdownReason.TERMINATE indicates the shard is closed and there are no
     *         more records to process. Shutdown.ZOMBIE indicates a fail over has occurred).
     */
    public ShutdownReason getShutdownReason() {
        return shutdownReason;
    }

    /**
     * Set shutdown reason.
     *
     * @param shutdownReason Reason for the shutdown
     * @return A reference to this updated object so that method calls can be chained together.
     */
    public ShutdownInput withShutdownReason(ShutdownReason shutdownReason) {
        this.shutdownReason = shutdownReason;
        return this;
    }

    /**
     * Get Checkpointer.
     *
     * @return The checkpointer object that the record processor should use to checkpoint
     */
    public IRecordProcessorCheckpointer getCheckpointer() {
        return checkpointer;
    }

    /**
     * Set the checkpointer.
     *
     * @param checkpointer The checkpointer object that the record processor should use to checkpoint
     * @return A reference to this updated object so that method calls can be chained together.
     */
    public ShutdownInput withCheckpointer(IRecordProcessorCheckpointer checkpointer) {
        this.checkpointer = checkpointer;
        return this;
    }

}
