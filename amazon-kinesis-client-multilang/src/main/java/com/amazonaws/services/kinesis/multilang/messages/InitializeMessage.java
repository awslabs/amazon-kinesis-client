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
package com.amazonaws.services.kinesis.multilang.messages;

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import lombok.Getter;
import lombok.Setter;

/**
 * An initialize message is sent to the client's subprocess to indicate that it should perform its initialization steps.
 */
@Getter
@Setter
public class InitializeMessage extends Message {
    /**
     * The name used for the action field in {@link Message}.
     */
    public static final String ACTION = "initialize";

    /**
     * The shard id that this processor is getting initialized for.
     */
    private String shardId;
    private String sequenceNumber;
    private Long subSequenceNumber;

    /**
     * Default constructor.
     */
    public InitializeMessage() {
    }

    /**
     * Convenience constructor.
     * 
     * @param shardId The shard id.
     */
    public InitializeMessage(InitializationInput initializationInput) {
        this.shardId = initializationInput.getShardId();
        if (initializationInput.getExtendedSequenceNumber() != null) {
            this.sequenceNumber = initializationInput.getExtendedSequenceNumber().getSequenceNumber();
            this.subSequenceNumber = initializationInput.getExtendedSequenceNumber().getSubSequenceNumber();
        } else {
            this.sequenceNumber = null;
            this.subSequenceNumber = null;
        }

    }

}
