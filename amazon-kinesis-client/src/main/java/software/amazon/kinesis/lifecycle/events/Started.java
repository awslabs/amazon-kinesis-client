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
package software.amazon.kinesis.lifecycle.events;

import lombok.Data;
import software.amazon.kinesis.lifecycle.InitializationInput;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

@Data
public class Started {

    private final String shardId;
    private final ExtendedSequenceNumber sequenceNumber;
    private final ExtendedSequenceNumber pendingSequenceNumber;

    public InitializationInput toInitializationInput() {
        return new InitializationInput().withShardId(shardId).withExtendedSequenceNumber(sequenceNumber)
                .withExtendedSequenceNumber(sequenceNumber);
    }

}
