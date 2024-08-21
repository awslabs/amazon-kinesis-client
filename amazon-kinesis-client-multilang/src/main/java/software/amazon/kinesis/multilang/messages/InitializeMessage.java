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
package software.amazon.kinesis.multilang.messages;

import lombok.Getter;
import lombok.Setter;
import software.amazon.kinesis.lifecycle.events.InitializationInput;

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
    public InitializeMessage() {}

    /**
     * Convenience constructor.
     *
     * @param initializationInput {@link InitializationInput}
     */
    public InitializeMessage(InitializationInput initializationInput) {
        this.shardId = initializationInput.shardId();
        if (initializationInput.extendedSequenceNumber() != null) {
            this.sequenceNumber = initializationInput.extendedSequenceNumber().sequenceNumber();
            this.subSequenceNumber =
                    initializationInput.extendedSequenceNumber().subSequenceNumber();
        } else {
            this.sequenceNumber = null;
            this.subSequenceNumber = null;
        }
    }
}
