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
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A checkpoint message is sent by the client's subprocess to indicate to the kcl processor that it should attempt to
 * checkpoint. The processor sends back a checkpoint message as an acknowledgement that it attempted to checkpoint along
 * with an error message which corresponds to the names of exceptions that a checkpointer can throw.
 */
@NoArgsConstructor
@Getter
@Setter
public class CheckpointMessage extends Message {
    /**
     * The name used for the action field in {@link Message}.
     */
    public static final String ACTION = "checkpoint";

    /**
     * The checkpoint this message is about.
     */
    private String sequenceNumber;

    private Long subSequenceNumber;

    /**
     * The name of an exception that occurred while attempting to checkpoint.
     */
    private String error;

    /**
     * Convenience constructor.
     *
     * @param sequenceNumber
     *            The sequence number that this message is about.
     * @param subSequenceNumber
     *            the sub sequence number for the checkpoint. This can be null.
     * @param throwable
     *            When responding to a client's process, the record processor will add the name of the exception that
     *            occurred while attempting to checkpoint if one did occur.
     */
    public CheckpointMessage(String sequenceNumber, Long subSequenceNumber, Throwable throwable) {
        this.setSequenceNumber(sequenceNumber);
        this.subSequenceNumber = subSequenceNumber;
        if (throwable != null) {
            this.setError(throwable.getClass().getSimpleName());
        }
    }
}
