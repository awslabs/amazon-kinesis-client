/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.multilang.messages;

/**
 * A checkpoint message is sent by the client's subprocess to indicate to the kcl processor that it should attempt to
 * checkpoint. The processor sends back a checkpoint message as an acknowledgement that it attempted to checkpoint along
 * with an error message which corresponds to the names of exceptions that a checkpointer can throw.
 */
public class CheckpointMessage extends Message {
    /**
     * The name used for the action field in {@link Message}.
     */
    public static final String ACTION = "checkpoint";

    /**
     * The checkpoint this message is about.
     */
    private String checkpoint;

    /**
     * The name of an exception that occurred while attempting to checkpoint.
     */
    private String error;

    /**
     * Default constructor.
     */
    public CheckpointMessage() {
    }

    /**
     * Convenience constructor.
     * 
     * @param sequenceNumber The sequence number that this message is about.
     * @param throwable When responding to a client's process, the record processor will add the name of the exception
     *        that occurred while attempting to checkpoint if one did occur.
     */
    public CheckpointMessage(String sequenceNumber, Throwable throwable) {
        this.setCheckpoint(sequenceNumber);
        if (throwable != null) {
            this.setError(throwable.getClass().getSimpleName());
        }
    }

    /**
     * @return The checkpoint.
     */
    public String getCheckpoint() {
        return checkpoint;
    }

    /**
     * @return The error.
     */
    public String getError() {
        return error;
    }

    /**
     * @param checkpoint The checkpoint.
     */
    public void setCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;
    }

    /**
     * @param error The error.
     */
    public void setError(String error) {
        this.error = error;
    }

}
