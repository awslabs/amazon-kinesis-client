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
 * A message sent by the client's process to indicate to the record processor that it completed a particular action.
 */
public class StatusMessage extends Message {
    /**
     * The name used for the action field in {@link Message}.
     */
    public static final String ACTION = "status";

    /**
     * The name of the most recently received action.
     */
    private String responseFor;

    /**
     * Default constructor.
     */
    public StatusMessage() {
    }

    /**
     * Convenience constructor.
     * 
     * @param responseFor The response for.
     */
    public StatusMessage(String responseFor) {
        this.setResponseFor(responseFor);
    }

    /**
     * 
     * @return The response for.
     */
    public String getResponseFor() {
        return responseFor;
    }

    /**
     * 
     * @param responseFor The response for.
     */
    public void setResponseFor(String responseFor) {
        this.responseFor = responseFor;
    }
}
