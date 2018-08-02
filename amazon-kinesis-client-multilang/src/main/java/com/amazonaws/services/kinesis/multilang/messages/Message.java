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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Abstract class for all messages that are sent to the client's process.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "action")
@JsonSubTypes({ 
        @Type(value = CheckpointMessage.class, name = CheckpointMessage.ACTION),
        @Type(value = InitializeMessage.class, name = InitializeMessage.ACTION),
        @Type(value = ProcessRecordsMessage.class, name = ProcessRecordsMessage.ACTION),
        @Type(value = ShutdownMessage.class, name = ShutdownMessage.ACTION),
        @Type(value = StatusMessage.class, name = StatusMessage.ACTION),
        @Type(value = ShutdownRequestedMessage.class, name = ShutdownRequestedMessage.ACTION),
})
public abstract class Message {

    private ObjectMapper mapper = new ObjectMapper();;

    /**
     * Default constructor.
     */
    public Message() {
    }

    /**
     * 
     * @param objectMapper An object mapper.
     * @return this
     */
    Message withObjectMapper(ObjectMapper objectMapper) {
        this.mapper = objectMapper;
        return this;
    }

    /**
     * 
     * @return A JSON representation of this object.
     */
    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            return super.toString();
        }
    }
}
