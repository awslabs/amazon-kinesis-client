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
package software.amazon.kinesis.exceptions;

/**
 *  This is thrown when the Amazon Kinesis Client Library encounters issues talking to its dependencies 
 *  (e.g. fetching data from Kinesis, DynamoDB table reads/writes, emitting metrics to CloudWatch).
 *  
 */
public class KinesisClientLibDependencyException extends KinesisClientLibRetryableException {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * @param message provides more details about the cause and potential ways to debug/address.
     */
    public KinesisClientLibDependencyException(String message) {
        super(message);
    }
    
    /**
     * @param message provides more details about the cause and potential ways to debug/address.
     * @param e Cause of the exception
     */
    public KinesisClientLibDependencyException(String message, Exception e) {
        super(message, e);
    }

}
