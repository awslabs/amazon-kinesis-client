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
package software.amazon.kinesis.lifecycle;

/**
 * Enumerates types of outcome of tasks executed as part of processing a shard.
 */
public enum TaskOutcome {
    /**
     * Denotes a successful task outcome.
     */
    SUCCESSFUL,
    /**
     * Denotes that the last record from the shard has been read/consumed.
     */
    END_OF_SHARD,
    /**
     * Denotes a failure or exception during processing of the shard.
     */
    FAILURE
}
