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

package software.amazon.kinesis.coordinator;

import lombok.NoArgsConstructor;

/**
 * A NoOp implementation of CoordinatorExceptionHandler to pass through CoordinatorConfig as default
 */
@NoArgsConstructor
public class PassThroughCoordinatorExceptionHandler implements CoordinatorExceptionHandler {

    @Override
    public void schedulerInitializationExceptionHandler(Exception e) {
    }
}
