/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
 * http://aws.amazon.com/asl/ or in the "license" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.kinesis.retrieval;

import software.amazon.kinesis.leases.exceptions.DependencyException;

/**
 *
 */
public interface ConsumerRegistration {
    /**
     * This method is used to get or create StreamConsumer information from Kinesis. It returns the StreamConsumer ARN
     * after retrieving it.
     *
     * @return StreamConsumer ARN
     * @throws DependencyException
     */
    String getOrCreateStreamConsumerArn() throws DependencyException;
}
