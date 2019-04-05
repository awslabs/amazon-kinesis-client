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
