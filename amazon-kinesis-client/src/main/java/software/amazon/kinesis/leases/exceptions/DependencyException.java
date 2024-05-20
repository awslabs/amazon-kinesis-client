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
package software.amazon.kinesis.leases.exceptions;

/**
 * Indicates that a lease operation has failed because a dependency of the leasing system has failed. This will happen
 * if DynamoDB throws an InternalServerException or a generic AmazonClientException (the specific subclasses of
 * AmazonClientException are all handled more gracefully).
 */
public class DependencyException extends LeasingException {

    private static final long serialVersionUID = 1L;

    public DependencyException(Throwable e) {
        super(e);
    }

    public DependencyException(String message, Throwable e) {
        super(message, e);
    }
}
