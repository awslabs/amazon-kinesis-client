/*
 * Copyright 2023 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.multilang;

import software.amazon.awssdk.regions.Region;

/**
 * Defines methods to process {@link NestedPropertyKey}s.
 */
public interface NestedPropertyProcessor {

    /**
     * Set the service endpoint where requests are sent.
     *
     * @param serviceEndpoint the service endpoint either with or without the protocol
     *      (e.g., https://sns.us-west-1.amazonaws.com, sns.us-west-1.amazonaws.com)
     * @param signingRegion the region to use for the client (e.g. us-west-1)
     *
     * @see #acceptEndpointRegion(Region)
     * @see <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/client/builder/SdkClientBuilder.html#endpointOverride(java.net.URI)">
     *     AwsClientBuilder.endpointOverride</a>
     */
    void acceptEndpoint(String serviceEndpoint, String signingRegion);

    /**
     * Set the service endpoint where requests are sent.
     *
     * @param region Region to be used by the client. This will be used to determine both the service endpoint
     *      (e.g., https://sns.us-west-1.amazonaws.com) and signing region (e.g., us-west-1) for requests.
     *
     * @see #acceptEndpoint(String, String)
     */
    void acceptEndpointRegion(Region region);

    /**
     * Set the external id, an optional field to designate who can assume an IAM role.
     *
     * @param externalId external id used in the service call used to retrieve session credentials
     */
    void acceptExternalId(String externalId);
}
