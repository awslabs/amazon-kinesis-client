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

public interface RetrievalSpecificConfig {
    /**
     * Creates and returns a retrieval factory for the specific configuration
     *
     * @return a retrieval factory that can create an appropriate retriever
     */
    RetrievalFactory retrievalFactory();

    /**
     * Validates this instance is configured properly. For example, this
     * method may validate that the stream name, if one is required, is
     * non-null.
     * <br/><br/>
     * If not in a valid state, an informative unchecked Exception -- for
     * example, an {@link IllegalArgumentException} -- should be thrown so
     * the caller may rectify the misconfiguration.
     *
     * @param isMultiStream whether state should be validated for multi-stream
     *
     * @deprecated remove keyword `default` to force implementation-specific behavior
     */
    @Deprecated
    default void validateState(boolean isMultiStream) {
        // TODO convert this to a non-default implementation in a "major" release
    }
}
