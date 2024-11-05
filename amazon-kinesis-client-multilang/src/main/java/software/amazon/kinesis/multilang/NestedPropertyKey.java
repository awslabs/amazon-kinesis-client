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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.CaseFormat;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;

/**
 * Key-Value pairs which may be nested in, and extracted from, a property value
 * in a Java properties file. For example, given the line in a property file of
 * {@code my_key = my_value|foo=bar} and a delimiter split on {@code |} (pipe),
 * the value {@code my_value|foo=bar} would have a nested key of {@code foo}
 * and its corresponding value is {@code bar}.
 * <br/><br/>
 * The order of nested properties does not matter, and these properties are optional.
 * Customers may choose to provide, in any order, zero-or-more nested properties.
 * <br/><br/>
 * Duplicate keys are not supported, and may result in a last-write-wins outcome.
 */
@Slf4j
public enum NestedPropertyKey {

    /**
     * Specify the service endpoint where requests will be submitted.
     * This property's value must be in the following format:
     * <pre>
     *     ENDPOINT ::= SERVICE_ENDPOINT "^" SIGNING_REGION
     *     SERVICE_ENDPOINT ::= URL
     *     SIGNING_REGION ::= AWS_REGION
     * </pre>
     *
     * It would be redundant to provide both this and {@link #ENDPOINT_REGION}.
     *
     * @see #ENDPOINT_REGION
     * @see <a href="https://docs.aws.amazon.com/general/latest/gr/rande.html">AWS Service endpoints</a>
     * @see <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-regions">Available Regions</a>
     */
    ENDPOINT {
        void visit(final NestedPropertyProcessor processor, final String endpoint) {
            final String[] tokens = endpoint.split("\\^");
            if (tokens.length != 2) {
                throw new IllegalArgumentException("Invalid " + name() + ": " + endpoint);
            }
            processor.acceptEndpoint(tokens[0], tokens[1]);
        }
    },

    /**
     * Specify the region where service requests will be submitted. This
     * region will determine both the service endpoint and signing region.
     * <br/><br/>
     * It would be redundant to provide both this and {@link #ENDPOINT}.
     *
     * @see #ENDPOINT
     * @see <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-regions">Available Regions</a>
     */
    ENDPOINT_REGION {
        void visit(final NestedPropertyProcessor processor, final String regionName) {
            List<Region> validRegions = Region.regions();
            Region region = Region.of(regionName);
            if (!validRegions.contains(region)) {
                throw new IllegalArgumentException("Invalid region name: " + regionName);
            }
            processor.acceptEndpointRegion(region);
        }
    },

    /**
     * External ids may be used when delegating access in a multi-tenant
     * environment, or to third parties.
     *
     * @see <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html">
     *     How to use an external ID when granting access to your AWS resources to a third party</a>
     */
    EXTERNAL_ID {
        void visit(final NestedPropertyProcessor processor, final String externalId) {
            processor.acceptExternalId(externalId);
        }
    },
    ;

    /**
     * Nested key within the property value. For example, a nested key-value
     * of {@code foo=bar} has a nested key of {@code foo}.
     */
    @Getter(AccessLevel.PACKAGE)
    private final String nestedKey;

    NestedPropertyKey() {
        // convert the enum from UPPER_SNAKE_CASE to lowerCamelCase
        nestedKey = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name());
    }

    abstract void visit(NestedPropertyProcessor processor, String value);

    /**
     * Parses any number of parameters. Each nested property will prompt a
     * visit to the {@code processor}.
     *
     * @param processor processor to be invoked for every nested property
     * @param params parameters to check for a nested property key
     */
    public static void parse(final NestedPropertyProcessor processor, final String... params) {
        // Construct a disposable cache to keep this O(n). Since parsing is
        // usually one-and-done, it's wasteful to maintain this cache in perpetuity.
        final Map<String, NestedPropertyKey> cachedKeys = new HashMap<>();
        for (final NestedPropertyKey npk : values()) {
            cachedKeys.put(npk.getNestedKey(), npk);
        }

        for (final String param : params) {
            if (param != null) {
                final String[] tokens = param.split("=");
                if (tokens.length == 2) {
                    final NestedPropertyKey npk = cachedKeys.get(tokens[0]);
                    if (npk != null) {
                        npk.visit(processor, tokens[1]);
                    } else {
                        log.warn("Unsupported nested key: {}", param);
                    }
                } else if (tokens.length > 2) {
                    log.warn("Malformed nested key: {}", param);
                } else {
                    log.info("Parameter is not a nested key: {}", param);
                }
            }
        }
    }
}
