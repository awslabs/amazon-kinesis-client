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
package software.amazon.kinesis.multilang.auth;

import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KclSTSAssumeRoleSessionCredentialsProviderTest {

    private static final String ARN = "arn";
    private static final String SESSION_NAME = "sessionName";

    /**
     * Test that the constructor doesn't throw an out-of-bounds exception if
     * there are no parameters beyond the required ARN and session name.
     */
    @Test
    public void testConstructorWithoutOptionalParams() {
        new KclStsAssumeRoleCredentialsProvider(new String[] {ARN, SESSION_NAME});
    }

    @Test
    public void testAcceptEndpoint() {
        // discovered exception during e2e testing; therefore, this test is
        // to simply verify the constructed STS client doesn't go *boom*
        final KclStsAssumeRoleCredentialsProvider provider = new KclStsAssumeRoleCredentialsProvider(ARN, SESSION_NAME);
        provider.acceptEndpoint("endpoint", "us-east-1");
    }

    @Test
    public void testVarArgs() {
        for (final String[] varargs : Arrays.asList(
                new String[] {ARN, SESSION_NAME, "externalId=eid", "foo"},
                new String[] {ARN, SESSION_NAME, "foo", "externalId=eid"})) {
            final VarArgsSpy provider = new VarArgsSpy(varargs);
            assertEquals("eid", provider.externalId);
        }
    }

    private static class VarArgsSpy extends KclStsAssumeRoleCredentialsProvider {

        private String externalId;

        public VarArgsSpy(String[] args) {
            super(args);
        }

        @Override
        public void acceptExternalId(final String externalId) {
            this.externalId = externalId;
            super.acceptExternalId(externalId);
        }
    }
}
