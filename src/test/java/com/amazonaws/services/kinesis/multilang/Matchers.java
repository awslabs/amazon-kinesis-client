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
package com.amazonaws.services.kinesis.multilang;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;

public class Matchers {

    public static Matcher<InitializationInput> withInit(InitializationInput initializationInput) {
        return new InitializationInputMatcher(initializationInput);
    }

    public static class InitializationInputMatcher extends TypeSafeDiagnosingMatcher<InitializationInput> {

        private final Matcher<String> shardIdMatcher;
        private final Matcher<ExtendedSequenceNumber> sequenceNumberMatcher;

        public InitializationInputMatcher(InitializationInput input) {
            shardIdMatcher = equalTo(input.getShardId());
            sequenceNumberMatcher = withSequence(input.getExtendedSequenceNumber());
        }

        @Override
        protected boolean matchesSafely(final InitializationInput item, Description mismatchDescription) {

            boolean matches = true;
            if (!shardIdMatcher.matches(item.getShardId())) {
                matches = false;
                shardIdMatcher.describeMismatch(item.getShardId(), mismatchDescription);
            }
            if (!sequenceNumberMatcher.matches(item.getExtendedSequenceNumber())) {
                matches = false;
                sequenceNumberMatcher.describeMismatch(item, mismatchDescription);
            }

            return matches;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("An InitializationInput matching: { shardId: ").appendDescriptionOf(shardIdMatcher)
                    .appendText(", sequenceNumber: ").appendDescriptionOf(sequenceNumberMatcher).appendText(" }");
        }
    }

    public static Matcher<ExtendedSequenceNumber> withSequence(ExtendedSequenceNumber extendedSequenceNumber) {
        if (extendedSequenceNumber == null) {
            return nullValue(ExtendedSequenceNumber.class);
        }
        return new ExtendedSequenceNumberMatcher(extendedSequenceNumber);
    }

    public static class ExtendedSequenceNumberMatcher extends TypeSafeDiagnosingMatcher<ExtendedSequenceNumber> {

        private final Matcher<String> sequenceNumberMatcher;
        private final Matcher<Long> subSequenceNumberMatcher;

        public ExtendedSequenceNumberMatcher(ExtendedSequenceNumber extendedSequenceNumber) {
            sequenceNumberMatcher = equalTo(extendedSequenceNumber.getSequenceNumber());
            subSequenceNumberMatcher = equalTo(extendedSequenceNumber.getSubSequenceNumber());
        }

        @Override
        protected boolean matchesSafely(ExtendedSequenceNumber item, Description mismatchDescription) {

            boolean matches = true;
            if (!sequenceNumberMatcher.matches(item.getSequenceNumber())) {
                matches = false;
                mismatchDescription.appendDescriptionOf(sequenceNumberMatcher);
            }
            if (!subSequenceNumberMatcher.matches(item.getSubSequenceNumber())) {
                matches = false;
                mismatchDescription.appendDescriptionOf(subSequenceNumberMatcher);
            }

            return matches;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("An ExtendedSequenceNumber matching: { sequenceNumber: ")
                    .appendDescriptionOf(sequenceNumberMatcher).appendText(", subSequenceNumber: ")
                    .appendDescriptionOf(subSequenceNumberMatcher);
        }
    }

}
