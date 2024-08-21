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

package software.amazon.kinesis.multilang.messages;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

public class JsonFriendlyRecordTest {

    private KinesisClientRecord kinesisClientRecord;

    @Test
    public void testRecordHandlesNullData() {
        kinesisClientRecord = defaultRecord().data(null).build();
        JsonFriendlyRecord jsonFriendlyRecord = JsonFriendlyRecord.fromKinesisClientRecord(kinesisClientRecord);

        assertThat(jsonFriendlyRecord, equivalentTo(kinesisClientRecord));
    }

    @Test
    public void testRecordHandlesNoByteArrayBuffer() {
        byte[] expected = new byte[] {1, 2, 3, 4};

        ByteBuffer expectedBuffer = ByteBuffer.allocateDirect(expected.length);

        expectedBuffer.put(expected);
        expectedBuffer.rewind();

        kinesisClientRecord = defaultRecord().data(expectedBuffer).build();
        JsonFriendlyRecord jsonFriendlyRecord = JsonFriendlyRecord.fromKinesisClientRecord(kinesisClientRecord);

        expectedBuffer.rewind();
        assertThat(jsonFriendlyRecord, equivalentTo(kinesisClientRecord));
    }

    @Test
    public void testRecordHandlesArrayByteBuffer() {
        ByteBuffer expected = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        kinesisClientRecord = defaultRecord().data(expected).build();
        JsonFriendlyRecord jsonFriendlyRecord = JsonFriendlyRecord.fromKinesisClientRecord(kinesisClientRecord);

        assertThat(jsonFriendlyRecord, equivalentTo(kinesisClientRecord));
    }

    private static RecordMatcher equivalentTo(KinesisClientRecord expected) {
        return new RecordMatcher(expected);
    }

    private static class RecordMatcher extends TypeSafeDiagnosingMatcher<JsonFriendlyRecord> {

        private final KinesisClientRecord expected;
        private final List<Matcher<?>> matchers;

        private RecordMatcher(KinesisClientRecord expected) {
            this.matchers = Arrays.asList(
                    new FieldMatcher<>(
                            "approximateArrivalTimestamp",
                            equalTo(expected.approximateArrivalTimestamp().toEpochMilli()),
                            JsonFriendlyRecord::getApproximateArrivalTimestamp),
                    new FieldMatcher<>("partitionKey", expected::partitionKey, JsonFriendlyRecord::getPartitionKey),
                    new FieldMatcher<>(
                            "sequenceNumber", expected::sequenceNumber, JsonFriendlyRecord::getSequenceNumber),
                    new FieldMatcher<>(
                            "subSequenceNumber", expected::subSequenceNumber, JsonFriendlyRecord::getSubSequenceNumber),
                    new FieldMatcher<>("data", dataEquivalentTo(expected.data()), JsonFriendlyRecord::getData));

            this.expected = expected;
        }

        @Override
        protected boolean matchesSafely(JsonFriendlyRecord item, Description mismatchDescription) {
            return matchers.stream()
                    .map(m -> {
                        if (!m.matches(item)) {
                            m.describeMismatch(item, mismatchDescription);
                            return false;
                        }
                        return true;
                    })
                    .reduce((l, r) -> l && r)
                    .orElse(true);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("A JsonFriendlyRecord matching ").appendList("(", ", ", ")", matchers);
        }
    }

    private static Matcher<Object> dataEquivalentTo(ByteBuffer expected) {
        if (expected == null) {
            return nullValue();
        } else {
            if (expected.hasArray()) {
                return sameInstance(expected.array());
            } else {
                byte[] contents = new byte[expected.limit()];
                expected.get(contents);
                return equalTo(contents);
            }
        }
    }

    private static class FieldMatcher<ItemT, ClassT> extends TypeSafeDiagnosingMatcher<ClassT> {

        final String fieldName;
        final Matcher<ItemT> matcher;
        final Function<ClassT, ItemT> extractor;

        private FieldMatcher(String fieldName, Supplier<ItemT> expected, Function<ClassT, ItemT> extractor) {
            this(fieldName, equalTo(expected.get()), extractor);
        }

        private FieldMatcher(String fieldName, Matcher<ItemT> matcher, Function<ClassT, ItemT> extractor) {
            this.fieldName = fieldName;
            this.matcher = matcher;
            this.extractor = extractor;
        }

        @Override
        protected boolean matchesSafely(ClassT item, Description mismatchDescription) {
            ItemT actual = extractor.apply(item);
            if (!matcher.matches(actual)) {
                mismatchDescription.appendText(fieldName).appendText(": ");
                matcher.describeMismatch(actual, mismatchDescription);
                return false;
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(fieldName).appendText(": ").appendDescriptionOf(matcher);
        }
    }

    private KinesisClientRecord.KinesisClientRecordBuilder defaultRecord() {
        return KinesisClientRecord.builder()
                .partitionKey("test-partition")
                .sequenceNumber("123")
                .approximateArrivalTimestamp(Instant.now());
    }
}
