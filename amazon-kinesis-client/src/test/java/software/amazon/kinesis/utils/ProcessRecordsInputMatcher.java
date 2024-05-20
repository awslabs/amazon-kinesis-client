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

package software.amazon.kinesis.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import lombok.Data;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

public class ProcessRecordsInputMatcher extends TypeSafeDiagnosingMatcher<ProcessRecordsInput> {

    private final ProcessRecordsInput template;
    private final Map<String, MatcherData> matchers = new HashMap<>();

    public static ProcessRecordsInputMatcher eqProcessRecordsInput(ProcessRecordsInput expected) {
        return new ProcessRecordsInputMatcher(expected);
    }

    public ProcessRecordsInputMatcher(ProcessRecordsInput template) {
        matchers.put("cacheEntryTime", nullOrEquals(template.cacheEntryTime(), ProcessRecordsInput::cacheEntryTime));
        matchers.put("checkpointer", nullOrEquals(template.checkpointer(), ProcessRecordsInput::checkpointer));
        matchers.put("isAtShardEnd", nullOrEquals(template.isAtShardEnd(), ProcessRecordsInput::isAtShardEnd));
        matchers.put(
                "millisBehindLatest",
                nullOrEquals(template.millisBehindLatest(), ProcessRecordsInput::millisBehindLatest));
        matchers.put("records", nullOrEquals(template.records(), ProcessRecordsInput::records));
        matchers.put("childShards", nullOrEquals(template.childShards(), ProcessRecordsInput::childShards));

        this.template = template;
    }

    private static MatcherData nullOrEquals(Object item, Function<ProcessRecordsInput, ?> accessor) {
        if (item == null) {
            return new MatcherData(nullValue(), accessor);
        }
        return new MatcherData(equalTo(item), accessor);
    }

    @Override
    protected boolean matchesSafely(ProcessRecordsInput item, Description mismatchDescription) {
        return matchers.entrySet().stream()
                .filter(e -> e.getValue().matcher.matches(e.getValue().accessor.apply(item)))
                .anyMatch(e -> {
                    mismatchDescription.appendText(e.getKey()).appendText(" ");
                    e.getValue().matcher.describeMismatch(e.getValue().accessor.apply(item), mismatchDescription);
                    return true;
                });
    }

    @Override
    public void describeTo(Description description) {
        matchers.forEach((k, v) -> description.appendText(k).appendText(" ").appendDescriptionOf(v.matcher));
    }

    @Data
    private static class MatcherData {
        private final Matcher<?> matcher;
        private final Function<ProcessRecordsInput, ?> accessor;
    }
}
