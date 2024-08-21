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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertThat;

@Slf4j
public class AWSExceptionManagerTest {

    private static final String EXPECTED_HANDLING_MARKER = AWSExceptionManagerTest.class.getSimpleName();

    private final AWSExceptionManager manager = new AWSExceptionManager();

    @Test
    public void testSpecificException() {
        manager.add(TestException.class, t -> {
            log.info("Handling test exception: {} -> {}", t.getMessage(), t.getAdditionalMessage());
            return new RuntimeException(EXPECTED_HANDLING_MARKER, t);
        });

        TestException te = new TestException("Main Message", "Sub Message");

        RuntimeException converted = manager.apply(te);

        assertThat(converted, isA(RuntimeException.class));
        assertThat(converted.getMessage(), equalTo(EXPECTED_HANDLING_MARKER));
        assertThat(converted.getCause(), equalTo(te));
    }

    @Test
    public void testParentException() {
        manager.add(IllegalArgumentException.class, i -> new RuntimeException("IllegalArgument", i));
        manager.add(Exception.class, i -> new RuntimeException("RawException", i));
        manager.add(IllegalStateException.class, i -> new RuntimeException(EXPECTED_HANDLING_MARKER, i));

        TestException testException = new TestException("IllegalStateTest", "Stuff");

        RuntimeException converted = manager.apply(testException);

        assertThat(converted.getMessage(), equalTo(EXPECTED_HANDLING_MARKER));
        assertThat(converted.getCause(), equalTo(testException));
    }

    @Test
    public void testDefaultHandler() {
        manager.defaultFunction(i -> new RuntimeException(EXPECTED_HANDLING_MARKER, i));

        manager.add(IllegalArgumentException.class, i -> new RuntimeException("IllegalArgument", i));
        manager.add(Exception.class, i -> new RuntimeException("RawException", i));
        manager.add(IllegalStateException.class, i -> new RuntimeException("IllegalState", i));

        Throwable t = new StackOverflowError("Whoops");

        RuntimeException converted = manager.apply(t);

        assertThat(converted.getMessage(), equalTo(EXPECTED_HANDLING_MARKER));
        assertThat(converted.getCause(), equalTo(t));
    }

    @Test
    public void testIdHandler() {
        manager.add(IllegalArgumentException.class, i -> new RuntimeException("IllegalArgument", i));
        manager.add(Exception.class, i -> new RuntimeException("RawException", i));
        manager.add(IllegalStateException.class, i -> i);

        TestException te = new TestException("Main Message", "Sub Message");
        RuntimeException converted = manager.apply(te);

        assertThat(converted.getClass(), equalTo(TestException.class));
        assertThat(converted, equalTo(te));
    }

    @Getter
    private static class TestException extends IllegalStateException {

        private final String additionalMessage;

        public TestException(String message, String additionalMessage) {
            super(message);
            this.additionalMessage = additionalMessage;
        }
    }
}
