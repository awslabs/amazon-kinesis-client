/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.kinesis.retrieval;

import org.junit.Test;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertThat;

@Slf4j
public class AWSExceptionManagerTest {

    @Test
    public void testSpecificException() {
        AWSExceptionManager manager = new AWSExceptionManager();
        final String EXPECTED_HANDLING_MARKER = "Handled-TestException";

        manager.add(TestException.class, t -> {
            log.info("Handling test exception: {} -> {}", t.getMessage(), t.getAdditionalMessage());
            return new RuntimeException(EXPECTED_HANDLING_MARKER, t);
        });

        TestException te = new TestException("Main Mesage", "Sub Message");


        RuntimeException converted = manager.apply(te);

        assertThat(converted, isA(RuntimeException.class));
        assertThat(converted.getMessage(), equalTo(EXPECTED_HANDLING_MARKER));
        assertThat(converted.getCause(), equalTo(te));

    }

    @Test
    public void testParentException() {
        AWSExceptionManager manager = new AWSExceptionManager();
        final String EXPECTED_HANDLING_MARKER = "Handled-IllegalStateException";
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
        final String EXPECTED_HANDLING_MARKER = "Handled-Default";
        AWSExceptionManager manager = new AWSExceptionManager().defaultFunction(i -> new RuntimeException(EXPECTED_HANDLING_MARKER, i));

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
        AWSExceptionManager manager = new AWSExceptionManager();

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