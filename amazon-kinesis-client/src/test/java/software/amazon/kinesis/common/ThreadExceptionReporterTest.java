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
package software.amazon.kinesis.common;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import ch.qos.logback.classic.Level;
import org.junit.Before;
import org.junit.Test;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class ThreadExceptionReporterTest {

    @Before
    public void before() {
        ThreadExceptionReporterAppender.loggingEvents.clear();
    }

    @Test
    public void exceptionReportedTest() {
        ThreadExceptionReporter reporter = new ThreadExceptionReporter("test");
        reporter.uncaughtException(Thread.currentThread(), new RuntimeException("Test"));

        assertThat(ThreadExceptionReporterAppender.loggingEvents.size(), equalTo(1));
        ILoggingEvent event = ThreadExceptionReporterAppender.loggingEvents.get(0);
        assertThat(event.getFormattedMessage(), equalTo("[test] Thread main (id-1) died unexpectedly: Test"));
        assertThat(event.getLevel(), equalTo(Level.ERROR));
        assertThat(event.getThrowableProxy().getClassName(), equalTo(RuntimeException.class.getName()));
        assertThat(event.getThrowableProxy().getMessage(), equalTo("Test"));
    }

    public static class ThreadExceptionReporterAppender extends AppenderBase<ILoggingEvent> {

        static List<ILoggingEvent> loggingEvents = new ArrayList<>();

        @Override
        protected void append(ILoggingEvent eventObject) {
            loggingEvents.add(eventObject);
        }
    }
}