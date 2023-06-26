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

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class ThrottlingReporterTest {

    private static final String SHARD_ID = "Shard-001";

    @Mock
    private Logger throttleLog;

    @Test
    public void testLessThanMaxThrottles() {
        ThrottlingReporter reporter = new LogTestingThrottingReporter(5, SHARD_ID);
        reporter.throttled();
        verify(throttleLog).warn(anyString());
        verify(throttleLog, never()).error(anyString());
    }

    @Test
    public void testMoreThanMaxThrottles() {
        ThrottlingReporter reporter = new LogTestingThrottingReporter(1, SHARD_ID);
        reporter.throttled();
        reporter.throttled();
        verify(throttleLog).warn(anyString());
        verify(throttleLog).error(anyString());
    }

    @Test
    public void testSuccessResetsErrors() {
        ThrottlingReporter reporter = new LogTestingThrottingReporter(1, SHARD_ID);
        reporter.throttled();
        reporter.throttled();
        reporter.throttled();
        reporter.throttled();
        reporter.success();
        reporter.throttled();
        verify(throttleLog, times(2)).warn(anyString());
        verify(throttleLog, times(3)).error(anyString());
    }

    private class LogTestingThrottingReporter extends ThrottlingReporter {

        public LogTestingThrottingReporter(int maxConsecutiveWarnThrottles, String shardId) {
            super(maxConsecutiveWarnThrottles, shardId);
        }

        @Override
        protected Logger getLog() {
            return throttleLog;
        }
    }

}