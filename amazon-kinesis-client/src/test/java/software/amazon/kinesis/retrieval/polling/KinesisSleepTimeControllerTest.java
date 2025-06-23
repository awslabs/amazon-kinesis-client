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

package software.amazon.kinesis.retrieval.polling;

import java.time.Instant;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class KinesisSleepTimeControllerTest {

    private KinesisSleepTimeController controller;
    private final long idleMillisBetweenCalls = 1000L;
    private final Integer recordCount = 10;
    private final Long millisBehindLatest = 5000L;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        controller = new KinesisSleepTimeController();
    }

    @Test
    public void testGetSleepTimeMillisWithNullLastSuccessfulCall() {
        // When lastSuccessfulCall is null, it should return the idleMillisBetweenCalls
        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(null)
                .idleMillisBetweenCalls(idleMillisBetweenCalls)
                .lastRecordsCount(recordCount)
                .lastMillisBehindLatest(millisBehindLatest)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);

        assertEquals(
                "When lastSuccessfulCall is null, should return idleMillisBetweenCalls",
                idleMillisBetweenCalls,
                sleepTime);
    }

    @Test
    public void testGetSleepTimeMillisWhenTimeSinceLastCallLessThanIdleTime() {
        // Create a lastSuccessfulCall time that's recent (less than idleMillisBetweenCalls ago)
        Instant now = Instant.now();
        Instant lastSuccessfulCall = now.minusMillis(500); // 500ms ago

        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(lastSuccessfulCall)
                .idleMillisBetweenCalls(idleMillisBetweenCalls)
                .lastRecordsCount(recordCount)
                .lastMillisBehindLatest(millisBehindLatest)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);

        // Should return the remaining time to wait (idleMillisBetweenCalls - timeSinceLastCall)
        assertTrue(
                "Sleep time should be positive when time since last call is less than idle time",
                sleepTime > 0 && sleepTime <= idleMillisBetweenCalls);

        // The exact value will vary slightly due to execution time, but should be close to 500ms
        long expectedApproxSleepTime = idleMillisBetweenCalls - 500;
        assertTrue(
                "Sleep time should be approximately " + expectedApproxSleepTime + "ms",
                Math.abs(sleepTime - expectedApproxSleepTime) < 100); // Allow for small timing variations
    }

    @Test
    public void testGetSleepTimeMillisWhenTimeSinceLastCallEqualsOrExceedsIdleTime() {
        // Create a lastSuccessfulCall time that's exactly idleMillisBetweenCalls ago
        Instant now = Instant.now();
        Instant lastSuccessfulCall = now.minusMillis(idleMillisBetweenCalls);

        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(lastSuccessfulCall)
                .idleMillisBetweenCalls(idleMillisBetweenCalls)
                .lastRecordsCount(recordCount)
                .lastMillisBehindLatest(millisBehindLatest)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);

        // Should return 0 as we've waited the full idle time
        assertEquals("Sleep time should be 0 when time since last call equals idle time", 0L, sleepTime);

        // Test with time exceeding idle time
        lastSuccessfulCall = now.minusMillis(idleMillisBetweenCalls + 500);
        sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(lastSuccessfulCall)
                .idleMillisBetweenCalls(idleMillisBetweenCalls)
                .lastRecordsCount(recordCount)
                .lastMillisBehindLatest(millisBehindLatest)
                .build();
        sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);

        assertEquals("Sleep time should be 0 when time since last call exceeds idle time", 0L, sleepTime);
    }

    @Test
    public void testGetSleepTimeMillisWithFutureLastSuccessfulCall() {
        // Test with a lastSuccessfulCall in the future (should handle this case gracefully)
        Instant now = Instant.now();
        Instant futureCall = now.plusMillis(500); // 500ms in the future

        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(futureCall)
                .idleMillisBetweenCalls(idleMillisBetweenCalls)
                .lastRecordsCount(recordCount)
                .lastMillisBehindLatest(millisBehindLatest)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);

        // The implementation uses Duration.abs(), so it should treat this the same as 500ms in the past
        assertTrue(
                "Sleep time should be positive when last call time is in the future",
                sleepTime > 0 && sleepTime <= idleMillisBetweenCalls);

        long expectedApproxSleepTime = idleMillisBetweenCalls - 500;
        assertTrue(
                "Sleep time should be approximately " + expectedApproxSleepTime + "ms",
                Math.abs(sleepTime - expectedApproxSleepTime) < 100); // Allow for small timing variations
    }

    @Test
    public void testGetSleepTimeMillisWithDifferentIdleTimes() {
        // Test with different idle times
        Instant now = Instant.now();
        Instant lastSuccessfulCall = now.minusMillis(300);

        // Test with shorter idle time
        long shorterIdleTime = 500L;
        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(lastSuccessfulCall)
                .idleMillisBetweenCalls(shorterIdleTime)
                .lastRecordsCount(recordCount)
                .lastMillisBehindLatest(millisBehindLatest)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);
        assertEquals(
                "Sleep time should be approximately 200ms with 500ms idle time and 300ms elapsed",
                200L,
                sleepTime,
                100);

        // Test with longer idle time
        long longerIdleTime = 2000L;
        sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(lastSuccessfulCall)
                .idleMillisBetweenCalls(longerIdleTime)
                .lastRecordsCount(recordCount)
                .lastMillisBehindLatest(millisBehindLatest)
                .build();
        sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);
        assertEquals(
                "Sleep time should be approximately 1700ms with 2000ms idle time and 300ms elapsed",
                1700L,
                sleepTime,
                100);
    }

    @Test
    public void testGetSleepTimeMillisIgnoresRecordCountAndMillisBehindLatest() {
        // Verify that the implementation ignores lastGetRecordsReturnedRecordsCount and millisBehindLatest parameters
        Instant now = Instant.now();
        Instant lastSuccessfulCall = now.minusMillis(500);

        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(lastSuccessfulCall)
                .idleMillisBetweenCalls(idleMillisBetweenCalls)
                .lastRecordsCount(0)
                .lastMillisBehindLatest(0L)
                .build();
        long sleepTime1 = controller.getSleepTimeMillis(sleepTimeControllerConfig);
        sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(lastSuccessfulCall)
                .idleMillisBetweenCalls(idleMillisBetweenCalls)
                .lastRecordsCount(100)
                .lastMillisBehindLatest(10000L)
                .build();
        long sleepTime2 = controller.getSleepTimeMillis(sleepTimeControllerConfig);

        // Both calls should return approximately the same sleep time since these parameters are ignored
        assertTrue(
                "Sleep time should be the same regardless of record count and millisBehindLatest",
                Math.abs(sleepTime1 - sleepTime2) < 10); // Allow for minimal timing variations
    }

    @Test
    public void testGetSleepTimeMillisWithReducedTpsThresholdAtTip() {
        Instant now = Instant.now();
        long lastSuccessfulCallGap = 500;
        Instant lastSuccessfulCall = now.minusMillis(lastSuccessfulCallGap); // 500ms ago
        long millisBehindLatestThresholdForReducedTps = 1000L;
        long smallMillisBehindLatest = 0L;
        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(lastSuccessfulCall)
                .idleMillisBetweenCalls(idleMillisBetweenCalls)
                .lastRecordsCount(recordCount)
                .lastMillisBehindLatest(smallMillisBehindLatest)
                .millisBehindLatestThresholdForReducedTps(millisBehindLatestThresholdForReducedTps)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);

        // Should give the difference between lastSuccessfulCall and millisBehindLatestThresholdForReducedTps.
        // In this case, the millisBehindLatestThresholdForReducedTps will be the greater wait time, as we are at tip.
        assertTrue(
                "Sleep time should be positive but less than millisBehindLatestThresholdForReducedTps",
                sleepTime > 0 && sleepTime <= millisBehindLatestThresholdForReducedTps);

        // The exact value will vary slightly due to execution time but should be close to 500ms. Absolute value
        // confirms that we are not waiting sum of both wait times and exceeding the expected wait time.
        long expectedApproxSleepTime = millisBehindLatestThresholdForReducedTps - lastSuccessfulCallGap;
        assertTrue(
                "Sleep time should be approximately " + expectedApproxSleepTime + "ms",
                Math.abs(sleepTime - expectedApproxSleepTime) < 100); // Allow for small timing variations
    }

    @Test
    public void testGetSleepTimeMillisWithReducedTpsThresholdBehindTip() {
        Instant now = Instant.now();
        Instant lastSuccessfulCall = now.minusMillis(idleMillisBetweenCalls); // no wait on idleMillisBetweenCalls
        long millisBehindLatestThresholdForReducedTps = 2000L;
        long equalMillisBehindLatest = 2001L;
        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(lastSuccessfulCall)
                .idleMillisBetweenCalls(idleMillisBetweenCalls)
                .lastRecordsCount(recordCount)
                .lastMillisBehindLatest(equalMillisBehindLatest)
                .millisBehindLatestThresholdForReducedTps(millisBehindLatestThresholdForReducedTps)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);

        // Should be no wait time, as we are outside the millisBehindLatest threshold
        assertEquals("Sleep time should be zero", 0, sleepTime);
    }

    @Test
    public void testGetSleepTimeMillisWithReducedTpsThresholdHigherIdleMillisWait() {
        Instant now = Instant.now();
        long lastSuccessfulCallGap = 300;
        Instant lastSuccessfulCall = now.minusMillis(lastSuccessfulCallGap); // 300ms ago
        long millisBehindLatestThresholdForReducedTps = 200L;
        long smallMillisBehindLatest = 0L;
        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(lastSuccessfulCall)
                .idleMillisBetweenCalls(idleMillisBetweenCalls)
                .lastRecordsCount(recordCount)
                .lastMillisBehindLatest(smallMillisBehindLatest)
                .millisBehindLatestThresholdForReducedTps(millisBehindLatestThresholdForReducedTps)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);

        // Should give the difference between idleMillisBetweenCalls and lastSuccessfulCall.
        // In this case, idleMillisBetweenCalls difference is greater than millisBehindLatestThresholdForReducedTps.
        assertTrue(
                "Sleep time should be positive but less than millisBehindLatestThresholdForReducedTps",
                sleepTime > 0 && sleepTime <= idleMillisBetweenCalls);

        // The exact value will vary slightly due to execution time, but should be close to 700ms
        long expectedApproxSleepTime = idleMillisBetweenCalls - lastSuccessfulCallGap;
        assertTrue(
                "Sleep time should be approximately " + expectedApproxSleepTime + "ms",
                Math.abs(sleepTime - expectedApproxSleepTime) < 100);
    }
}
