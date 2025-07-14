/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.coordinator.assignment.exclusion;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;

@Slf4j
@ThreadSafe
@KinesisClientInternalApi
public class WorkerIdExclusionMonitor implements Runnable {

    private static volatile WorkerIdExclusionMonitor instance;

    private static final long MONITOR_INTERVAL_MILLIS = Duration.ofMinutes(1).toMillis();
    private static final double JITTER_FACTOR = 0.5;
    private static final Duration CLOCK_SKEW_BUFFER_THRESHOLD = Duration.ofMinutes(2);

    private final CoordinatorStateDAO coordinatorStateDAO;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledFuture<?> scheduledFuture;

    private volatile WorkerIdExclusionState currState;
    private volatile WorkerIdExclusionState prevState;

    private WorkerIdExclusionMonitor(
            CoordinatorStateDAO coordinatorStateDAO, ScheduledExecutorService scheduledExecutorService) {
        this.coordinatorStateDAO = coordinatorStateDAO;
        this.scheduledExecutorService = scheduledExecutorService;

        final long jitter = (long) ((new Random()).nextDouble() * MONITOR_INTERVAL_MILLIS * JITTER_FACTOR);

        this.scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(
                this, MONITOR_INTERVAL_MILLIS + jitter, MONITOR_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    public static void create(
            CoordinatorStateDAO coordinatorStateDAO, ScheduledExecutorService scheduledExecutorService) {
        if (instance != null) {
            log.warn("Singleton class should only be instantiated once!");
            return;
            // throw new UnsupportedOperationException("Singleton class should only be instantiated once!");
        } else {
            instance = new WorkerIdExclusionMonitor(coordinatorStateDAO, scheduledExecutorService);
        }
    }

    public static WorkerIdExclusionMonitor getInstance() {
        return instance;
    }

    @Override
    public synchronized void run() {
        try {
            this.prevState = this.currState;
            this.currState = getCurrentState();
        } catch (Exception e) {
            log.error("Caught exception during run! " + e.getMessage());
        }
    }

    public synchronized void shutdown() {
        if (this.scheduledFuture != null) {
            log.info("Cancelling {}", this);
            this.scheduledFuture.cancel(false);
        } else {
            log.info("Monitor {} is not running", this);
        }
    }

    private WorkerIdExclusionState getCurrentState() throws Exception {
        return WorkerIdExclusionState.fromDynamoRecord(getDynamoRecord());
    }

    private Map<String, AttributeValue> getDynamoRecord() throws Exception {
        return this.coordinatorStateDAO.getDynamoRecord(WorkerIdExclusionState.WORKER_ID_EXCLUSION_HASH_KEY);
    }

    public synchronized boolean isExcluded(@NonNull String workerId) {
        return hasActivePattern() && matches(workerId);
    }

    public synchronized boolean hasActivePattern() {
        return this.currState != null && this.currState.getRegex() != null && !isExpired(this.currState);
    }

    public synchronized Pattern getPattern() {
        return this.currState == null ? null : this.currState.getRegex();
    }

    public synchronized boolean hasNewState() {
        return this.currState != null && !this.currState.equals(this.prevState);
    }

    private boolean matches(@NonNull String str) {
        return this.currState.getRegex().pattern().matches(str);
    }

    public static boolean isExpired(@NonNull WorkerIdExclusionState state) {
        return hasElapsed(state.getExpirationInstant().plus(CLOCK_SKEW_BUFFER_THRESHOLD));
    }

    public static boolean hasElapsed(Instant instant) {
        return instant.isBefore(Instant.now());
    }
}
