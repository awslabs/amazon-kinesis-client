/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.leader;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.LeaderDecider;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;

import static java.util.Objects.nonNull;

/**
 * MigrationAdaptiveLeaderDecider that wraps around the actual LeaderDecider which can dynamically
 * change based on the MigrationStateMachine.
 */
@Slf4j
@KinesisClientInternalApi
@ThreadSafe
public class MigrationAdaptiveLeaderDecider implements LeaderDecider {

    private final MetricsFactory metricsFactory;
    private LeaderDecider currentLeaderDecider;

    public MigrationAdaptiveLeaderDecider(final MetricsFactory metricsFactory) {
        this.metricsFactory = metricsFactory;
    }

    @Override
    public synchronized Boolean isLeader(final String workerId) {
        if (currentLeaderDecider == null) {
            throw new IllegalStateException("LeaderDecider uninitialized");
        }

        final MetricsScope scope =
                MetricsUtil.createMetricsWithOperation(metricsFactory, METRIC_OPERATION_LEADER_DECIDER);
        try {
            publishSelectedLeaderDeciderMetrics(scope, currentLeaderDecider);
            return currentLeaderDecider.isLeader(workerId);
        } finally {
            MetricsUtil.endScope(scope);
        }
    }

    private static void publishSelectedLeaderDeciderMetrics(
            final MetricsScope scope, final LeaderDecider leaderDecider) {
        scope.addData(
                String.format(leaderDecider.getClass().getSimpleName()), 1D, StandardUnit.COUNT, MetricsLevel.DETAILED);
    }

    public synchronized void updateLeaderDecider(final LeaderDecider leaderDecider) {
        if (currentLeaderDecider != null) {
            currentLeaderDecider.shutdown();
            log.info(
                    "Updating leader decider dynamically from {} to {}",
                    this.currentLeaderDecider.getClass().getSimpleName(),
                    leaderDecider.getClass().getSimpleName());
        } else {
            log.info(
                    "Initializing dynamic leader decider with {}",
                    leaderDecider.getClass().getSimpleName());
        }
        currentLeaderDecider = leaderDecider;
        currentLeaderDecider.initialize();
    }

    @Override
    public void shutdown() {
        if (nonNull(currentLeaderDecider)) {
            log.info("Shutting down current {}", currentLeaderDecider.getClass().getSimpleName());
            currentLeaderDecider.shutdown();
            currentLeaderDecider = null;
        } else {
            log.info("LeaderDecider has already been shutdown");
        }
    }
}
