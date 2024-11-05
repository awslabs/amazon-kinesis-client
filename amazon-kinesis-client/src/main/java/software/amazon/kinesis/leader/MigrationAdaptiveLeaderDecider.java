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
