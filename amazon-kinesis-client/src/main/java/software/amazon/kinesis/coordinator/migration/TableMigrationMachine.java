package software.amazon.kinesis.coordinator.migration;

import java.time.Instant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.leader.DynamoDBLockBasedLeaderDecider;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

@Getter
@Slf4j
public class TableMigrationMachine {

    public static final String TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME = "tableMigration";
    public static final String TABLE_MIGRATION_FEATURE_NAME = "SINGLE_TABLE_MIGRATION";
    public static final int TABLE_MIGRATION_FEATURE_INDEX =
            WorkerMetricStats.Features.valueOf(TABLE_MIGRATION_FEATURE_NAME).ordinal();

    private static final long DEPLOYING_TO_PENDING_BAKE_TIME = 60L * 3L; // 1 hour, in seconds (set to 3m for testing)
    private static final long PENDING_TO_COMPLETE_BAKE_TIME = 60L * 3L; // 1 hour, in seconds (set to 3m for testing)

    private int minSupportCode = 0;
    private Long steadySinceEpoch = null;
    private States tableMigrationStatus = null;
    private boolean workerStatsTableFoundEmpty = false;

    /**
     * The different states the multi-to-single table migration could be in:
     *  DEPLOYING -> code needs to be deployed everywhere
     *  PENDING -> leader knows code is deployed everywhere
     *  COMPLETE -> all workers are using lease table for everything
     */
    @RequiredArgsConstructor
    public enum States {
        DEPLOYING("DEPLOYING"),
        PENDING("PENDING"),
        COMPLETE("COMPLETE");

        @Getter
        private final String name;
    }

    /**
     * If default config option ENABLE is selected, single table migration will proceed.
     * If leader has DISABLE config option, it will not advance to PENDING.
     * If already in PENDING and leader has DISABLE, it will rollback the migration progress.
     */
    public enum Options {
        ENABLE,
        DISABLE
    }

    public synchronized boolean setMinSupportCode(int minSupport) {
        if (minSupportCode != minSupport) {
            minSupportCode = minSupport;
            resetSteadySinceEpoch();
            return true;
        }
        return false;
    }

    public synchronized boolean setWorkerStatsTableFoundEmpty(boolean empty) {
        if (workerStatsTableFoundEmpty != empty) {
            workerStatsTableFoundEmpty = empty;
            resetSteadySinceEpoch();
            return true;
        }
        return false;
    }

    private synchronized boolean setTableMigrationStatus(States status, DynamoDBLockBasedLeaderDecider leaderDecider) {
        if (tableMigrationStatus != status) {
            leaderDecider.tableMigrationStatus = tableMigrationStatus = status;
            resetSteadySinceEpoch(leaderDecider);
            return true;
        }
        return false;
    }

    private synchronized void resetSteadySinceEpoch() {
        steadySinceEpoch = Instant.now().getEpochSecond();
    }

    private synchronized void resetSteadySinceEpoch(DynamoDBLockBasedLeaderDecider leaderDecider) {
        leaderDecider.steadySinceEpoch = steadySinceEpoch = Instant.now().getEpochSecond();
    }

    private void copyFromLeaderDecider(DynamoDBLockBasedLeaderDecider leaderDecider) {
        steadySinceEpoch = leaderDecider.steadySinceEpoch;
        tableMigrationStatus = leaderDecider.tableMigrationStatus;
    }

    public void update(DynamoDBLockBasedLeaderDecider leaderDecider) {
        copyFromLeaderDecider(leaderDecider);

        long epochSecond = Instant.now().getEpochSecond();

        switch (tableMigrationStatus) {
            default:
            case DEPLOYING: {
                if (minSupportCode >= TABLE_MIGRATION_FEATURE_INDEX
                        && steadySinceEpoch + DEPLOYING_TO_PENDING_BAKE_TIME <= epochSecond) {
                    // all workers support feature and bake time complete -> move to PENDING
                    setTableMigrationStatus(States.PENDING, leaderDecider);
                }
                break;
            }
            case PENDING: {
                if (workerStatsTableFoundEmpty && (steadySinceEpoch + PENDING_TO_COMPLETE_BAKE_TIME <= epochSecond)) {
                    // worker stats table was empty and bake time complete -> move to COMPLETE
                    setTableMigrationStatus(States.COMPLETE, leaderDecider);
                    // TODO: cancel sync to coordinator table scheduled update here
                }
                break;
            }
            case COMPLETE: {
                // no-op -> TODO: maybe implement rollback detection (i.e. check if status manually set back to PENDING)
                break;
            }
        }
    }
}
