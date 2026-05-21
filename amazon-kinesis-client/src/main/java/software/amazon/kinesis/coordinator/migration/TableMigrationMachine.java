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

    private static final long INIT_TO_DEPLOYED_BAKE_TIME = 60L * 3L; // 1 hour, in seconds (set to 3m for testing)
    private static final long PENDING_TO_COMPLETE_BAKE_TIME = 60L * 3L; // 1 hour, in seconds (set to 3m for testing)

    private int minSupportCode = 0;
    private Long steadySinceEpoch = null;
    private States tableMigrationStatus = null;
    private boolean workerStatsTableFoundEmpty = false;

    /**
     * The different states the multi-to-single table migration could be in:
     *  INIT -> code needs to be deployed everywhere before we can safely start the migration
     *  DEPLOYING -> leader knows the code is deployed everywhere, waiting for second phase deployment to begin
     *  PENDING -> table migration is in progress, workers are moving worker stats over
     *  COMPLETE -> all workers are using lease table for everything
     */
    @RequiredArgsConstructor
    public enum States {
        INIT("INIT"),
        DEPLOYED("DEPLOYED"),
        PENDING("PENDING"),
        COMPLETE("COMPLETE");

        @Getter
        private final String name;
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

    private boolean copyFromLeaderDecider(DynamoDBLockBasedLeaderDecider leaderDecider) {
        steadySinceEpoch = leaderDecider.steadySinceEpoch;

        if (tableMigrationStatus != leaderDecider.tableMigrationStatus) {
            tableMigrationStatus = leaderDecider.tableMigrationStatus;
            return true;
        }
        return false;
    }

    public void update(DynamoDBLockBasedLeaderDecider leaderDecider) {
        boolean updated = copyFromLeaderDecider(leaderDecider);
        long epochSecond = Instant.now().getEpochSecond();

        switch (tableMigrationStatus) {
            default:
            case INIT: {
                if (minSupportCode >= TABLE_MIGRATION_FEATURE_INDEX
                        && steadySinceEpoch + INIT_TO_DEPLOYED_BAKE_TIME <= epochSecond) {
                    // all workers support feature and bake time complete -> move to PENDING
                    log.info("All workers have table migration support deployed, setting status to deployed");
                    setTableMigrationStatus(States.DEPLOYED, leaderDecider);
                }
                break;
            }
            case DEPLOYED: {
                // no-op; DEPLOYED -> PENDING transition happens through non-leader on second-phase deployment
                log.info("Waiting for second phase deployment to begin, cannot create lease table leader lock yet");
                break;
            }
            case PENDING: {
                if (updated) {
                    // update was not made by leader -> second-phase deployment must have started
                    // copy coordinator states from coordinator table to lease table; try every 10s until success
                    leaderDecider.copyCoordinatorStatesToLeaseTable(10000L);
                }
                if (workerStatsTableFoundEmpty && (steadySinceEpoch + PENDING_TO_COMPLETE_BAKE_TIME <= epochSecond)) {
                    // worker stats table was empty and bake time complete -> move to COMPLETE
                    log.info("Worker stats table found empty, setting table migration status to complete");
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
