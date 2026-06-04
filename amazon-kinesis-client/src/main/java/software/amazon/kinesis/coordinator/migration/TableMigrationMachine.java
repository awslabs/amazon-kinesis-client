package software.amazon.kinesis.coordinator.migration;

import java.time.Instant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.worker.metricstats.WorkerMetricStats;

/**
 * Static class that stores global variables related to the state of the table migration and exposes
 * a method to decide and return the next state transition.
 */
@Slf4j
public class TableMigrationMachine {

    public static final String TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME = "tableMigration";
    public static final String TABLE_MIGRATION_FEATURE_NAME = "SINGLE_TABLE_MIGRATION";
    public static final int TABLE_MIGRATION_FEATURE_INDEX =
            WorkerMetricStats.Features.valueOf(TABLE_MIGRATION_FEATURE_NAME).ordinal();

    private static final long INIT_TO_DEPLOYED_BAKE_TIME = 60L * 3L; // 1 hour, in seconds (set to 3m for testing)
    private static final long PENDING_TO_COMPLETE_BAKE_TIME = 60L * 3L; // 1 hour, in seconds (set to 3m for testing)

    @Getter
    @Setter
    private static volatile int minSupportCode = 0;

    @Getter
    @Setter
    private static volatile boolean workerStatsTableFoundEmpty = false;

    private TableMigrationMachine() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated!");
    }

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

    public static synchronized States update(States tableMigrationStatus, long steadySinceEpoch) {
        long epochSecond = Instant.now().getEpochSecond();

        States newTableMigrationStatus = tableMigrationStatus;
        switch (tableMigrationStatus) {
            default:
            case INIT: {
                if (minSupportCode >= TABLE_MIGRATION_FEATURE_INDEX
                        && steadySinceEpoch + INIT_TO_DEPLOYED_BAKE_TIME <= epochSecond) {
                    log.info("All workers have table migration support deployed, setting status to deployed");
                    newTableMigrationStatus = States.DEPLOYED;
                }
                break;
            }
            case DEPLOYED: {
                // no-op; DEPLOYED -> PENDING transition happens through non-leader on second-phase deployment
                log.info("Waiting for second phase deployment to begin, cannot create lease table leader lock yet");
                break;
            }
            case PENDING: {
                if (workerStatsTableFoundEmpty && (steadySinceEpoch + PENDING_TO_COMPLETE_BAKE_TIME <= epochSecond)) {
                    log.info("Worker stats table found empty, setting table migration status to complete");
                    newTableMigrationStatus = States.COMPLETE;
                    // TODO: cancel sync to coordinator table scheduled update here
                }
                break;
            }
            case COMPLETE: {
                // no-op -> in future could implement rollback detection if status manually set back to PENDING
                break;
            }
        }
        return newTableMigrationStatus;
    }
}
