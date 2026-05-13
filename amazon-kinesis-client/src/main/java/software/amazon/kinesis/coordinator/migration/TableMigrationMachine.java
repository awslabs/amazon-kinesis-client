package software.amazon.kinesis.coordinator.migration;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.leader.DynamoDBLockBasedLeaderDecider;

@Slf4j
public class TableMigrationMachine {

    public static final String TABLE_MIGRATION_STATUS_ATTRIBUTE_NAME = "tableMigration";

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

    /** TODO: if leaderDecider's lastLeaderResult is false, cancel the scheduled update or no-op */
    public void update(DynamoDBLockBasedLeaderDecider leaderDecider) {
        for (int i = 0; i < 100; i++) {
            log.info("HELLO, WORLD! WE'RE DOING A SCHEDULED UPDATE BECAUSE WE'RE THE LEADER!!!");
        }

        switch (leaderDecider.tableMigrationStatus) {
            default:
            case DEPLOYING: {
                break;
            }
            case PENDING: {
                break;
            }
            case COMPLETE: {
                break;
            }
        }
    }

    /**
     * Maps the two strings to their States by name (assumes valid input or null) and returns the difference between their ordinals.
     * @param status - the first string, should be the name of a States
     * @param other - the second string, should be the name of a States
     * @return the difference in the ordinals (similar to compareTo() impl)
     */
    public static int compareStatuses(String status, String other) {
        return States.valueOf(status).ordinal() - States.valueOf(other).ordinal();
    }
}
