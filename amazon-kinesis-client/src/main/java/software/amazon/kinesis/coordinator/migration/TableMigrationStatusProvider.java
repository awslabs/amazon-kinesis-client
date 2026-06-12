package software.amazon.kinesis.coordinator.migration;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Provides the table migration status using which dependent classes
 * will decide whether to read from Lease Table or Legacy table or
 * both and merge the results.
 *
 * The methods and internal state is guarded for concurrent access to allow
 * both all DAO layers to access the TableMode concurrently while
 * it could be dynamically updated.
 */
@KinesisClientInternalApi
@Slf4j
@ThreadSafe
@NoArgsConstructor
public class TableMigrationStatusProvider {

    private TableMigrationStatus currentMode = TableMigrationStatus.TABLE_MIGRATION_STATUS_UNKNOWN;
    private boolean initialized = false;
    private boolean dynamicModeChangeSupportNeeded;

    /**
     * Specify whether both tables should be initialized to
     * support dynamically changing table migration status.
     * @return true if table migration status can change dynamically
     *         false otherwise.
     */
    public synchronized boolean dynamicModeChangeSupportNeeded() {
        return dynamicModeChangeSupportNeeded;
    }

    /**
     * Provide the current table migration status in which KCL should access entities from DDB
     * storage. Returns UNKNOWN if not yet initialized.
     * @return  the current table migration status
     */
    public synchronized TableMigrationStatus getTableMigrationStatus() {
        return currentMode;
    }

    synchronized void initialize(final boolean dynamicModeChangeSupportNeeded, final TableMigrationStatus mode) {
        if (!initialized) {
            log.info("Initializing dynamicModeChangeSupportNeeded {} mode {}", dynamicModeChangeSupportNeeded, mode);
            this.dynamicModeChangeSupportNeeded = dynamicModeChangeSupportNeeded;
            this.currentMode = mode;
            this.initialized = true;
            return;
        }
        log.info(
                "Already initialized dynamicModeChangeSupportNeeded {} mode {}. Ignoring new values {}, {}",
                this.dynamicModeChangeSupportNeeded,
                this.currentMode,
                dynamicModeChangeSupportNeeded,
                mode);
    }

    synchronized void updateTableMigrationStatus(final TableMigrationStatus mode) {
        if (!initialized) {
            throw new IllegalStateException("Cannot change mode before initializing");
        }
        if (dynamicModeChangeSupportNeeded) {
            log.info("Changing table migration status from {} to {}", currentMode, mode);
            this.currentMode = mode;
            return;
        }
        throw new IllegalStateException(String.format(
                "TableMigrationStatus already initialized to %s cannot" + " change to %s", this.currentMode, mode));
    }
}
