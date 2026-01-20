package software.amazon.kinesis.coordinator.streamInfo;

/**
 * DISABLED: No metadata tracking or lifecycle control
 * TRACK_ONLY: Populate metadata table, but no lifecycle control
 */
public enum StreamInfoMode {
    DISABLED,
    TRACK_ONLY
}
