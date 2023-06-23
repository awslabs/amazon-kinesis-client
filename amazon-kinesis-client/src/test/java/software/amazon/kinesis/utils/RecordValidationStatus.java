package software.amazon.kinesis.utils;

/**
 * Possible outcomes for record validation in RecordValidatorQueue
 */
public enum RecordValidationStatus {
    OUT_OF_ORDER,
    MISSING_RECORD,
    NO_ERROR
}
