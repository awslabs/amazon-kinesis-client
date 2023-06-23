package software.amazon.kinesis.utils;

/**
 * Specifies the types of resharding possible in integration tests
 * Split doubles the number of shards.
 * Merge halves the number of shards.
 */
public enum ReshardOptions {
    SPLIT,
    MERGE
}
