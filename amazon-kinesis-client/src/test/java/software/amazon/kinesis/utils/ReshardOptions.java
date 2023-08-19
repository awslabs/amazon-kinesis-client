package software.amazon.kinesis.utils;

/**
 * Specifies the types of resharding possible in integration tests
 * Split doubles the number of shards.
 * Merge halves the number of shards.
 */
public enum ReshardOptions {
    SPLIT {
        public int calculateShardCount(int currentShards) {
            return (int) (2.0 * currentShards);
        }
    },
    MERGE {
        public int calculateShardCount(int currentShards) {
            return (int) (0.5 * currentShards);
        }
    };

    public abstract int calculateShardCount(int currentShards);
}
