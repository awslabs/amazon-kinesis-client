package software.amazon.kinesis.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class that maintains a dictionary that maps shard IDs to a list of records
 * that are processed by that shard.
 * Validation ensures that
 * 1. The records processed by each shard are in increasing order (duplicates allowed)
 * 2. The total number of unique records processed is equal to the number of records put on the stream
 */
@Slf4j
public class RecordValidatorQueue {

    private final ConcurrentHashMap<String, List<String>> dict = new ConcurrentHashMap<>();

    public void add(String shardId, String data) {
        final List<String> values = dict.computeIfAbsent(shardId, key -> new ArrayList<>());
        values.add(data);
    }

    public RecordValidationStatus validateRecords(int trueTotalShardCount) {
        /**
         * Validate that each List in the HashMap has data records in increasing order
         */
        boolean incOrder = true;
        for (Map.Entry<String, List<String>> entry : dict.entrySet()) {
            List<String> recordsPerShard = entry.getValue();
            int prevVal = -1;
            boolean shardIncOrder = true;
            for (String record : recordsPerShard) {
                int nextVal = Integer.parseInt(record);
                if (prevVal > nextVal) {
                    log.error("The records are not in increasing order. Saw record data {} before {}.", prevVal, nextVal);
                    shardIncOrder = false;
                }
                prevVal = nextVal;
            }
            if (!shardIncOrder) {
                incOrder = false;
                break;
            }
        }

        /**
         * If this is true, then there was some record that was processed out of order
         */
        if (!incOrder) {
            return RecordValidationStatus.OUT_OF_ORDER;
        }

        /**
         * Validate that no records are missing over all shards
         */
        int totalShardCount = 0;
        for (Map.Entry<String, List<String>> entry : dict.entrySet()) {
            List<String> recordsPerShard = entry.getValue();
            Set<String> noDupRecords = new HashSet<String>(recordsPerShard);
            totalShardCount += noDupRecords.size();
        }

        /**
         * If this is true, then there was some record that was missed during processing.
         */
        if (totalShardCount != trueTotalShardCount) {
            log.error("Failed to get correct number of records processed. Should be {} but was {}", trueTotalShardCount, totalShardCount);
            return RecordValidationStatus.MISSING_RECORD;
        }

        /**
         * Record validation succeeded.
         */
        return RecordValidationStatus.NO_ERROR;
    }

}