package software.amazon.kinesis.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

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

    public RecordValidationStatus validateRecords(int expectedRecordCount) {

        // Validate that each List in the HashMap has data records in increasing order
        for (Map.Entry<String, List<String>> entry : dict.entrySet()) {
            List<String> recordsPerShard = entry.getValue();
            int prevVal = -1;
            for (String record : recordsPerShard) {
                int nextVal = Integer.parseInt(record);
                if (prevVal > nextVal) {
                    log.error(
                            "The records are not in increasing order. Saw record data {} before {}.", prevVal, nextVal);
                    return RecordValidationStatus.OUT_OF_ORDER;
                }
                prevVal = nextVal;
            }
        }

        // Validate that no records are missing over all shards
        int actualRecordCount = 0;
        for (Map.Entry<String, List<String>> entry : dict.entrySet()) {
            List<String> recordsPerShard = entry.getValue();
            Set<String> noDupRecords = new HashSet<String>(recordsPerShard);
            actualRecordCount += noDupRecords.size();
        }

        // If this is true, then there was some record that was missed during processing.
        if (actualRecordCount != expectedRecordCount) {
            log.error(
                    "Failed to get correct number of records processed. Should be {} but was {}",
                    expectedRecordCount,
                    actualRecordCount);
            return RecordValidationStatus.MISSING_RECORD;
        }

        // Record validation succeeded.
        return RecordValidationStatus.NO_ERROR;
    }
}
