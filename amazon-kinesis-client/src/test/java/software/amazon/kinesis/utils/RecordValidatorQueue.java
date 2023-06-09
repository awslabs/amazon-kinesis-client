package software.amazon.kinesis.utils;

import lombok.extern.slf4j.Slf4j;
import java.util.*;

@Slf4j
public class RecordValidatorQueue {

    HashMap<String, List<String>> dict = new HashMap<>();

    public void add( String shardId, String data ) {
        if ( dict.containsKey( shardId ) ) {
            // Just add the data to this item
            List<String> oldVal = dict.get( shardId );
            oldVal.add( data );
            dict.put( shardId, oldVal );
        } else {
            List<String> newVal = new ArrayList<>();
            newVal.add( data );
            dict.put( shardId, newVal );
        }
    }

    public int validateRecords( int trueTotalShardCount ) {
        // Validate that each List in the HashMap has data records in increasing order

        boolean incOrder = true;
        for ( Map.Entry<String, List<String>> entry : dict.entrySet() ) {
            List<String> recordsPerShard = entry.getValue();
            int prevVal = -1;
            boolean shardIncOrder = true;
            for ( String record : recordsPerShard ) {
                int nextVal = Integer.parseInt( record );
                if ( prevVal > nextVal ) {
                    log.error("The records are not in increasing order. Saw record data {} before {}.", prevVal, nextVal );
                    shardIncOrder = false;
                }
                prevVal = nextVal;
            }
            if ( !shardIncOrder ) {
                incOrder = false;
            }
        }

        // If this is true, then there was some record that was processed out of order
        if ( !incOrder ) {
            return -1;
        }

        // Validate that no records are missing over all shards
        int totalShardCount = 0;
        for ( Map.Entry<String, List<String>> entry : dict.entrySet() ) {
            List<String> recordsPerShard = entry.getValue();
            Set<String> noDupRecords = new HashSet<String>( recordsPerShard );
            totalShardCount += noDupRecords.size();
        }

        // If this is true, then there was some record that was missed during processing.
        if ( totalShardCount != trueTotalShardCount ) {
            log.error( "Failed to get correct number of records processed. Should be {} but was {}", trueTotalShardCount, totalShardCount );
            return -2;
        }
        return 0;
    }

}
