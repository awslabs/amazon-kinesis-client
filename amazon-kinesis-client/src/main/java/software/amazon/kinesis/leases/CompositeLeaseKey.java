package software.amazon.kinesis.leases;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.Optional;

public class CompositeLeaseKey {

//    private static final String LEASE_TOKEN_SEPERATOR = ":";
//
//    private String streamName;
//
//    @Getter
//    private String shardId;
//
//    public CompositeLeaseKey(String shardId) {
//        this(null, shardId);
//    }
//
//    public CompositeLeaseKey(String streamName, String shardId) {
//        this.streamName = streamName;
//        this.shardId = shardId;
//    }
//
//    public Optional<String> getStreamName() {
//        return Optional.ofNullable(streamName);
//    }
//
//    public String getLeaseKey(boolean isMultiStreamingEnabled) {
//        Validate.isTrue(!(isMultiStreamingEnabled && StringUtils.isEmpty(streamName)),
//                "Empty stream name found while multiStreaming is enabled");
//        return isMultiStreamingEnabled ? StringUtils.joinWith(LEASE_TOKEN_SEPERATOR, streamName, shardId) : shardId;
//    }
//
//    public static CompositeLeaseKey getLeaseKey(String leaseKey) {
//        Validate.notNull(leaseKey);
//        String leaseTokens[] = leaseKey.split(LEASE_TOKEN_SEPERATOR);
//        Validate.inclusiveBetween(1, 2, leaseTokens.length);
//        return leaseTokens.length == 2 ?
//                new CompositeLeaseKey(leaseTokens[0], leaseTokens[1]) :
//                new CompositeLeaseKey(leaseTokens[0]);
//    }

}
