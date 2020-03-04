package software.amazon.kinesis.leases;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.Validate;

import java.util.Objects;

import static com.google.common.base.Verify.verifyNotNull;

@Setter
@NoArgsConstructor
@Getter
@Accessors(fluent = true)
public class MultiStreamLease extends Lease {

    @NonNull private String streamIdentifier;
    @NonNull private String shardId;

    public MultiStreamLease(Lease other) {
        super(other);
        MultiStreamLease casted = validateAndCast(other);
        streamIdentifier(casted.streamIdentifier);
        shardId(casted.shardId);
    }

    @Override
    public void update(Lease other) {
        MultiStreamLease casted = validateAndCast(other);
        super.update(casted);
        streamIdentifier(casted.streamIdentifier);
        shardId(casted.shardId);
    }

    public static String getLeaseKey(String streamName, String shardId) {
        verifyNotNull(streamName, "streamName should not be null");
        verifyNotNull(shardId, "shardId should not be null");
        return streamName + ":" + shardId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), streamIdentifier);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof MultiStreamLease)) {
            return false;
        }
        MultiStreamLease other = (MultiStreamLease) obj;
        if (streamIdentifier == null) {
            if (other.streamIdentifier != null) {
                return false;
            }
        } else if (!streamIdentifier.equals(other.streamIdentifier)) {
            return false;
        }
        return true;
    }

    /**
     * Returns a deep copy of this object. Type-unsafe - there aren't good mechanisms for copy-constructing generics.
     *
     * @return A deep copy of this object.
     */
    @Override
    public MultiStreamLease copy() {
        return new MultiStreamLease(this);
    }

    /**
     * Validate and cast the lease to MultiStream lease
     * @param lease
     * @return MultiStreamLease
     */
    public static MultiStreamLease validateAndCast(Lease lease) {
        Validate.isInstanceOf(MultiStreamLease.class, lease);
        return (MultiStreamLease) lease;
    }

}
