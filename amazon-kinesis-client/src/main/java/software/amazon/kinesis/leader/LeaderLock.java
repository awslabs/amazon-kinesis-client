package software.amazon.kinesis.leader;

import java.util.Collections;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.CoordinatorState;
import software.amazon.kinesis.leases.EntityType;

/**
 * Data model of the KCL Lock. This is used to elect a leader among
 * the worker which can then perform tasks that require to be done
 * once for the whole cluster: such as detecting new and deleted
 * streams in MultiStreamMode, running LeaseAssignmentManager etc.
 */
@Getter
@ToString(callSuper = true)
@Slf4j
@KinesisClientInternalApi
public class LeaderLock extends CoordinatorState {

    public static final String LEADER_HASH_KEY = "Leader";

    public LeaderLock() {
        super(LEADER_HASH_KEY, EntityType.CoordinatorStateType.LEADER_LOCK, Collections.emptyMap());
    }
}
