package software.amazon.kinesis.lifecycle;

import software.amazon.kinesis.leases.ShardInfo;

public class NoOpTaskExecutionListener implements TaskExecutionListener {
    /**
     * Empty constructor for NoOp Shard Prioritization.
     */
    public NoOpTaskExecutionListener() {

    }

    @Override
    public void onTaskBegin(ConsumerState state, ShardInfo shardInfo) {

    }

    @Override
    public void onTaskEnd(ConsumerState state, ShardInfo shardInfo) {

    }
}
