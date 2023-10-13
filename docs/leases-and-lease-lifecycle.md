# Leases and Lease Lifecycle

Leases are created in, and leveraged by, KCL to manage a stream's shards.
This document should help provide insights into the lease lifecycle.

**Nota bene (N.B.):** while actual shard ids are formatted like `shardId-000000000042`, this document uses `shardId[_-]42` for concision.

## Leases

In KCL, a lease provides a temporal assignment between one Kinesis shard and an assigned worker.
Leases are persistent for the duration of shard processing (detailed later).
However, lease assignment is transient -- leases may be "stolen" by other workers in the same KCL application.

## Lease Table

To persist metadata about lease state (e.g., last read checkpoint, current assigned worker), KCL creates a lease table in [DynamoDB][dynamodb].
Each KCL application will have its own distinct lease table that transcludes the application name.
More information, including schema, is provided at [KCL LeaseTable][kcl-leasetable].

## Lease Assignment

The "life" of a lease closely mirrors the duration that a shard is being processed.

![Activity diagram of KCL shard-to-lease assignments.
shard-0 (lease-0) is unmodified.
shard-1 (lease-1) is split into shard-4 (lease-4) and shard-5 (lease-5).
shard-2 (lease-2) and shard-3 (lease-3) are merged into shard-6 (lease-6).
](images/leases-and-mutations.png)

Specifically, leases are unique to the shard and are not recycled for stream mutations (i.e., split, merge).
Shards created by stream mutations will generate a new lease.

It should be noted that the number of tracked leases may exceed the number of shards.
Per the diagram (above), this can occur when there are stream mutations propagating through KCL.
For example, a 10-shard stream that is split on every shard may temporarily have up-to 30 leases: 10 original + 20 children.

N.B. Leases are uniquely identified by their `leaseKey` which looks vastly different than `lease_X`.
For details on the `leaseKey` format, please see [KCL LeaseTable][kcl-leasetable].

## Lease Lifecycle

Leases follow a relatively simple, progressive state machine:
`DISCOVERY -> CREATION -> PROCESSING -> SHARD_END -> DELETION`

Excluding `SHARD_END`, these phases are illustrative of KCL logic and are not explicitly codified.

1. `DISCOVERY`: KCL [shard-syncing](#lease-syncing) identifies new shards.
Discovered shards may result from:
   * First time starting KCL with an empty lease table.
   * Stream mutations (i.e., split, merge) that create child shards.
   * In multi-stream mode, dynamic discovery of a new stream.
1. `CREATION`: Leases are created 1:1 for each discovered shard, and initialized at the configured initial position.
   * Leases are only created if they are eligible for processing.
   For example, child shards will not have leases created until its parent(s) have reached `SHARD_END`.
1. `PROCESSING`: Leases are processed, and continually updated with new checkpoints.
   * In general, leases spend the majority of their life in this state.
1. `SHARD_END`: The associated shard is `SHARD_END` and all records have been processed by KCL for the shard.
1. `DELETION`: Since there are no more records to process, KCL will delete the lease from the lease table.
   * Deletion will only be triggered after all parents of a child shard have converged to `SHARD_END`.
   Convergence is required to preserve ordering of records between parent-child relationships.
   * Deletion is configurable yet recommended to minimize I/O of lease table scans.

### Lease Syncing

Lease syncing is a complex responsibility owned by the "leader" host in a KCL application.
By invoking the [ListShards API][list-shards], KCL will identify the shards for the configured stream(s).
This process is scheduled at a configurable interval so KCL can self-identify new shards introduced via stream mutations.

![Abridged sequence diagram of the Shard Sync process.
Listed participants are the Scheduler, LeaseCoordinator, PeriodicShardSyncManager, ShardSyncTask,
Lease Table (DDB), LeaseRefresher, LeaseSynchronizer, HierarchicalShardSyncer, and ShardDetector.
](images/lease-shard-sync.png)

For convenience, links to code:
* `Scheduler`: [implementation][scheduler]
* `LeaseCoordinator`: [interface][lease-coordinator], [implementation][lease-coordinator-impl]
* `PeriodicShardSyncManager`: [implementation][periodic-shard-sync-manager]
* `ShardSyncTask`: [interface][consumer-task], [implementation][consumer-task-impl]
* `LeaseRefresher`: [interface][lease-refresher], [implementation][lease-refresher-impl]
* `LeaseSynchronizer`: [implementation][non-empty-lease-table-synchronizer]
* `HierarchicalShardSyner`: [implementation][hierarchical-shard-syncer]
* `ShardDetector`: [interface][shard-detector], [implementation][shard-detector-impl]

Lease creation is a deterministic process.
Assume a stream has the following shard hierarchy:
<pre>
Shard structure (each level depicts a stream segment):
  0 1 2 3 4   5    - shards till epoch 102
  \ / \ / |   |
   6   7  4   5    - shards from epoch 103 - 205
   \  /   |  / \
     8    4 9  10  - shards from epoch 206+ (still open)
</pre>

Then [NonEmptyLeaseTableSynchronizer][non-empty-lease-table-synchronizer]
would create leases dependent on the configured initial position.
Assuming leases `(4, 5, 7)` already exist, the leases created for an initial position would be:
* `LATEST` creates `(6)` to resolve the gap on-par with epochs 103-205 which is required to eventually reach `LATEST`
* `TRIM_HORIZON` creates `(0, 1)` to resolve the gap starting from the `TRIM_HORIZON`
* `AT_TIMESTAMP(epoch=200)` creates `(0, 1)` to resolve the gap leading into epoch 200

## Lease Balancing

KCL will, at a cadence configured by `leaseDuration` and `epsilonMillis`, attempt to "balance" leases across workers.
Lease balancing is done to protect against stagnation if a worker stops updating the lease table (e.g., host failure).
This operation is naive and only attempts to equally distribute leases across the available hosts;
shards are not guaranteed to be equal in their `put` workloads, and balancing is blind to this I/O skew.

![Sequence diagram of the KCL Lease Taking workflow.
Participants include the LeaseCoordinator, LeaseTaker, LeaseRefresher, and Lease Table (DDB).
LeaseRefresher is leveraged to acquire the leases from the lease table.
LeaseTaker identifies which leases are eligible for taking/stealing.
All taken/stolen leases are passed through LeaseRefresher to update the lease table.
](images/lease-taking.png)

For convenience, links to code:
* `LeaseCoordinator`: [interface][lease-coordinator], [implementation][lease-coordinator-impl]
* `LeaseTaker`: [interface][lease-taker], [implementation][lease-taker-impl]
* `LeaseRefresher`: [interface][lease-refresher], [implementation][lease-refresher-impl]

Leases are stolen if-and-only-if there were zero expired leases and the looking-to-steal-worker desires more leases.
Stolen leases are randomly selected from whichever worker has the most leases.
The number of leases to steal on each loop is configured via `maxLeasesToStealAtOneTime`.

Customers should consider the following trade-offs when configuring the lease-taking cadence:
1. `LeaseRefresher` invokes a DDB `scan` against the lease table which has a cost proportional to the number of leases.
1. Frequent balancing may cause high lease turn-over which incurs DDB `write` costs, and potentially redundant work for stolen leases.
1. High `maxLeasesToStealAtOneTime` may cause churn.
    * For example, worker `B` steals multiple leases from worker `A` creating a numerical imbalance.
    In the next loop, worker `C` may steal leases from worker `B`.
1. Low `maxLeasesToStealAtOneTime` may increase the time to fully (re)assign leases after an impactful event (e.g., deployment, host failure).

# Additional Reading

Informative articles that are recommended (in no particular order):
* https://aws.amazon.com/blogs/big-data/processing-amazon-dynamodb-streams-using-the-amazon-kinesis-client-library/

[consumer-task]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/lifecycle/ConsumerTask.java
[consumer-task-impl]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/ShardSyncTask.java
[dynamodb]: https://aws.amazon.com/dynamodb/
[hierarchical-shard-syncer]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/HierarchicalShardSyncer.java
[kcl-leasetable]: https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html#shared-throughput-kcl-consumers-leasetable
[lease-coordinator]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseCoordinator.java
[lease-coordinator-impl]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/dynamodb/DynamoDBLeaseCoordinator.java
[lease-refresher]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseRefresher.java
[lease-refresher-impl]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/dynamodb/DynamoDBLeaseRefresher.java
[lease-taker]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseTaker.java
[lease-taker-impl]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/dynamodb/DynamoDBLeaseTaker.java
[list-shards]: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html
[non-empty-lease-table-synchronizer]: https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/HierarchicalShardSyncer.java#L857-L910
[periodic-shard-sync-manager]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/coordinator/PeriodicShardSyncManager.java
[scheduler]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/coordinator/Scheduler.java
[shard-detector]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/ShardDetector.java
[shard-detector-impl]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/KinesisShardDetector.java
