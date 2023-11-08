# Lease Lifecycle

A lease is data that defines the binding between a worker and a shard.
Distributed KCL consumer applications use leases to partition data record processing across a fleet of workers.
At any given time, each shard of data records is bound to a particular worker by a lease identified by the `leaseKey` variable.

This document describes the lease lifecycle.

**Note:** shard ids are simplified from `shardId-000000000042` to read as `shardId[_-]42` for simplicity.

## Leases

In KCL, a lease provides a temporal assignment between one shard and an assigned worker.
Leases are persistent for the duration of shard processing (detailed later).
However, the worker that is processing a lease may change since [leases may be "stolen"](#lease-balancing) by other workers in the same KCL application.

## Lease Table

To persist metadata about lease state (e.g., [last read checkpoint, current assigned worker][kcl-concepts]), KCL creates a lease table in [DynamoDB][dynamodb].
Each KCL application will have its own distinct lease table that includes the application name.
More information, including schema, is provided at [KCL LeaseTable][kcl-leasetable].

## Lease Assignment

Leases are unique to the shard and are not recycled for stream operations (i.e., split, merge).
A shard created by stream operations will generate a new lease.

![Activity diagram of KCL shard-to-lease assignments.
shard-0 (lease-0) is unmodified.
shard-1 (lease-1) is split into shard-4 (lease-4) and shard-5 (lease-5).
shard-2 (lease-2) and shard-3 (lease-3) are merged into shard-6 (lease-6).
](images/leases-and-operations.png)

It should be noted that the number of tracked leases may exceed the number of shards.
Per the diagram (above), this can occur when there are stream operations propagating through KCL.
For example, a 10-shard stream that is split on every shard may temporarily have up-to 30 leases: 10 original + 20 children.

**Note:** Leases are uniquely identified by their `leaseKey` which looks vastly different than `lease_X`.
For details on the `leaseKey` format, please see [KCL LeaseTable][kcl-leasetable].

## Lease Lifecycle

Leases follow a relatively simple, progressive state machine:
`DISCOVERY -> CREATION -> PROCESSING -> SHARD_END -> DELETION`

Excluding `SHARD_END`, these phases are illustrative of KCL logic and are not explicitly codified.

1. `DISCOVERY`: KCL [shard-syncing](#shard-syncing) identifies new shards.
Discovered shards may result from:
   * First time starting KCL with an empty lease table.
   * Stream operations (i.e., split, merge) that create child shards.
   * In multi-stream mode, dynamic discovery of a new stream.
1. `CREATION`: Leases are created 1:1 for each discovered shard.
   * Leases are only created if they are eligible for processing.
   For example, child shards will not have leases created until its parent(s) have reached `SHARD_END`.
   * Leases are initialized at the configured initial position.
      * A notable exception is that child leases are initialized at `TRIM_HORIZON` to avoid processing gaps from their parent lease(s).
1. `PROCESSING`: Leases are processed, and continually updated with new checkpoints.
   * In general, leases spend the majority of their life in this state.
1. `SHARD_END`: The associated shard is `SHARD_END` and all records have been processed by KCL for the shard.
1. `DELETION`: Since there are no more records to process, KCL will delete the lease from the lease table.
   * Lease deletion will not occur until after its child lease(s) enter `PROCESSING`.
      * This tombstone helps KCL ensure durability and convergence for all discovered leases.
      * For more information, see [LeaseCleanupManager#cleanupLeaseForCompletedShard(...)][lease-cleanup-manager-cleanupleaseforcompletedshard][^fixed-commit-footnote]
   * [Deletion is configurable][lease-cleanup-config] yet recommended to minimize I/O of lease table scans.

### Shard Syncing

Shard syncing is a complex responsibility owned by the leader host in a KCL application.
By invoking the [ListShards API][list-shards], KCL will identify the shards for the configured stream(s).
This process is scheduled at a [configurable interval][lease-auditor-config] to determine whether a shard sync should be executed to identify new shards.
A shard sync is not guaranteed to identify new shards (e.g., KCL has already discovered all existing shards).

The following diagram is an abridged sequence diagram of key classes that initialize the shard sync workflow:
![Abridged sequence diagram of the Shard Sync initialization process.
Listed participants are the Scheduler, LeaseCoordinator, PeriodicShardSyncManager, and Lease Table (DDB).
Scheduler initializes the LeaseCoordinator which, in turn, creates the lease table if it does not exist.
Finally, Scheduler starts the PeriodicShardSyncManager which schedules itself to execute every leasesRecoveryAuditorExecutionFrequencyMillis.
](images/lease-shard-sync-initialization.png)

The following diagram outlines the key classes involved in the shard sync workflow:
![Abridged sequence diagram of the Shard Sync main processing loop.
Listed participants are the PeriodicShardSyncManager, ShardSyncTask, ShardDetector,
HierarchicalShardSyncer, LeaseRefresher, LeaseSynchronizer, and Lease Table (DDB).
On each iteration, PeriodicShardSyncManger determines whether it's the leader and a shard-sync is required before proceeding.
PeriodicShardSyncManager calls ShardSyncTask which calls HierarchicalShardSyncer which acquires the shard lists from ShardDetector.
HierarchicalShardSyncer then invokes LeaseRefresher to scan the DDB lease table, and uses those returned leases to identify shards which do not have leases.
Finally, HierarchicalShardSyncer uses LeaseRefresher to create any new leases in DDB.
](images/lease-shard-sync-loop.png)

For more information, here are the links to KCL code:
* `Scheduler`: [implementation][scheduler]
* `LeaseCoordinator`: [interface][lease-coordinator], [implementation][lease-coordinator-impl]
* `PeriodicShardSyncManager`: [implementation][periodic-shard-sync-manager]
* `ShardSyncTask`: [interface][consumer-task], [implementation][consumer-task-impl]
* `ShardDetector`: [interface][shard-detector], [implementation][shard-detector-impl]
* `HierarchicalShardSyncer`: [implementation][hierarchical-shard-syncer]
* `LeaseRefresher`: [interface][lease-refresher], [implementation][lease-refresher-impl]
* `LeaseSynchronizer`: [implementation][non-empty-lease-table-synchronizer]

Lease creation is a deterministic process.
This is illustrative of how KCL operates.
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

#### Avoiding a Shard-Sync

To reduce Kinesis Data Streams API calls, KCL will attempt to avoid unnecessary shard syncs.
For example, if the discovered shards cover the entire partition range then a shard-sync is unlikely to yield a material difference.
For more information, see
[PeriodicShardSyncManager#checkForShardSync(...)][periodic-shard-sync-manager-checkforshardsync])[^fixed-commit-footnote].

## Lease Balancing

KCL balances leases across workers at an interval configured by `leaseDuration` and `epsilonMillis`.
Lease balancing is done to protect against interruptions in processing should a worker stop updating the lease table (e.g., host failure).
This operation only accounts for lease assignments and does not factor in I/O load.
For example, leases that are equally-distributed across KCL are not guaranteed to have equal I/O distribution.

![Sequence diagram of the KCL Lease Taking workflow.
Participants include the LeaseCoordinator, LeaseTaker, LeaseRefresher, and Lease Table (DDB).
LeaseRefresher is leveraged to acquire the leases from the lease table.
LeaseTaker identifies which leases are eligible for taking/stealing.
All taken/stolen leases are passed through LeaseRefresher to update the lease table.
](images/lease-taking.png)

For more information, here are the links to KCL code:
* `LeaseCoordinator`: [interface][lease-coordinator], [implementation][lease-coordinator-impl]
* `LeaseTaker`: [interface][lease-taker], [implementation][lease-taker-impl]
* `LeaseRefresher`: [interface][lease-refresher], [implementation][lease-refresher-impl]

Leases are stolen if-and-only-if there were zero expired leases and the looking-to-steal-worker desires more leases.
Stolen leases are randomly selected from whichever worker has the most leases.
The maximum number of leases to steal on each loop is configured via [maxLeasesToStealAtOneTime][max-leases-to-steal-config].

Customers should consider the following trade-offs when configuring the lease-taking cadence:
1. `LeaseRefresher` invokes a DDB `scan` against the lease table which has a cost proportional to the number of leases.
1. Frequent balancing may cause high lease turn-over which incurs DDB `write` costs, and potentially redundant work for stolen leases.
1. Low `maxLeasesToStealAtOneTime` may increase the time to fully (re)assign leases after an impactful event (e.g., deployment, host failure).

# Additional Reading

Recommended reading:
* [Processing Amazon DynamoDB Streams Using the Amazon Kinesis Client Library](https://aws.amazon.com/blogs/big-data/processing-amazon-dynamodb-streams-using-the-amazon-kinesis-client-library/)

[^fixed-commit-footnote]: This link is a point-in-time reference to a specific commit to guarantee static line numbers.
    This code reference is not guaranteed to remain consistent with the `master` branch.

[consumer-task]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/lifecycle/ConsumerTask.java
[consumer-task-impl]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/ShardSyncTask.java
[dynamodb]: https://aws.amazon.com/dynamodb/
[hierarchical-shard-syncer]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/HierarchicalShardSyncer.java
[kcl-concepts]: https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html#shared-throughput-kcl-consumers-concepts
[kcl-leasetable]: https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html#shared-throughput-kcl-consumers-leasetable
[lease-auditor-config]: https://github.com/awslabs/amazon-kinesis-client/blob/3d6800874cdc5e4c18df6ea0197f607f6298cab7/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseManagementConfig.java#L204-L209
[lease-cleanup-config]: https://github.com/awslabs/amazon-kinesis-client/blob/3d6800874cdc5e4c18df6ea0197f607f6298cab7/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseManagementConfig.java#L112-L128
[lease-cleanup-manager-cleanupleaseforcompletedshard]: https://github.com/awslabs/amazon-kinesis-client/blob/3d6800874cdc5e4c18df6ea0197f607f6298cab7/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseCleanupManager.java#L263-L294
[lease-coordinator]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseCoordinator.java
[lease-coordinator-impl]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/dynamodb/DynamoDBLeaseCoordinator.java
[lease-refresher]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseRefresher.java
[lease-refresher-impl]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/dynamodb/DynamoDBLeaseRefresher.java
[lease-taker]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseTaker.java
[lease-taker-impl]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/dynamodb/DynamoDBLeaseTaker.java
[list-shards]: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html
[max-leases-to-steal-config]: https://github.com/awslabs/amazon-kinesis-client/blob/3d6800874cdc5e4c18df6ea0197f607f6298cab7/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseManagementConfig.java#L142-L149
[non-empty-lease-table-synchronizer]: https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/HierarchicalShardSyncer.java#L857-L910
[periodic-shard-sync-manager]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/coordinator/PeriodicShardSyncManager.java
[periodic-shard-sync-manager-checkforshardsync]: https://github.com/awslabs/amazon-kinesis-client/blob/3d6800874cdc5e4c18df6ea0197f607f6298cab7/amazon-kinesis-client/src/main/java/software/amazon/kinesis/coordinator/PeriodicShardSyncManager.java#L267-L300
[scheduler]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/coordinator/Scheduler.java
[shard-detector]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/ShardDetector.java
[shard-detector-impl]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/KinesisShardDetector.java
