# Changelog

For **1.x** release notes, please see [v1.x/CHANGELOG.md](https://github.com/awslabs/amazon-kinesis-client/blob/v1.x/CHANGELOG.md)

For **2.x** release notes, please see [v2.x/CHANGELOG.md](https://github.com/awslabs/amazon-kinesis-client/blob/v2.x/CHANGELOG.md)

---
### Release 3.0.0 (November 06, 2024)
* New lease assignment / load balancing algorithm
    * KCL 3.x introduces a new lease assignment and load balancing algorithm. It assigns leases among workers based on worker utilization metrics and throughput on each lease, replacing the previous lease count-based lease assignment algorithm.
    * When KCL detects higher variance in CPU utilization among workers, it proactively reassigns leases from over-utilized workers to under-utilized workers for even load balancing. This ensures even CPU utilization across workers and removes the need to over-provision the stream processing compute hosts.
* Optimized DynamoDB RCU usage
    * KCL 3.x optimizes DynamoDB read capacity unit (RCU) usage on the lease table by implementing a global secondary index with leaseOwner as the partition key. This index mirrors the leaseKey attribute from the base lease table, allowing workers to efficiently discover their assigned leases by querying the index instead of scanning the entire table.
    * This approach significantly reduces read operations compared to earlier KCL versions, where workers performed full table scans, resulting in higher RCU consumption.
* Graceful lease handoff
    * KCL 3.x introduces a feature called "graceful lease handoff" to minimize data reprocessing during lease reassignments. Graceful lease handoff allows the current worker to complete checkpointing of processed records before transferring the lease to another worker. For graceful lease handoff, you should implement checkpointing logic within the existing `shutdownRequested()` method.
    * This feature is enabled by default in KCL 3.x, but you can turn off this feature by adjusting the configuration property `isGracefulLeaseHandoffEnabled`.
    * While this approach significantly reduces the probability of data reprocessing during lease transfers, it doesn't completely eliminate the possibility. To maintain data integrity and consistency, it's crucial to design your downstream consumer applications to be idempotent. This ensures that the application can handle potential duplicate record processing without adverse effects.
* New DynamoDB metadata management artifacts
    * KCL 3.x introduces two new DynamoDB tables for improved lease management:
        * Worker metrics table: Records CPU utilization metrics from each worker. KCL uses these metrics for optimal lease assignments, balancing resource utilization across workers. If CPU utilization metric is not available, KCL assigns leases to balance the total sum of shard throughput per worker instead.
        * Coordinator state table: Stores internal state information for workers. Used to coordinate in-place migration from KCL 2.x to KCL 3.x and leader election among workers.
    * Follow this [documentation](https://docs.aws.amazon.com/streams/latest/dev/kcl-migration-from-2-3.html#kcl-migration-from-2-3-IAM-permissions) to add required IAM permissions for your KCL application.
* Other improvements and changes
    * Dependency on the AWS SDK for Java 1.x has been fully removed.
        * The Glue Schema Registry integration functionality no longer depends on AWS SDK for Java 1.x. Previously, it required this as a transient dependency.
        * Multilangdaemon has been upgraded to use AWS SDK for Java 2.x. It no longer depends on AWS SDK for Java 1.x.
    * `idleTimeBetweenReadsInMillis` (PollingConfig) now has a minimum default value of 200.
        * This polling configuration property determines the [publishers](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/retrieval/polling/PrefetchRecordsPublisher.java) wait time between GetRecords calls in both success and failure cases. Previously, setting this value below 200 caused unnecessary throttling. This is because Amazon Kinesis Data Streams supports up to five read transactions per second per shard for shared-throughput consumers.
    * Shard lifecycle management is improved to deal with edge cases around shard splits and merges to ensure records continue being processed as expected.
* Migration
    * The programming interfaces of KCL 3.x remain identical with KCL 2.x for an easier migration, with the exception of those applications that do not use the recommended approach of using the Config Builder. These applications will have to refer to [the troubleshooting guide](https://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html#compiliation-error-leasemanagementconfig). For detailed migration instructions, please refer to the [Migrate consumers from KCL 2.x to KCL 3.x](https://docs.aws.amazon.com/streams/latest/dev/kcl-migration-from-2-3.html) page in the Amazon Kinesis Data Streams developer guide.
* Configuration properties
    * New configuration properties introduced in KCL 3.x are listed in this [doc](https://github.com/awslabs/amazon-kinesis-client/blob/master/docs/kcl-configurations.md#new-configurations-in-kcl-3x).
    * Deprecated configuration properties in KCL 3.x are listed in this [doc](https://github.com/awslabs/amazon-kinesis-client/blob/master/docs/kcl-configurations.md#discontinued-configuration-properties-in-kcl-3x). You need to keep the deprecated configuration properties during the migration from any previous KCL version to KCL 3.x.
* Metrics
    * New CloudWatch metrics introduced in KCL 3.x are explained in the [Monitor the Kinesis Client Library with Amazon CloudWatch](https://docs.aws.amazon.com/streams/latest/dev/monitoring-with-kcl.html) in the Amazon Kinesis Data Streams developer guide. The following operations are newly added in KCL 3.x:
        * `LeaseAssignmentManager`
        * `WorkerMetricStatsReporter`
        * `LeaseDiscovery`
