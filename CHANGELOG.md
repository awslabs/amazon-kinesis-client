# Changelog

For **1.x** release notes, please see [v1.x/CHANGELOG.md](https://github.com/awslabs/amazon-kinesis-client/blob/v1.x/CHANGELOG.md)

For **2.x** release notes, please see [v2.x/CHANGELOG.md](https://github.com/awslabs/amazon-kinesis-client/blob/v2.x/CHANGELOG.md)

---
### Release 3.1.3 (September 24, 2025)
* [#1619](https://github.com/awslabs/amazon-kinesis-client/pull/1619) Bump awssdk from 2.31.77 to 2.33.0 and bump fasterxml.jackson from 2.12.7.1 to 2.20.0

### Release 3.1.2 (Aug 20, 2025)
* [#1501](https://github.com/awslabs/amazon-kinesis-client/pull/1501) Allow migration to KCL 3.x when there are fewer leases than workers
* [#1496](https://github.com/awslabs/amazon-kinesis-client/pull/1496) Decrease DDB lease renewal verbosity
* [#1523](https://github.com/awslabs/amazon-kinesis-client/pull/1523) Replace ProcessRecordsInput.builder() method with ProcessRecordsInput.toBuilder() to avoid copying over fields
* [#1519](https://github.com/awslabs/amazon-kinesis-client/pull/1519) Fix flaky LeaseAssignmentManager retry behavior test
* [#1504](https://github.com/awslabs/amazon-kinesis-client/pull/1504) Bump awssdk.version from 2.31.62 to 2.31.77
* [#1538](https://github.com/awslabs/amazon-kinesis-client/pull/1538) Bump com.amazonaws:dynamodb-lock-client from 1.3.0 to 1.4.0
* [#1513](https://github.com/awslabs/amazon-kinesis-client/pull/1513) Bump org.apache.commons:commons-lang3 from 3.14.0 to 3.18.0 in /amazon-kinesis-client


### Release (3.1.1 - July 8, 2025)
* [#1495](https://github.com/awslabs/amazon-kinesis-client/pull/1495) Add millisBehindLatestThresholdForReducedTps property to PollingConfig
* [#1498](https://github.com/awslabs/amazon-kinesis-client/pull/1498) Add multilang support for leaseAssignmentIntervalMillis config
* [#1499](https://github.com/awslabs/amazon-kinesis-client/pull/1499) Add Lease Table ARN Hash as Consumer ID in UserAgent for Kinesis requests
* [#1493](https://github.com/awslabs/amazon-kinesis-client/pull/1493) Bump awssdk.version from 2.25.64 to 2.31.62
* [#1502](https://github.com/awslabs/amazon-kinesis-client/pull/1502) Bump io.reactivex.rxjava3:rxjava from 3.1.8 to 3.1.11
* [#1457](https://github.com/awslabs/amazon-kinesis-client/pull/1457) Bump org.projectlombok:lombok from 1.18.24 to 1.18.38
* [#1505](https://github.com/awslabs/amazon-kinesis-client/pull/1505) Bump schema-registry-common and schema-registry-serde from 1.1.19 to 1.1.24

### Release (3.1.0 - June 09, 2025)
* [#1481](https://github.com/awslabs/amazon-kinesis-client/pull/1481) Convert awssdk object to lease early to release memory
* [#1480](https://github.com/awslabs/amazon-kinesis-client/pull/1480) Added the ability to control sleep time in recordsfetcher
* [#1479](https://github.com/awslabs/amazon-kinesis-client/pull/1479) Overriding the DataFetcher to return a custom GetRecordsResponseAdapter so that customers can have custom logic to send data to KinesisClientRecord
* [#1478](https://github.com/awslabs/amazon-kinesis-client/pull/1478) Bump commons-beanutils:commons-beanutils from 1.9.4 to 1.11.0 in /amazon-kinesis-client-multilang

### Release 3.0.3 (May 7, 2025)
* [#1464](https://github.com/awslabs/amazon-kinesis-client/pull/1464) Add config for LeaseAssignmentManager frequency and improve assignment time of newly created leases
* [#1463](https://github.com/awslabs/amazon-kinesis-client/pull/1463) Extend ShardConsumer constructor to have ConsumerTaskFactory as a parameter to support [DynamoDB Streams Kinesis Adapter](https://github.com/awslabs/dynamodb-streams-kinesis-adapter) compatibility
* [#1472](https://github.com/awslabs/amazon-kinesis-client/pull/1472) Remove unused synchronized keyword

### Release 3.0.2 (March 12, 2025)
* [#1443](https://github.com/awslabs/amazon-kinesis-client/pull/1443) Reduce DynamoDB IOPS for smaller stream and worker count applications
* The below two PRs are intended to support [DynamoDB Streams Kinesis Adapter](https://github.com/awslabs/dynamodb-streams-kinesis-adapter) compatibility
  * [#1441](https://github.com/awslabs/amazon-kinesis-client/pull/1441) Make consumerTaskFactory overridable by customers
  * [#1440](https://github.com/awslabs/amazon-kinesis-client/pull/1440) Make ShutdownTask, ProcessTask, InitializeTask, BlockOnParentTask, and ShutdownNotificationTask overridable by customers
* [#1437](https://github.com/awslabs/amazon-kinesis-client/pull/1437) Suppress LeaseAssignmentManager excessive WARN logs when there is no issue
* [#1439](https://github.com/awslabs/amazon-kinesis-client/pull/1439) Upgrade io.netty:netty-handler from 4.1.108.Final to 4.1.118.Final
* [#1400](https://github.com/awslabs/amazon-kinesis-client/pull/1400) Upgrade com.fasterxml.jackson.core:jackson-databind from 2.10.1 to 2.12.7.1

### Release 3.0.1 (November 14, 2024)
* [#1401](https://github.com/awslabs/amazon-kinesis-client/pull/1401) Fixed the lease graceful handoff behavior in the multi-stream processing mode
* [#1398](https://github.com/awslabs/amazon-kinesis-client/pull/1398) Addressed several KCL 3.0 related issues raised via GitHub
    * Fixed transitive dependencies and added a Maven plugin to catch potential transitive dependency issues at build time
    * Removed the redundant shutdown of the leaseCoordinatorThreadPool
    * Fixed typo THROUGHOUT_PUT_KBPS
    * Fixed issues in scheduler shutdown sequence

* Note: If you are using [multi-stream processing with KCL](https://docs.aws.amazon.com/streams/latest/dev/kcl-multi-stream.html), you need to use the release 3.0.1 or later.

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
