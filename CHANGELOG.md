# Changelog

For **1.x** release notes, please see [v1.x/CHANGELOG.md](https://github.com/awslabs/amazon-kinesis-client/blob/v1.x/CHANGELOG.md)

For **2.x** release notes, please see [v2.x/CHANGELOG.md](https://github.com/awslabs/amazon-kinesis-client/blob/v2.x/CHANGELOG.md)

---

### Release 3.5.1 (July 14, 2026)
* [#1779](https://github.com/awslabs/amazon-kinesis-client/pull/1779) Add Metrics for table migration
    * KCL now emits CloudWatch metrics tracking the single-table metadata migration, giving operators visibility into migration progress and state transitions.
* [#1790](https://github.com/awslabs/amazon-kinesis-client/pull/1790) Extend GSI creation timeout to 1 hour
* [#1786](https://github.com/awslabs/amazon-kinesis-client/pull/1786) Increase GSI creation timeout
* [#1788](https://github.com/awslabs/amazon-kinesis-client/pull/1788) Move DDBLeaseRenewer updateLease revert on failure to be inside the synchronized block
* [#1792](https://github.com/awslabs/amazon-kinesis-client/pull/1792) Skip non-lease entity types in DynamoDBMultiStreamLeaseSerializer
* [#1787](https://github.com/awslabs/amazon-kinesis-client/pull/1787) Bump com.fasterxml.jackson.core:jackson-databind from 2.20.0 to 2.21.5
* [#1795](https://github.com/awslabs/amazon-kinesis-client/pull/1795) Bump ch.qos.logback:logback-classic from 1.3.14 to 1.3.16

### Release 3.5.0 (June 22, 2026)
* [#1773](https://github.com/awslabs/amazon-kinesis-client/pull/1773) Single-table migration for KCL metadata
    * KCL 3.5 introduces a mechanism to consolidate all KCL metadata into a single DynamoDB lease table. In earlier KCL 3.x versions, KCL maintained separate DynamoDB tables for lease management, worker metrics (the worker metrics table), and coordinator state (the coordinator state table). Starting with KCL 3.5+, the WorkerMetricStats and CoordinatorState entities can be co-located alongside lease records in the lease table, reducing the number of DynamoDB tables your application provisions and operates. This is optional, but cannot be rolled back once completed.
    * Each record in the lease table is now tagged with an entityType attribute, allowing KCL to distinguish between leases, worker metrics, and coordinator state entries that share the same table.
    * Multi-phase migration. Migrating from earlier versions of KCL v3 (3.0-3.4) to KCL 3.5 (or higher) follows a coordinated, leader-driven process so that a fleet can be upgraded with zero downtime and the ability to roll back.
        * The programming interfaces of KCL 3.5+ remain compatible with KCL 3.x, so you don't need to change your record-processing code. For detailed instructions, see the Migrate consumers to KCL 3.5 page in the Amazon Kinesis Data Streams developer guide. How you adopt the single-table format depends on your current version:
            * Migrating from KCL 2.x → 3.5+: Follow the standard Migrate from KCL 2.x to 3.x guide, targeting 3.5. The single-table format is used by default for new 2.x migrations. Your application uses only the lease table for all metadata, and there is no separate table-migration step to perform or monitor.
            * Migrating from earlier KCL 3.x (3.0–3.4) → 3.5+ (single-table format): Perform the leader-driven migration. Prerequisite: your application must already be running in native KCL 3.x mode: that is, it has completed the 2.x→3.x client-version migration and no longer sets CoordinatorConfig.clientVersionConfig. Do not run the 2.x→3.x client-version migration and the single-table migration at the same time.
                * Phase 1: Deploy KCL 3.5+ with migrateAllEntitiesToLeaseTable = false (the default). Wait for all workers to roll out and for the TableMigration3.5 entry to reach DEPLOYED, and verify there are no regressions.
                * Phase 2: Redeploy with migrateAllEntitiesToLeaseTable = true. The leader drives the state through PENDING to COMPLETE, copying coordinator state into the lease table while workers cut their metric writes over to the lease table. Rollback is supported until COMPLETE.
                * Optional cleanup (after COMPLETE): Once the migration reaches COMPLETE and you have verified stability, you can clean up in a subsequent deployment. Because COMPLETE is a terminal state, the migrateAllEntitiesToLeaseTable setting is ignored, so you can safely remove migrateAllEntitiesToLeaseTable from your configuration. You can also remove the deprecated coordinatorStateTableConfig and workerMetricsTableConfig settings, and manually delete the old worker-metrics and coordinator-state DynamoDB tables. KCL stops using them but does not delete them automatically.
            * Important - IAM permissions: Do not modify or remove your KCL application's IAM permissions to the worker-metrics and coordinator-state tables until the migration has fully reached the COMPLETE state. KCL reads from and writes to those tables throughout the migration (and during any rollback). Removing access earlier can disrupt the migration and prevent normal operation of your application. Remove these permissions only as part of the optional cleanup, after the COMPLETE state is reached.
            * Deprecated configuration properties. KCL no longer creates separate metadata tables for new migrations. We strongly recommend leaving these properties unchanged during the migration. You can safely remove them after the migration reaches the COMPLETE state (see optional cleanup). With this change, the following properties are deprecated in KCL 3.5:
                * CoordinatorConfig.coordinatorStateTableConfig
                * LeaseManagementConfig.workerUtilizationAwareAssignmentConfig.workerMetricsTableConfig
    * Migration state machine and rollback safety. The migration is driven by an internal state machine that progresses through INIT → DEPLOYED → PENDING → COMPLETE, each gated by a configurable bake time. The migration remains safely rollback-able up until it reaches the COMPLETE state, after this state KCL will remain in single-table operation mode.
        * INIT – Starting state set during the Phase 1 upgrade, while workers emit the SupportCode attribute.
        * DEPLOYED – The leader has observed the minimum required support code across the fleet for the bake time, completing Phase 1.
        * PENDING – Phase 2 is underway. All workers are writing WorkerMetricStats to the lease table and the leader has copied all CoordinatorState from the legacy table to the lease table. This state still allows rollback to DEPLOYED if a regression is detected.
        * COMPLETE – Migration is finished. All workers use the lease table exclusively for both reads and writes of all entities. No rollback is permitted once this state is reached. The bake time for this final step is configurable via tableMigrationCompleteBakeTimeSeconds.
    * New configuration properties (in CoordinatorConfig):
        * migrateAllEntitiesToLeaseTable – Set to true to initiate Phase 2 of the single-table migration. Defaults to false.
        * tableMigrationCompleteBakeTimeSeconds – Controls the bake time before the migration transitions to the final COMPLETE state.
    * Deprecated configuration properties. Because KCL no longer supports separate metadata tables once migration completes, the configuration fields used to name and provision the separate worker metrics and coordinator state tables are now deprecated.
* [#1781](https://github.com/awslabs/amazon-kinesis-client/pull/1781) Configurable lease table parallel scan
    * The parallel scan used when listing the lease table is now configurable, giving you control over scan throughput and DynamoDB read consumption for large lease tables.
* [#1776](https://github.com/awslabs/amazon-kinesis-client/pull/1776) Updated migration script for single-table format
    * The KCL migration tooling has been updated to support the new single-table metadata format.
* [#1772](https://github.com/awslabs/amazon-kinesis-client/pull/1772) Skip unnecessary DescribeStreamConsumer calls for active consumers
    * For enhanced fan-out consumers that are already in the ACTIVE state, KCL now skips the redundant DescribeStreamConsumer call and associated sleep, reducing startup latency and API call volume.
* [#1765](https://github.com/awslabs/amazon-kinesis-client/pull/1765) Treat expired pending-handoff leases as available
    * Leases that are pending a graceful handoff but whose handoff has expired are now treated as available to take, preventing leases from becoming stuck when a handoff does not complete.
* [#1744](https://github.com/awslabs/amazon-kinesis-client/pull/1744) Fix FanoutRecordsPublisher restart behavior at SHARD_END
    * Corrected the restartFrom behavior of FanoutRecordsPublisher when a shard reaches SHARD_END, improving reliability of shard lifecycle transitions for enhanced fan-out consumers.
* [#1728](https://github.com/awslabs/amazon-kinesis-client/pull/1728) CloudWatch OTEL endpoint support
    * Added support for publishing KCL CloudWatch metrics through an OpenTelemetry (OTEL) endpoint.

### Release 3.4.3 (April 30, 2026)
* [#1734](https://github.com/awslabs/amazon-kinesis-client/pull/1734) Fix metricsScope not being closed properly in Scheduler.run
* [#1731](https://github.com/awslabs/amazon-kinesis-client/pull/1731) Perform initial shard sync operation after leader election
* [#1730](https://github.com/awslabs/amazon-kinesis-client/pull/1730) Add StreamType to StreamIdentifier and skip hash range ERROR logs for DynamoDB Streams
* [#1729](https://github.com/awslabs/amazon-kinesis-client/pull/1729) Fix heartbeat not being stopped for graceful handoff lease in shutting down state
* [#1670](https://github.com/awslabs/amazon-kinesis-client/pull/1670) Add lease count-based assignment strategy as an alternative to worker utilization-aware assignment

### Release (3.4.2 - April 1, 2026)
* [#1718](https://github.com/awslabs/amazon-kinesis-client/pull/1718) Fix stuck leases when worker restarts with pending checkpoint leases
* [#1717](https://github.com/awslabs/amazon-kinesis-client/pull/1717) Refactor KinesisDataFetcher to use GetRecordsResponseAdapter
* [#1715](https://github.com/awslabs/amazon-kinesis-client/pull/1715) Revert to use Set for metricsEnabledDimension
* [#1713](https://github.com/awslabs/amazon-kinesis-client/pull/1713) Allow GetRecordsResponseAdapter to provide ProcessRecordsInput Mapping

### Release 3.4.1 (March 2, 2026)
* [#1708](https://github.com/awslabs/amazon-kinesis-client/pull/1708) Fix bug that occurs when maxPendingProcessRecordsInput is set to 0
* [#1702](https://github.com/awslabs/amazon-kinesis-client/pull/1702) Shutdown migrationComponentsInitializer as part of graceful shutdown
* [#1700](https://github.com/awslabs/amazon-kinesis-client/pull/1700) Fix shutdown behavior for DDBLeaseCoordinator

### Release 3.4.0 (February 12, 2026)
* [#1664](https://github.com/awslabs/amazon-kinesis-client/pull/1664) Add streamId support in KCL (reserved for future use)
* [#1698](https://github.com/awslabs/amazon-kinesis-client/pull/1698) Introduce jitter to WorkerMetricStatsManager startup

### Release 3.3.0 (January 23, 2026)
* [#1653](https://github.com/awslabs/amazon-kinesis-client/pull/1653) Add missing PR in 3.2.1 changelog
* [#1656](https://github.com/awslabs/amazon-kinesis-client/pull/1656) Log error if there is an issue in KPL record deaggregation 
* [#1657](https://github.com/awslabs/amazon-kinesis-client/pull/1657) Fix integ tests and add Github workflow to run integ tests 
* [#1669](https://github.com/awslabs/amazon-kinesis-client/pull/1669) Catch throwable in LAM performAssignment to prevent zombie leader 
* [#1662](https://github.com/awslabs/amazon-kinesis-client/pull/1662) Refactor E2E application integration test files to a singular package
* [#1673](https://github.com/awslabs/amazon-kinesis-client/pull/1673) Avoid race condition in leader lock acquisition during shutdown
* [#1674](https://github.com/awslabs/amazon-kinesis-client/pull/1674) Add maxPendingProcessRecordsInput config to PollingConfig
* [#1645](https://github.com/awslabs/amazon-kinesis-client/pull/1645) Bump awssdk.version from 2.37.1 to 2.37.3
* [#1689](https://github.com/awslabs/amazon-kinesis-client/pull/1689) Bump gsr.version from 1.1.24 to 1.1.27

### Release 3.2.1 (November 14, 2025)
* [#1650](https://github.com/awslabs/amazon-kinesis-client/pull/1650) Remove explicit netty dependency to fix mismatched versions
* [#1648](https://github.com/awslabs/amazon-kinesis-client/pull/1648) Capture subscribe exceptions in onError instead of throwing in RxJava

### Release 3.2.0 (November 5, 2025)
* [#1482](https://github.com/awslabs/amazon-kinesis-client/pull/1482) Add config for leader lifetime and heartbeat
* [#1533](https://github.com/awslabs/amazon-kinesis-client/pull/1533) Bump awssdk.version from 2.33.0 to 2.37.1
* [#1634](https://github.com/awslabs/amazon-kinesis-client/pull/1634) Remove old commons-collections version
* [#1635](https://github.com/awslabs/amazon-kinesis-client/pull/1635) Remove var keyword from unit test
* [#1641](https://github.com/awslabs/amazon-kinesis-client/pull/1641) Bump netty-handler from 4.1.118.Final to 4.2.7.Final

### Release 3.1.3 (September 24, 2025)
* [#1619](https://github.com/awslabs/amazon-kinesis-client/pull/1619) Bump awssdk from 2.31.77 to 2.33.0 and bump fasterxml.jackson from 2.12.7.1 to 2.20.0

### Release 3.1.2 (August 20, 2025)
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
