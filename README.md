# Amazon Kinesis Client Library for Java
[![Build Status](https://travis-ci.org/awslabs/amazon-kinesis-client.svg?branch=master)](https://travis-ci.org/awslabs/amazon-kinesis-client)

The **Amazon Kinesis Client Library for Java** (Amazon KCL) enables Java developers to easily consume and process data from [Amazon Kinesis][kinesis].

* [Kinesis Product Page][kinesis]
* [Forum][kinesis-forum]
* [Issues][kinesis-client-library-issues]

### Recommended Upgrade for All Users of the 1.x Amazon Kinesis Client
:warning: We recommend customers to migrate to 1.14.1 or newer to avoid [known bugs](https://github.com/awslabs/amazon-kinesis-client/issues/778) in 1.14.0 version

### Recommended Upgrade for All Users of the 2.x Amazon Kinesis Client
**:warning: It's highly recommended for users of version 2.0 of the Amazon Kinesis Client to upgrade to version 2.0.3 or later. A [bug has been](https://github.com/awslabs/amazon-kinesis-client/issues/391) identified in versions prior to 2.0.3 that could cause records to be delivered to the wrong record processor.**  

**:information_source: Amazon Kinesis Client versions 1.x are not impacted.**  

Please open an issue if you have any questions.

## Features

* Provides an easy-to-use programming model for processing data using Amazon Kinesis
* Helps with scale-out and fault-tolerant processing

## Getting Started

1. **Sign up for AWS** &mdash; Before you begin, you need an AWS account. For more information about creating an AWS account and retrieving your AWS credentials, see [AWS Account and Credentials][docs-signup] in the AWS SDK for Java Developer Guide.
1. **Sign up for Amazon Kinesis** &mdash; Go to the Amazon Kinesis console to sign up for the service and create an Amazon Kinesis stream. For more information, see [Create an Amazon Kinesis Stream][kinesis-guide-create] in the Amazon Kinesis Developer Guide.
1. **Minimum requirements** &mdash; To use the Amazon Kinesis Client Library, you'll need **Java 1.8+**. For more information about Amazon Kinesis Client Library requirements, see [Before You Begin][kinesis-guide-begin] in the Amazon Kinesis Developer Guide.
1. **Using the Amazon Kinesis Client Library** &mdash; The best way to get familiar with the Amazon Kinesis Client Library is to read [Developing Record Consumer Applications][kinesis-guide-applications] in the Amazon Kinesis Developer Guide.

## Building from Source

After you've downloaded the code from GitHub, you can build it using Maven. To disable GPG signing in the build, use
 this command: `mvn clean install -Dgpg.skip=true`. Note: This command runs Integration tests, which in turn creates AWS
  resources (which requires manual cleanup). Integration tests require valid AWS credentials need to be discovered at
   runtime. To skip running integration tests, add ` -DskipITs` option to the build command.  

## Integration with the Kinesis Producer Library
For producer-side developers using the **[Kinesis Producer Library (KPL)][kinesis-guide-kpl]**, the KCL integrates without additional effort. When the KCL retrieves an aggregated Amazon Kinesis record consisting of multiple KPL user records, it will automatically invoke the KPL to extract the individual user records before returning them to the user.

## Amazon KCL support for other languages
To make it easier for developers to write record processors in other languages, we have implemented a Java based daemon, called MultiLangDaemon that does all the heavy lifting. Our approach has the daemon spawn a sub-process, which in turn runs the record processor, which can be written in any language. The MultiLangDaemon process and the record processor sub-process communicate with each other over [STDIN and STDOUT using a defined protocol][multi-lang-protocol]. There will be a one to one correspondence amongst record processors, child processes, and shards. For Python developers specifically, we have abstracted these implementation details away and [expose an interface][kclpy] that enables you to focus on writing record processing logic in Python. This approach enables KCL to be language agnostic, while providing identical features and similar parallel processing model across all languages.

## Using the KCL
The recommended way to use the KCL for Java is to consume it from Maven.

### Version 2.x
  ``` xml
  <dependency>
      <groupId>software.amazon.kinesis</groupId>
      <artifactId>amazon-kinesis-client</artifactId>
      <version>2.3.5</version>
  </dependency>
  ```

### Version 1.x
[Version 1.x tracking branch](https://github.com/awslabs/amazon-kinesis-client/tree/v1.x)
  ``` xml
  <dependency>
      <groupId>com.amazonaws</groupId>
      <artifactId>amazon-kinesis-client</artifactId>
      <version>1.14.1</version>
  </dependency>
  ```

## Release Notes

### Latest Release 2.3.5 (June 14, 2021)
* [#824](https://github.com/awslabs/amazon-kinesis-client/pull/824)
	* Upgraded logback-classic from 1.1.7 to 1.2.3
	* Upgraded AWS Java SDK version to 1.12.3
	* Upgraded AWS SDK version to 2.16.81
* [#815](https://github.com/awslabs/amazon-kinesis-client/pull/815) Converted Future to CompletableFuture which helps in proper conversion to Scala using Scala Future Converters.
* [#810](https://github.com/awslabs/amazon-kinesis-client/pull/810) Bump commons-io from 2.6 to 2.7
* [#804](https://github.com/awslabs/amazon-kinesis-client/pull/804) Allowing user to specify an initial timestamp in which daemon will process records.
* [#802](https://github.com/awslabs/amazon-kinesis-client/pull/802) Upgraded guava from 26.0-jre to 29.0-jre
* [#801](https://github.com/awslabs/amazon-kinesis-client/pull/801) Fixing a bug that causes to block indefinitely when trying to unlock a lock that isn't locked.
* [#762](https://github.com/awslabs/amazon-kinesis-client/pull/762) Added support for web identity token in multilang

### Release 2.3.4 (February 19, 2021)
[Milestone#56](https://github.com/awslabs/amazon-kinesis-client/milestone/56)
* [#788](https://github.com/awslabs/amazon-kinesis-client/pull/788) Fixing a bug that caused paginated `ListShards` calls with the `ShardFilter` parameter to fail when the lease table was being initialized.

### Release 2.3.3 (December 23, 2020)
[Milestone#55](https://github.com/awslabs/amazon-kinesis-client/milestone/55)
* Fixing bug in PrefetchRecordsPublisher which was causing retry storms if initial request fails.
* Fixing bug where idleTimeBetweenReadsInMillis property was ignored in PollingConfig.

### Release 2.3.2 (November 19, 2020)
[Milestone#54](https://github.com/awslabs/amazon-kinesis-client/milestone/54)
* Adding support for Glue Schema Registry. Deserialize and read schemas associated with the records.
* Updating AWS SDK version to 2.15.31

### Release 2.3.1 (October 20, 2020)
[Milestone#53](https://github.com/awslabs/amazon-kinesis-client/milestone/53)
* Introducing support for processing multiple kinesis data streams with the same KCL 2.x for java consumer application
  * To build a consumer application that can process multiple streams at the same time, you must implement a new
  interface called MultistreamTracker (https://github.com/awslabs/amazon-kinesis-client/blob/0c5042dadf794fe988438436252a5a8fe70b6b0b/amazon-kinesis-client/src/main/java/software/amazon/kinesis/processor/MultiStreamTracker.java)

  * MultistreamTracker will also publish various metrics around the current active streams being processed, the number
   of streams which are deleted at this time period or are pending deletion.

### Release 2.3.0 (August 17, 2020)
* [Milestone#52](https://github.com/awslabs/amazon-kinesis-client/milestones/52)

* Behavior of shard synchronization is moving from each worker independently learning about all existing shards to workers only discovering the children of shards that each worker owns. This optimizes memory usage, lease table IOPS usage, and number of calls made to kinesis for streams with high shard counts and/or frequent resharding.
* When bootstrapping an empty lease table, KCL utilizes the `ListShard` API's filtering option (the ShardFilter optional request parameter) to retrieve and create leases only for a snapshot of shards open at the time specified by the `ShardFilter` parameter. The `ShardFilter` parameter enables you to filter out the response of the `ListShards` API, using the `Type` parameter. KCL uses the `Type` filter parameter and the following of its valid values to identify and return a snapshot of open shards that might require new leases.
	* Currently, the following shard filters are supported:
		* `AT_TRIM_HORIZON` - the response includes all the shards that were open at `TRIM_HORIZON`.
		* `AT_LATEST` - the response includes only the currently open shards of the data stream.
	    * `AT_TIMESTAMP` - the response includes all shards whose start timestamp is less than or equal to the given timestamp and end timestamp is greater than or equal to the given timestamp or still open.
	* `ShardFilter` is used when creating leases for an empty lease table to initialize leases for a snapshot of shards specified at `RetrievalConfig#initialPositionInStreamExtended`.
	* For more information about ShardFilter, see the [official AWS documentation on ShardFilter](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ShardFilter.html).

* Introducing support for the `ChildShards` response of the `GetRecords` and the `SubscribeToShard` APIs to perform lease/shard synchronization that happens at `SHARD_END` for closed shards, allowing a KCL worker to only create leases for the child shards of the shard it finished processing.
	* For shared throughout consumer applications, this uses the `ChildShards` response of the `GetRecords` API. For dedicated throughput (enhanced fan-out) consumer applications, this uses the `ChildShards` response of the `SubscribeToShard` API.
	* For more information, see the official AWS Documentation on [GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html), [SubscribeToShard](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SubscribeToShard.html), and [ChildShard](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ChildShard.html).

* KCL now also performs additional periodic shard/lease scans in order to identify any potential holes in the lease table to ensure the complete hash range of the stream is being processed and create leases for them if required. `PeriodicShardSyncManager` is the new component that is responsible for running periodic lease/shard scans.
	* New configuration options are available to configure `PeriodicShardSyncManager` in `LeaseManagementConfig`

	| Name                                                  | Default            | Description                                                                                                                                                                                                                                                   |
	| ----------------------------------------------------- | -----------------  | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
	| leasesRecoveryAuditorExecutionFrequencyMillis         | 120000 (2 minutes) | Frequency (in millis) of the auditor job to scan for partial leases in the lease table. If the auditor detects any hole in the leases for a stream, then it would trigger shard sync based on leasesRecoveryAuditorInconsistencyConfidenceThreshold.          |
	| leasesRecoveryAuditorInconsistencyConfidenceThreshold | 3                  | Confidence threshold for the periodic auditor job to determine if leases for a stream in the lease table is inconsistent. If the auditor finds same set of inconsistencies consecutively for a stream for this many times, then it would trigger a shard sync |

	* New CloudWatch metrics are also now emitted to monitor the health of `PeriodicShardSyncManager`:

	| Name                        | Description                                            |
	| --------------------------- | ------------------------------------------------------ |
	| NumStreamsWithPartialLeases | Number of streams that had holes in their hash ranges. |
	| NumStreamsToSync            | Number of streams which underwent a full shard sync.   |

* Introducing deferred lease cleanup. Leases will be deleted asynchronously by `LeaseCleanupManager` upon reaching `SHARD_END`, when a shard has either expired past the stream’s retention period or been closed as the result of a resharding operation.
	* New configuration options are available to configure `LeaseCleanupManager`.

	| Name                                | Default    | Description                                                                                               |
	| ----------------------------------- | ---------- | --------------------------------------------------------------------------------------------------------- |
	| leaseCleanupIntervalMillis          | 1 minute   | Interval at which to run lease cleanup thread.                                                            |
	| completedLeaseCleanupIntervalMillis | 5 minutes  | Interval at which to check if a lease is completed or not.                                                |
	| garbageLeaseCleanupIntervalMillis   | 30 minutes | Interval at which to check if a lease is garbage (i.e trimmed past the stream's retention period) or not. |

* Introducing _experimental_ support for multistreaming, allowing a single KCL application to multiplex processing multiple streams.
	* New configuration options are available to enable multistreaming in `RetrievalConfig#appStreamTracker`.

* Fixing a bug in `PrefetchRecordsPublisher` restarting while it was already running.
* Including an optimization to `HierarchicalShardSyncer` to only create leases for one layer of shards.
* Adding support to prepare and commit lease checkpoints with arbitrary bytes.
	* This allows checkpointing of an arbitrary byte buffer up to the maximum permitted DynamoDB item size ([currently 400 KB as of release](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)), and can be used for recovery by passing a serialized byte buffer to `RecordProcessorCheckpointer#prepareCheckpoint` and `RecordProcessorCheckpointer#checkpoint`.
* Upgrading version of AWS SDK to 2.14.0.
* [#725](https://github.com/awslabs/amazon-kinesis-client/pull/725) Allowing KCL to consider lease tables in `UPDATING` healthy.

### For remaining release notes check **[CHANGELOG.md][changelog-md]**.

[kinesis]: http://aws.amazon.com/kinesis
[kinesis-forum]: http://developer.amazonwebservices.com/connect/forum.jspa?forumID=169
[kinesis-client-library-issues]: https://github.com/awslabs/amazon-kinesis-client/issues
[docs-signup]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html
[kinesis-guide]: http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html
[kinesis-guide-begin]: http://docs.aws.amazon.com/kinesis/latest/dev/before-you-begin.html
[kinesis-guide-create]: http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html
[kinesis-guide-applications]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-app.html
[kinesis-guide-monitoring-with-kcl]: http://docs.aws.amazon.com//kinesis/latest/dev/monitoring-with-kcl.html
[kinesis-guide-kpl]: http://docs.aws.amazon.com//kinesis/latest/dev/developing-producers-with-kpl.html
[kinesis-guide-consumer-deaggregation]: http://docs.aws.amazon.com//kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
[kclpy]: https://github.com/awslabs/amazon-kinesis-client-python
[multi-lang-protocol]: https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client-multilang/src/main/java/software/amazon/kinesis/multilang/package-info.java
[changelog-md]: https://github.com/awslabs/amazon-kinesis-client/blob/master/CHANGELOG.md
[migration-guide]: https://docs.aws.amazon.com/streams/latest/dev/kcl-migration.html
