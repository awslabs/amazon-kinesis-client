# Bugs in 1.14.0 version
We recommend customers to migrate to 1.14.1 to avoid [known bugs](https://github.com/awslabs/amazon-kinesis-client/issues/778) in 1.14.0 version

# Amazon Kinesis Client Library for Java
[![Build Status](https://travis-ci.org/awslabs/amazon-kinesis-client.svg?branch=master)](https://travis-ci.org/awslabs/amazon-kinesis-client) ![BuildStatus](https://codebuild.us-west-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiaWo4bDYyUkpWaG9ZTy9zeFVoaVlWbEwxazdicDJLcmZwUUpFWVVBM0ZueEJSeFIzNkhURzdVbUd6WUZHcGNxa3BEUzNrL0I5Nzc4NE9rbXhvdEpNdlFRPSIsIml2UGFyYW1ldGVyU3BlYyI6IlZDaVZJSTM1QW95bFRTQnYiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=v1.x)

The **Amazon Kinesis Client Library for Java** (Amazon KCL) enables Java developers to easily consume and process data from [Amazon Kinesis][kinesis].

* [Kinesis Product Page][kinesis]
* [Forum][kinesis-forum]
* [Issues][kinesis-client-library-issues]

## Features

* Provides an easy-to-use programming model for processing data using Amazon Kinesis
* Helps with scale-out and fault-tolerant processing

## Getting Started

1. **Sign up for AWS** &mdash; Before you begin, you need an AWS account. For more information about creating an AWS account and retrieving your AWS credentials, see [AWS Account and Credentials][docs-signup] in the AWS SDK for Java Developer Guide.
1. **Sign up for Amazon Kinesis** &mdash; Go to the Amazon Kinesis console to sign up for the service and create an Amazon Kinesis stream. For more information, see [Create an Amazon Kinesis Stream][kinesis-guide-create] in the Amazon Kinesis Developer Guide.
1. **Minimum requirements** &mdash; To use the Amazon Kinesis Client Library, you'll need **Java 1.8+**. For more information about Amazon Kinesis Client Library requirements, see [Before You Begin][kinesis-guide-begin] in the Amazon Kinesis Developer Guide.
1. **Using the Amazon Kinesis Client Library** &mdash; The best way to get familiar with the Amazon Kinesis Client Library is to read [Developing Record Consumer Applications][kinesis-guide-applications] in the Amazon Kinesis Developer Guide.

## Building from Source

After you've downloaded the code from GitHub, you can build it using Maven. To disable GPG signing in the build, use this command: `mvn clean install -Dgpg.skip=true`

## Integration with the Kinesis Producer Library
For producer-side developers using the **[Kinesis Producer Library (KPL)][kinesis-guide-kpl]**, the KCL integrates without additional effort. When the KCL retrieves an aggregated Amazon Kinesis record consisting of multiple KPL user records, it will automatically invoke the KPL to extract the individual user records before returning them to the user.

## Amazon KCL support for other languages
To make it easier for developers to write record processors in other languages, we have implemented a Java based daemon, called MultiLangDaemon that does all the heavy lifting. Our approach has the daemon spawn a sub-process, which in turn runs the record processor, which can be written in any language. The MultiLangDaemon process and the record processor sub-process communicate with each other over [STDIN and STDOUT using a defined protocol][multi-lang-protocol]. There will be a one to one correspondence amongst record processors, child processes, and shards. For Python developers specifically, we have abstracted these implementation details away and [expose an interface][kclpy] that enables you to focus on writing record processing logic in Python. This approach enables KCL to be language agnostic, while providing identical features and similar parallel processing model across all languages.

## Release Notes
### Latest Release (1.15.0 - Jun 7, 2023)
* **[#1108](https://github.com/awslabs/amazon-kinesis-client/pull/1108) Add support for Stream ARNs**
* [#1111](https://github.com/awslabs/amazon-kinesis-client/pull/1111) More consistent testing behavior with HashRangesAreAlwaysComplete
* [#1054](https://github.com/awslabs/amazon-kinesis-client/pull/1054) Upgrade log4j-core from 2.17.1 to 2.20.0
* [#1103](https://github.com/awslabs/amazon-kinesis-client/pull/1103) Upgrade jackson-core from 2.13.0 to 2.15.0
* [#943](https://github.com/awslabs/amazon-kinesis-client/pull/943) Upgrade nexus-staging-maven-plugin from 1.6.8 to 1.6.13
* [#1044](https://github.com/awslabs/amazon-kinesis-client/pull/1044) Upgrade aws-java-sdk.version from 1.12.406 to 1.12.408
* [#1055](https://github.com/awslabs/amazon-kinesis-client/pull/1055) Upgrade maven-compiler-plugin from 3.10.0 to 3.11.0

### Release (1.14.10 - Feb 15, 2023)
* Updated aws-java-sdk from 1.12.130 to 1.12.406
* Updated com.google.protobuf from 3.19.4 to 3.19.6
	* [Issue #1026](https://github.com/awslabs/amazon-kinesis-client/issues/1026)
	* [PR #1042](https://github.com/awslabs/amazon-kinesis-client/pull/1042)

### Release (1.14.9 - Dec 14, 2022)
* [#995](https://github.com/awslabs/amazon-kinesis-client/commit/372f98b21a91487e36612d528c56765a44b0aa86) Every other change for DynamoDBStreamsKinesis Adapter Compatibility
* [#970](https://github.com/awslabs/amazon-kinesis-client/commit/251b331a2e0fd912b50f8b5a12d088bf0b3263b9) PeriodicShardSyncManager Changes Needed for DynamoDBStreamsKinesisAdapter

### Release (1.14.8 - Feb 24, 2022)
* [Bump log4j-core from 2.17.0 to 2.17.1](https://github.com/awslabs/amazon-kinesis-client/commit/94b138a9d9a502ee0f4f000bb0efd2766ebadc37)
* [Bump protobuf-java from 3.19.1 to 3.19.4](https://github.com/awslabs/amazon-kinesis-client/commit/a809b12c43c57a3d6ad3827feb60e4322614259c)
* [Bump maven-compiler-plugin from 3.8.1 to 3.10.0](https://github.com/awslabs/amazon-kinesis-client/commit/37b5d7b9a1ccad483469ef542a6a7237462b14f2)

### Release (1.14.7 - Dec 22, 2021)
* [#881](https://github.com/awslabs/amazon-kinesis-client/pull/881) Update log4j test dependency from 2.16.0 to 2.17.0 and some other dependencies

### Release (1.14.6 - Dec 15, 2021)
* [#876](https://github.com/awslabs/amazon-kinesis-client/pull/876) Update log4j test dependency from 2.15.0 to 2.16.0

### Release (1.14.5 - Dec 10, 2021)
* [#872](https://github.com/awslabs/amazon-kinesis-client/pull/872) Update log4j test dependency from 1.2.17 to 2.15.0
* [#873](https://github.com/awslabs/amazon-kinesis-client/pull/873) Upgrading version of AWS Java SDK to 1.12.128

### Release (1.14.4 - June 14, 2021)
* [Milestone#61](https://github.com/awslabs/amazon-kinesis-client/milestone/61)
* [#816](https://github.com/awslabs/amazon-kinesis-client/pull/816) Updated the Worker shutdown logic to make sure that the `LeaseCleanupManager` also terminates all the threads that it has started.
* [#821](https://github.com/awslabs/amazon-kinesis-client/pull/821) Upgrading version of AWS Java SDK to 1.12.3

### Release (1.14.3 - May 3, 2021)
* [Milestone#60](https://github.com/awslabs/amazon-kinesis-client/milestone/60)
* [#811](https://github.com/awslabs/amazon-kinesis-client/pull/811) Fixing a bug in `KinesisProxy` that can lead to undetermined behavior during partial failures.
* [#811](https://github.com/awslabs/amazon-kinesis-client/pull/811) Adding guardrails to handle duplicate shards from the service.

## Release (1.14.2 - February 24, 2021)
* [Milestone#57](https://github.com/awslabs/amazon-kinesis-client/milestone/57)
* [#790](https://github.com/awslabs/amazon-kinesis-client/pull/790) Fixing a bug that caused paginated `ListShards` calls with the `ShardFilter` parameter to fail when the lease table was being initialized.

### Release (1.14.1 - January 27, 2021)
* [Milestone#56](https://github.com/awslabs/amazon-kinesis-client/milestone/56)

* Fix for cross DDB table interference when multiple KCL applications are run in same JVM.
* Fix and guards to avoid potential checkpoint rewind during shard end, which may block children shard processing.
* Fix for thread cycle wastage on InitializeTask for deleted shard.
* Improved logging in LeaseCleanupManager that would indicate why certain shards are not cleaned up from the lease table.

### Release (1.14.0 - August 17, 2020)

* [Milestone#50](https://github.com/awslabs/amazon-kinesis-client/milestone/50)

* Behavior of shard synchronization is moving from each worker independently learning about all existing shards to workers only discovering the children of shards that each worker owns. This optimizes memory usage, lease table IOPS usage, and number of calls made to kinesis for streams with high shard counts and/or frequent resharding.
* When bootstrapping an empty lease table, KCL utilizes the ListShard API's filtering option (the ShardFilter optional request parameter) to retrieve and create leases only for a snapshot of shards open at the time specified by the ShardFilter parameter. The ShardFilter parameter enables you to filter out the response of the ListShards API, using the Type parameter. KCL uses the Type filter parameter and the following of its valid values to identify and return a snapshot of open shards that might require new leases.
	* Currently, the following shard filters are supported:
		* `AT_TRIM_HORIZON` - the response includes all the shards that were open at `TRIM_HORIZON`.
		* `AT_LATEST` - the response includes only the currently open shards of the data stream.
	    * `AT_TIMESTAMP` - the response includes all shards whose start timestamp is less than or equal to the given timestamp and end timestamp is greater than or equal to the given timestamp or still open.
	* `ShardFilter` is used when creating leases for an empty lease table to initialize leases for a snapshot of shards specified at `KinesisClientLibConfiguration#initialPositionInStreamExtended`.
	* For more information about ShardFilter, see the [official AWS documentation on ShardFilter](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ShardFilter.html).

* Introducing support for the `ChildShards` response of the `GetRecords` API to perform lease/shard synchronization that happens at `SHARD_END` for closed shards, allowing a KCL worker to only create leases for the child shards of the shard it finished processing.
	* For KCL 1.x applications, this uses the `ChildShards` response of the `GetRecords` API.
	* For more information, see the official AWS Documentation on [GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) and [ChildShard](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ChildShard.html).

* KCL now also performs additional periodic shard/lease scans in order to identify any potential holes in the lease table to ensure the complete hash range of the stream is being processed and create leases for them if required. When `KinesisClientLibConfiguration#shardSyncStrategyType` is set to `ShardSyncStrategyType.SHARD_END`, `PeriodicShardSyncManager#leasesRecoveryAuditorInconsistencyConfidenceThreshold` will be used to determine the threshold for number of consecutive scans containing holes in the lease table after which to enforce a shard sync. When `KinesisClientLibConfiguration#shardSyncStrategyType` is set to `ShardSyncStrategyType.PERIODIC`, `leasesRecoveryAuditorInconsistencyConfidenceThreshold` is ignored.
	* New configuration options are available to configure `PeriodicShardSyncManager` in `KinesisClientLibConfiguration`

	| Name                                                  | Default | Description                                                                                                                                                                                                                                                                                       |
	| ----------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
	| leasesRecoveryAuditorInconsistencyConfidenceThreshold | 3       | Confidence threshold for the periodic auditor job to determine if leases for a stream in the lease table is inconsistent. If the auditor finds same set of inconsistencies consecutively for a stream for this many times, then it would trigger a shard sync. Only used for `ShardSyncStrategyType.SHARD_END`. |

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

* Including an optimization to `KinesisShardSyncer` to only create leases for one layer of shards.
* Changing default shard prioritization strategy to be `NoOpShardPrioritization` to allow prioritization of completed shards. Customers who are upgrading to this version and are reading from `TRIM_HORIZON` should continue using `ParentsFirstShardPrioritization` while upgrading.
* Upgrading version of AWS SDK to 1.11.844.
* [#719](https://github.com/awslabs/amazon-kinesis-client/pull/719) Upgrading version of Google Protobuf to 3.11.4.
* [#712](https://github.com/awslabs/amazon-kinesis-client/pull/712) Allowing KCL to consider lease tables in `UPDATING` healthy.

###### For remaining release notes check **[CHANGELOG.md][changelog-md]**.

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
[multi-lang-protocol]: https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/multilang/package-info.java
[changelog-md]: https://github.com/awslabs/amazon-kinesis-client/blob/master/CHANGELOG.md
