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

### Latest Release (1.14.0 - August 17, 2020)

* [Milestone#50](https://github.com/awslabs/amazon-kinesis-client/milestone/50)

* Behavior of shard synchronization is moving from each worker independently learning about all existing shards to workers only discovering the children of shards that each worker owns. This optimizes memory usage, lease table IOPS usage, and number of calls made to kinesis for streams with high shard counts and/or frequent resharding.
* When bootstrapping an empty lease table, KCL utilizes the `ListShard` API's filtering option (the ShardFilter optional request parameter) to retrieve and create leases only for a snapshot of shards open at the time specified by the `ShardFilter` parameter. The `ShardFilter` parameter enables you to filter out the response of the `ListShards` API, using the `Type` parameter. KCL uses the `Type` filter parameter and the following of its valid values to identify and return a snapshot of open shards that might require new leases.
	* Currently, the following shard filters are supported:
		* `AT_TRIM_HORIZON` - the response includes all the shards that were open at `TRIM_HORIZON`.
		* `AT_LATEST` - the response includes only the currently open shards of the data stream.
	    * `AT_TIMESTAMP` - the response includes all shards whose start timestamp is less than or equal to the given timestamp and end timestamp is greater than or equal to the given timestamp or still open.
	* `ShardFilter` is used when creating leases for an empty lease table to initialize leases for a snapshot of shards specified at `KinesisClientLibConfiguration#initialPositionInStreamExtended`.
	* For more information about ShardFilter, see the [official AWS documentation on ShardFilter](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ShardFilter.html).

* Introducing support for the `ChildShards` response of the `GetRecords` and the `SubscribeToShard` APIs to perform lease/shard synchronization that happens at `SHARD_END` for closed shards, allowing a KCL worker to only create leases for the child shards of the shard it finished processing.
	* For shared throughout consumer applications, this uses the `ChildShards` response of the `GetRecords` API. For dedicated throughput (enhanced fan-out) consumer applications, this uses the `ChildShards` response of the `SubscribeToShard` API.
	* For more information, see the official AWS Documentation on [GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html), [SubscribeToShard](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SubscribeToShard.html), and [ChildShard](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ChildShard.html).

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
* Upgrading version of AWS SDK to 2.13.X.
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
